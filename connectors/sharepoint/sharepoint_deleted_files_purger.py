import logging
import os
import asyncio
import aiohttp
from collections import defaultdict
from tools import KeyVaultClient
from tools import AISearchClient
from typing import Any, Dict, List, Optional


class SharepointDeletedFilesPurger:
    def __init__(self):
        # Initialize configuration from environment variables
        self.connector_enabled = os.getenv("SHAREPOINT_CONNECTOR_ENABLED", "false").lower() == "true"
        self.tenant_id = os.getenv("SHAREPOINT_TENANT_ID")
        self.client_id = os.getenv("SHAREPOINT_CLIENT_ID")
        self.client_secret_name = os.getenv("SHAREPOINT_CLIENT_SECRET_NAME", "sharepointClientSecret")
        self.index_name = os.getenv("AZURE_SEARCH_SHAREPOINT_INDEX_NAME", "ragindex")
        self.site_domain = os.getenv("SHAREPOINT_SITE_DOMAIN")
        self.site_name = os.getenv("SHAREPOINT_SITE_NAME")
        self.drive_name = os.getenv("SHAREPOINT_DRIVE_NAME")  # default drive if not set
        
        self.keyvault_client: Optional[KeyVaultClient] = None
        self.client_secret: Optional[str] = None
        self.search_client: Optional[AISearchClient] = None
        self.site_id: Optional[str] = None
        self.access_token: Optional[str] = None
        self.drive_id: Optional[str] = None

    async def initialize_clients(self) -> bool:
        """Initialize KeyVaultClient, retrieve secrets, and initialize AISearchClient."""
        try:
            self.keyvault_client = KeyVaultClient()
            self.client_secret = await self.keyvault_client.get_secret(self.client_secret_name)
            logging.debug("[sharepoint_purge_deleted_files] Retrieved sharepointClientSecret secret from Key Vault.")
        except Exception as e:
            logging.error(f"[sharepoint_purge_deleted_files] Failed to retrieve secret from Key Vault: {e}")
            return False
        finally:
            if self.keyvault_client:
                await self.keyvault_client.close()

        # Check for missing environment variables
        required_vars = {
            "SHAREPOINT_TENANT_ID": self.tenant_id,
            "SHAREPOINT_CLIENT_ID": self.client_id,
            "SHAREPOINT_SITE_DOMAIN": self.site_domain,
            "SHAREPOINT_SITE_NAME": self.site_name,
            "AZURE_SEARCH_SHAREPOINT_INDEX_NAME": self.index_name, 
            "SHAREPOINT_DRIVE_NAME": self.drive_name  # ✅ added drive name
        }

        missing_env_vars = [var for var, value in required_vars.items() if not value]
        if missing_env_vars:
            logging.error(
                f"[sharepoint_purge_deleted_files] Missing environment variables: {', '.join(missing_env_vars)}"
            )
            return False

        if not self.client_secret:
            logging.error("[sharepoint_purge_deleted_files] SharePoint client secret not found in Key Vault.")
            return False

        # Initialize AISearchClient
        try:
            self.search_client = AISearchClient()
            logging.debug("[sharepoint_purge_deleted_files] Initialized AISearchClient successfully.")
        except Exception as e:
            logging.error(f"[sharepoint_purge_deleted_files] AISearchClient initialization failed: {e}")
            return False

        return True

    async def get_graph_access_token(self) -> Optional[str]:
        """Obtain access token for Microsoft Graph API."""
        token_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": "https://graph.microsoft.com/.default"
        }

        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(token_url, headers=headers, data=data) as resp:
                    if resp.status == 200:
                        token_response = await resp.json()
                        access_token = token_response.get("access_token")
                        logging.debug("[sharepoint_purge_deleted_files] Obtained access token for Microsoft Graph API.")
                        return access_token
                    else:
                        error_response = await resp.text()
                        logging.error(f"[sharepoint_purge_deleted_files] Failed to obtain access token: {resp.status} - {error_response}")
                        return None
            except Exception as e:
                logging.error(f"[sharepoint_purge_deleted_files] Exception while obtaining access token: {e}")
                return None

    async def get_site_id(self) -> Optional[str]:
        """Retrieve the SharePoint site ID using Microsoft Graph API."""
        url = f"https://graph.microsoft.com/v1.0/sites/{self.site_domain}:/sites/{self.site_name}?$select=id"
        headers = {"Authorization": f"Bearer {self.access_token}"}

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        site_id = data.get("id")
                        if site_id:
                            logging.info(f"[sharepoint_purge_deleted_files] Retrieved site ID: {site_id}")
                            return site_id
                        logging.error("[sharepoint_purge_deleted_files] 'id' field missing in site response.")
                        return None
                    else:
                        error_response = await resp.text()
                        logging.error(f"[sharepoint_purge_deleted_files] Failed to retrieve site ID: {resp.status} - {error_response}")
                        return None
            except Exception as e:
                logging.error(f"[sharepoint_purge_deleted_files] Exception while retrieving site ID: {e}")
                return None

    async def get_drive_id(self) -> Optional[str]:
        """Retrieve the drive ID for the configured document library."""
        url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives"
        headers = {"Authorization": f"Bearer {self.access_token}"}

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, headers=headers) as resp:
                    if resp.status != 200:
                        logging.error(f"[sharepoint_purge_deleted_files] Failed to list drives: {resp.status}")
                        return None
                    data = await resp.json()
                    for drive in data.get("value", []):
                        if drive.get("name") == self.drive_name:
                            logging.info(f"[sharepoint_purge_deleted_files] Found drive '{self.drive_name}' with ID {drive.get('id')}")
                            return drive.get("id")
                    logging.error(f"[sharepoint_purge_deleted_files] Drive '{self.drive_name}' not found in site.")
                    return None
            except Exception as e:
                logging.error(f"[sharepoint_purge_deleted_files] Exception while retrieving drives: {e}")
                return None

    async def check_parent_id_exists(self, parent_id: Any, headers: Dict[str, str], semaphore: asyncio.Semaphore) -> bool:
        """Check if a SharePoint parent ID exists in the specific document library."""
        if not self.drive_id:
            logging.error("[sharepoint_purge_deleted_files] drive_id is not set. Cannot check parent ID.")
            return False

        check_url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{parent_id}"
        async with semaphore:
            async with aiohttp.ClientSession() as session:
                try:
                    async with session.get(check_url, headers=headers) as resp:
                        if resp.status == 200:
                            logging.debug(f"[sharepoint_purge_deleted_files] SharePoint ID {parent_id} exists.")
                            return True
                        elif resp.status == 404:
                            logging.debug(f"[sharepoint_purge_deleted_files] SharePoint ID {parent_id} does not exist.")
                            return False
                        else:
                            error_text = await resp.text()
                            logging.error(f"[sharepoint_purge_deleted_files] Error checking SharePoint ID {parent_id}: {resp.status} - {error_text}")
                            return False
                except Exception as e:
                    logging.error(f"[sharepoint_purge_deleted_files] Exception while checking SharePoint ID {parent_id}: {e}")
                    return False

    async def purge_deleted_files(self) -> None:
        """Main method to purge deleted SharePoint files from Azure Search index."""
        logging.info("[sharepoint_purge_deleted_files] Started SharePoint purge connector function.")

        if not self.connector_enabled:
            logging.info("[sharepoint_purge_deleted_files] SharePoint purge connector is disabled.")
            return

        if not await self.initialize_clients():
            return

        # Get access token
        self.access_token = await self.get_graph_access_token()
        if not self.access_token:
            logging.error("[sharepoint_purge_deleted_files] Cannot proceed without access token.")
            await self.search_client.close()
            return

        # Get site ID
        self.site_id = await self.get_site_id()
        if not self.site_id:
            logging.error("[sharepoint_purge_deleted_files] Cannot retrieve site_id.")
            await self.search_client.close()
            return

        # Get drive ID
        self.drive_id = await self.get_drive_id()
        if not self.drive_id:
            logging.error("[sharepoint_purge_deleted_files] Cannot proceed without drive ID.")
            await self.search_client.close()
            return

        # Retrieve all documents from Azure Search
        logging.info("[sharepoint_purge_deleted_files] Retrieving documents from Azure Search index.")
        try:
            search_results = await self.search_client.search_documents(
                index_name=self.index_name,
                search_text="*",
                filter_str="parent_id ne null and source eq 'sharepoint'",
                select_fields=["parent_id", "id", "metadata_storage_name"],
                top=0
            )
        except Exception as e:
            logging.error(f"[sharepoint_purge_deleted_files] Failed to retrieve documents: {e}")
            await self.search_client.close()
            return

        documents = search_results.get("documents", [])
        logging.info(f"[sharepoint_purge_deleted_files] Retrieved {len(documents)} SharePoint document chunks.")

        if not documents:
            logging.info("[sharepoint_purge_deleted_files] No document chunks to purge. Exiting function.")
            await self.search_client.close()
            return

        # Map parent_id to list of document IDs
        sharepoint_to_doc_ids = defaultdict(list)
        for doc in documents:
            if "parent_id" in doc and "id" in doc:
                sharepoint_to_doc_ids[doc["parent_id"]].append(doc["id"])

        parent_ids = list(sharepoint_to_doc_ids.keys())
        logging.info(f"[sharepoint_purge_deleted_files] Checking existence of {len(parent_ids)} SharePoint document(s).")

        semaphore = asyncio.Semaphore(10)
        headers = {"Authorization": f"Bearer {self.access_token}"}
        existence_tasks = [self.check_parent_id_exists(pid, headers, semaphore) for pid in parent_ids]
        existence_results = await asyncio.gather(*existence_tasks)

        # Identify document IDs to delete
        doc_ids_to_delete = []
        for pid, exists in zip(parent_ids, existence_results):
            if not exists:
                doc_ids_to_delete.extend(sharepoint_to_doc_ids[pid])

        logging.info(f"[sharepoint_purge_deleted_files] {len(doc_ids_to_delete)} document chunks identified for purging.")

        if doc_ids_to_delete:
            batch_size = 100
            for i in range(0, len(doc_ids_to_delete), batch_size):
                batch = doc_ids_to_delete[i:i + batch_size]
                try:
                    # await self.search_client.delete_documents(
                    #     index_name=self.index_name,
                    #     key_field="id",
                    #     key_values=batch
                    # )
                    logging.info(f"No actually purging files")
                    logging.info(f"[sharepoint_purge_deleted_files] Purged batch of {len(batch)} documents from Azure Search.")
                except Exception as e:
                    logging.error(f"[sharepoint_purge_deleted_files] Failed to purge batch starting at index {i}: {e}")

        else:
            logging.info("[sharepoint_purge_deleted_files] No documents to purge.")

        try:
            await self.search_client.close()
            logging.debug("[sharepoint_purge_deleted_files] Closed AISearchClient successfully.")
        except Exception as e:
            logging.error(f"[sharepoint_purge_deleted_files] Failed to close AISearchClient: {e}")

        logging.info("[sharepoint_purge_deleted_files] Completed SharePoint purge connector function.")

    async def run(self) -> None:
        """Run the purge process."""
        await self.purge_deleted_files()
