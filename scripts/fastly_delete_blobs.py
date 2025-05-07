import multiprocessing
import os
from datetime import datetime

from azure.storage.blob import BlobClient, ContainerClient
from dotenv import load_dotenv

load_dotenv()
CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER = "fastly"


def delete_blob(blob_name: str):
    blob_client = BlobClient.from_connection_string(CONN_STR, CONTAINER, blob_name)
    if blob_client.exists():
        print(f"{blob_name} exists, deleting it")
        blob_client.delete_blob()


def mass_delete_blobs(older_than: datetime):
    """Delete Fastly logs older than an arbitrary amount of time.
    `older_than` should be TZ-aware."""
    container_client = ContainerClient.from_connection_string(CONN_STR, CONTAINER)

    for prefix in ["azure-nginx", "ria-ac3"]:
        print(f"Deleting logs prefixed by {prefix}")
        blob_list = container_client.list_blobs(name_starts_with=prefix)
        log_list = [b.name for b in blob_list if b.creation_time < older_than]
        print(f"{len(log_list)} logs found, deleting")
        # Use an multiprocessing pool to delete blobs in parallel.
        pool = multiprocessing.Pool(8)
        pool.imap_unordered(delete_blob, log_list, chunksize=10)
        pool.close()
        pool.join()
