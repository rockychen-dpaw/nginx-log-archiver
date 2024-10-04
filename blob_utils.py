import os

from azure.storage.blob import BlobClient, ContainerClient


def get_container_blobs(conn_str, container_name, name_starts_with=None):
    """
    Check Azure blob storage for the list of blobs, returns a
    list of filenames (minus any prefix).
    """
    container_client = ContainerClient.from_connection_string(conn_str, container_name)
    if name_starts_with:
        blob_list = container_client.list_blobs(name_starts_with)
    else:
        blob_list = container_client.list_blobs()
    return [blob.name for blob in blob_list]


def get_blob_client(
    conn_str,
    container_name,
    blob_name,
    max_single_get=8 * 1024 * 1024,
    max_get=2 * 1024 * 1024,
    max_put=16 * 1024 * 1024,
    timeout=120,
):
    """
    Return a BlobClient class having defaults to account for a slower internet connection.
    The parent class defaults are 64MB and 20s.
    https://learn.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobclient?view=azure-python#azure-storage-blob-blobclient-from-connection-string
    """
    return BlobClient.from_connection_string(
        conn_str,
        container_name,
        blob_name,
        max_single_get_size=max_single_get,
        max_chunk_get_size=max_get,
        max_single_put_size=max_put,
        connection_timeout=timeout,
    )


def upload_blob(
    conn_str, container_name, source_path, blob_prefix=None, overwrite=True
):
    """
    Upload a single file at `source_path` to Azure blob storage, allowing an optional
    prefix value and/or overwriting any existing blob.
    """
    blob_name = os.path.basename(source_path)
    if blob_prefix:
        blob_name = f"{blob_prefix}/{blob_name}"
    blob_client = get_blob_client(conn_str, container_name, blob_name)

    with open(source_path, "rb") as source_data:
        blob_client.upload_blob(source_data, overwrite=overwrite)


def download_blob(conn_str, container_name, blob_name, dest_path):
    """Download a single Azure container blob to `dest_path`."""
    blob_client = get_blob_client(conn_str, container_name, blob_name)

    with open(dest_path, "wb") as downloaded_blob:
        download_stream = blob_client.download_blob()
        downloaded_blob.write(download_stream.readall())
