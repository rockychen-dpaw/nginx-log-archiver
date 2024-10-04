import argparse
import logging
import os
import sys
from datetime import datetime
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from azure.storage.blob import BlobClient, ContainerClient
from dotenv import load_dotenv

# Load environment variables.
load_dotenv()
# Assumes a connection string secret present as an environment variable.
CONN_STR = os.getenv("AZURE_CONNECTION_STRING")

# Configure logging for the default logger and for the `azure` logger.
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger.addHandler(handler)

# Set the logging level for all azure-* libraries (the azure-storage-blob library uses this one).
# Reference: https://learn.microsoft.com/en-us/azure/developer/python/sdk/azure-sdk-logging
azure_logger = logging.getLogger("azure")
azure_logger.setLevel(logging.WARNING)


def archive_logs(datestamp, container_name="access-logs"):
    """Given the supplied datestamp string, download CSV-formatted Nginx access logs having this prefix,
    combine them into a single parquet dataset, upload the parquet dataset to the blob container,
    then delete the original CSV-formatted logs.
    """
    # Validate the supplied datestamp value.
    try:
        datetime.strptime(datestamp, "%Y%m%d")
    except Exception as e:
        logger.warning(f"Invalid datestamp value: {e}")
        return

    container_client = ContainerClient.from_connection_string(CONN_STR, container_name)
    blob_list = container_client.list_blobs(name_starts_with=datestamp)
    csv_blobs = [blob.name for blob in blob_list]

    csv_dir = TemporaryDirectory()
    schema = pa.schema(
        [
            pa.field("timestamp", pa.timestamp("s", tz="Australia/Perth")),
            pa.field("remote_ip", pa.string()),
            pa.field("host", pa.string()),
            pa.field("path", pa.string()),
            pa.field("params", pa.string()),
            pa.field("method", pa.string()),
            pa.field("protocol", pa.string()),
            pa.field("status", pa.int16()),
            pa.field("request_time_µs", pa.int64()),
            pa.field("bytes_sent", pa.int64()),
            pa.field("user_agent", pa.string()),
            pa.field("email", pa.string()),
        ]
    )

    # Download CSVs
    for blob_name in csv_blobs:
        logger.info(f"Downloading {blob_name}")
        dest_path = os.path.join(csv_dir.name, blob_name)
        blob_client = BlobClient.from_connection_string(CONN_STR, container_name, blob_name)

        with open(dest_path, "wb") as downloaded_blob:
            download_stream = blob_client.download_blob()
            downloaded_blob.write(download_stream.readall())

    csv_files = sorted(os.listdir(csv_dir.name))

    # Prepend a header row in each CSV
    headers = "timestamp,remote_ip,host,path,params,method,protocol,status,request_time_µs,bytes_sent,user_agent,email"
    for csv_file in csv_files:
        path = os.path.join(csv_dir.name, csv_file)
        if os.path.getsize(path) > 0:
            os.system(f"sed -i '1s;^;{headers}\\n;' {path}")
        else:  # sed doesn't work for an empty source file.
            os.system(f"echo '{headers}' > {path}")

    # Read the directory of CSVs in as a dataset, then convert to a table.
    dataset = ds.dataset(csv_dir.name, schema=schema, format="csv")
    table = dataset.to_table()

    # Output the dataset locally to a parquet file.
    pq_file = f"{datestamp}.nginx.access.parquet"
    pq_path = os.path.join(csv_dir.name, pq_file)
    logger.info(f"Outputting dataset table to {pq_path}")
    pq.write_table(table, pq_path)

    # Upload the parquet file to the container.
    logger.info(f"Uploading to blob archive/{pq_file}")
    blob_client = BlobClient.from_connection_string(CONN_STR, container_name, f"archive/{pq_file}")
    with open(pq_path, "rb") as source_data:
        blob_client.upload_blob(source_data, overwrite=True)

    # Ensure that the uploaded blob's access tier is 'Cold'.
    blob_client.set_standard_blob_tier("Cold")

    # Delete the original remote CSV log files.
    for blob_name in csv_blobs:
        logger.info(f"Deleting blob {blob_name}")
        blob_client = BlobClient.from_connection_string(CONN_STR, container_name, blob_name)
        blob_client.delete_blob(delete_snapshots="include")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        "--datestamp",
        help="A datestamp value in the format %%Y%%m%%d",
        action="store",
        required=True,
    )
    parser.add_argument(
        "-c",
        "--container",
        help="The blob container name (optional)",
        default="access-logs",
        action="store",
        required=False,
    )
    args = parser.parse_args()
    archive_logs(datestamp=args.datestamp, container_name=args.container)
