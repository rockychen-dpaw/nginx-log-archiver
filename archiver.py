import argparse
import os
from datetime import datetime
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
from azure.storage.blob import BlobClient, ContainerClient
from dotenv import load_dotenv

from utils import configure_logging

# Load environment variables.
load_dotenv()
# Assumes a connection string secret present as an environment variable.
CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

# Configure logging.
LOGGER = configure_logging()


def download_logs(datestamp, destination, container_name="access-logs"):
    """Given the passed in datestamp and destination directory, download logs from blob storage."""
    container_client = ContainerClient.from_connection_string(CONN_STR, container_name)
    blob_list = container_client.list_blobs(name_starts_with=datestamp)
    csv_blobs = [blob.name for blob in blob_list]
    if not csv_blobs:  # Nil source data, abort.
        LOGGER.info(f"No source data for datestamp {datestamp}")
        return

    # Download CSVs
    for blob_name in csv_blobs:
        LOGGER.info(f"Downloading {blob_name}")
        dest_path = os.path.join(destination, blob_name)
        blob_client = BlobClient.from_connection_string(CONN_STR, container_name, blob_name)

        try:
            with open(dest_path, "wb") as downloaded_blob:
                download_stream = blob_client.download_blob()
                downloaded_blob.write(download_stream.readall())
        except Exception as e:
            LOGGER.error(f"Exception during download of {blob_name}, aborting")
            LOGGER.exception(e)
            return

    return True


def prepend_header_row(path):
    """Given the passed in CSV file path, prepend a header row."""
    if os.path.getsize(path) <= 0:
        # Pass/abort on empty/null file (no data).
        LOGGER.warning(f"Empty file/no data: {path}")
        return False

    headers = "timestamp,remote_ip,host,path,params,method,protocol,status,request_time_µs,bytes_sent,user_agent,email"
    LOGGER.info(f"Prepending header row in {path}")
    os.system(f"sed -i '1s;^;{headers}\\n;' {path}")
    return True


def invalid_row_handler(row):
    """A callable to handle each CSV row that fails parsing based on the supplied schema.
    Ref: https://arrow.apache.org/docs/python/generated/pyarrow.csv.ParseOptions.html#pyarrow.csv.ParseOptions
    """
    LOGGER.warning(f"BAD ROW:\n{row}")
    return "error"


def archive_logs(datestamp, container_name="access-logs", delete_source=False):
    """Given the supplied datestamp string, download CSV-formatted Nginx access logs having this prefix,
    combine them into a single parquet dataset, upload the parquet dataset to the blob container,
    then delete the original CSV-formatted logs.
    """
    # Validate the supplied datestamp value.
    d = None
    try:
        d = datetime.strptime(datestamp, "%Y%m%d")
    except Exception:
        pass
    try:
        d = datetime.strptime(datestamp, "%Y-%m-%d")
    except Exception:
        pass

    if not d:
        LOGGER.warning(f"Invalid datestamp value: {e}")
        return

    LOGGER.info(f"Enumerating blobs in container {container_name} starting with {datestamp}")
    container_client = ContainerClient.from_connection_string(CONN_STR, container_name)
    blob_list = container_client.list_blobs(name_starts_with=datestamp)
    csv_blobs = [blob.name for blob in blob_list]

    if not csv_blobs:  # Nil source data, abort.
        LOGGER.info(f"No source data for datestamp {datestamp}")
        return

    LOGGER.info(f"Archiving logs for datestamp {datestamp}")

    # Use a temporary directory to download CSV logs into.
    csv_dir = TemporaryDirectory()
    downloaded = download_logs(datestamp, csv_dir.name, container_name)
    if not downloaded:
        return

    csv_files = sorted(os.listdir(csv_dir.name))

    # Read each CSV in the directory in as a separate table.
    tables = []
    parse_options = pv.ParseOptions(newlines_in_values=True, invalid_row_handler=invalid_row_handler)
    for csv_file in csv_files:
        # Prepend a header row in each CSV
        path = os.path.join(csv_dir.name, csv_file)
        result = prepend_header_row(path)
        if not result:
            LOGGER.info(f"Skipping {csv_file}")
            continue

        LOGGER.info(f"Loading {csv_file}")
        try:
            table = pv.read_csv(path, parse_options=parse_options)
            tables.append(table)
        except Exception as e:
            LOGGER.warning(f"Exception while loading {csv_file}")
            LOGGER.warning(e)
            return

    # Concat all the tables together.
    combined_table = pa.concat_tables(tables)

    # Output the combined table to a parquet file.
    pq_file = f"{datestamp}.nginx.access.parquet"
    pq_path = os.path.join(csv_dir.name, pq_file)
    LOGGER.info(f"Outputting table to {pq_path}")
    pq.write_table(combined_table, pq_path)

    # Upload the parquet file to the container.
    LOGGER.info(f"Uploading to blob archive/{pq_file}")
    blob_client = BlobClient.from_connection_string(CONN_STR, container_name, f"archive/{pq_file}")
    with open(pq_path, "rb") as source_data:
        blob_client.upload_blob(source_data, overwrite=True)

    # Ensure that the uploaded blob's access tier is 'Cold'.
    blob_client.set_standard_blob_tier("Cold")

    if delete_source:
        # Delete the original remote CSV log files.
        for blob_name in csv_blobs:
            LOGGER.info(f"Deleting blob {blob_name}")
            blob_client = BlobClient.from_connection_string(CONN_STR, container_name, blob_name)
            blob_client.delete_blob(delete_snapshots="include")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""A script to archive CSV-formatted Nginx logs (one file per hour) as Parquet data files (one file per day).
        Logs are moved within the same Blob container to having the prefix `archive`.""",
    )
    parser.add_argument(
        "-d",
        "--datestamp",
        help="A datestamp value in the format YYYYmmdd or YYYY-mm-dd (Fastly logs)",
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
    parser.add_argument(
        "--delete-source",
        help="Delete the source CSV after processing (optional)",
        action="store_true",
        required=False,
    )
    args = parser.parse_args()
    archive_logs(datestamp=args.datestamp, container_name=args.container, delete_source=args.delete_source)
