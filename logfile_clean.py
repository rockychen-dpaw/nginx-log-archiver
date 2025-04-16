import argparse
import os
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from azure.storage.blob import BlobClient, ContainerClient
from dotenv import load_dotenv

from utils import configure_logging

# Load environment variables.
load_dotenv()
# Assumes a Azure storage connection string is defined as an environment variable.
CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
UTC = ZoneInfo("UTC")

# Configure logging.
LOGGER = configure_logging()


def delete_logs(container, days, extension, dry_run):
    container_client = ContainerClient.from_connection_string(CONN_STR, container)
    blob_list = container_client.list_blobs()
    # Generate a datetime (UTC) for `days` days ago.
    d = date.today() - timedelta(days=days)
    dt = datetime(d.year, d.month, d.day, 0, 0, 0).astimezone(UTC)
    # Filter the list of blobs to those older than the nomindated datetime.
    log_list = [b for b in blob_list if (b.creation_time < dt and b.name.endswith(extension))]

    for log in log_list:
        blob_client = BlobClient.from_connection_string(CONN_STR, container, log.name)
        if dry_run:
            LOGGER.info(f"Deleting {log.name} (dry run)")
        else:
            LOGGER.info(f"Deleting {log.name}")
            blob_client.delete_blob()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A script to remove logfiles older than x days from a blob container")
    parser.add_argument(
        "-c",
        "--container",
        help="The source container name for logfiles",
        type=str,
        action="store",
        required=True,
    )
    parser.add_argument(
        "-d",
        "--days",
        help="The maximum age (in days) to retain log files",
        type=int,
        action="store",
        required=True,
    )
    parser.add_argument(
        "-e",
        "--extension",
        help="The file extension used for log files (e.g. json, log)",
        type=str,
        action="store",
        required=True,
    )
    parser.add_argument(
        "--dry-run",
        help="Perform a trial run, without making any changes",
        action="store_true",
        required=False,
    )
    args = parser.parse_args()
    delete_logs(
        container=args.container,
        days=args.days,
        extension=args.extension,
        dry_run=args.dry_run,
    )
