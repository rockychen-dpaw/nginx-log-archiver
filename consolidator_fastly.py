import argparse
import os
from tempfile import TemporaryDirectory

import unicodecsv as csv
from dotenv import load_dotenv

from utils import configure_logging, download_logs, upload_log

# Load environment variables.
load_dotenv()
# Assumes a connection string secret present as an environment variable.
CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

# Configure logging.
LOGGER = configure_logging()


def consolidate_logfiles(timestamp, source_dir, destination_dir):
    """Given the passed-in timestamp, source_dir and destination_file, consolidate all access requests
    in logs in the source into a single CSV file in destination.
    """
    loglist = []
    source_logs = [f for f in os.listdir(source_dir) if f.startswith(timestamp)]

    for f in source_logs:
        source = os.path.join(source_dir, f)
        LOGGER.info(f"Processing {source} access requests")
        if f.startswith(timestamp) and f.endswith(".log"):
            # Read the log file into memory.
            log = open(os.path.join(source_dir, f), "rb")
            reader = csv.UnicodeReader(log)
            for row in reader:
                # Get the timestamp to sort on, plus the entire row.
                loglist.append([row[0], row])
            log.close()

    out_log = os.path.join(destination_dir, f"{timestamp}:00:00Z.nginx.access.csv")
    LOGGER.info(f"Exporting to {out_log}")
    f = open(out_log, "wb")
    writer = csv.writer(f)
    for row in sorted(loglist):
        if len(row[1]) < 12:  # Skip badly-parsed rows.
            continue
        else:
            # Just append the row, not the timestamp value.
            writer.writerow(row[1])

    # Return the path to the consolidated log.
    return out_log


def consolidate_fastly_logs(timestamp, services, container_src="fastly", container_dest="fastly"):
    """Download logs for the specified timestamp and services, consolidate and upload the source logs."""
    # Use a temporary directory to download logs.
    temp_dir = TemporaryDirectory()
    # Download Fastly logs.
    downloads = download_logs(timestamp, services, temp_dir.name, container_src, CONN_STR)
    if downloads:
        # Consolidate access request logs into one CSV file.
        out_log = consolidate_logfiles(timestamp, temp_dir.name, temp_dir.name)
        # Upload consolidated CSV log to blob storage.
        upload_log(out_log, container_dest, CONN_STR)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="A script to consolidate multiple Nginx log files from different Fastly services into a single CSV file."
    )
    parser.add_argument(
        "-t",
        "--timestamp",
        help="A timestamp value to the nearest hour in the format YYYY-mm-ddTHH",
        action="store",
        required=True,
    )
    parser.add_argument(
        "-s",
        "--services",
        help="A comma-seperated list of services for which to process logs",
        action="store",
        required=True,
    )
    parser.add_argument(
        "-c",
        "--container",
        help="The source logs container name (optional, default 'fastly')",
        default="fastly",
        action="store",
        required=False,
    )
    parser.add_argument(
        "-d",
        "--destination-container",
        help="The destination container name (optional, default 'fastly')",
        default="fastly",
        action="store",
        required=False,
    )
    args = parser.parse_args()
    consolidate_fastly_logs(
        timestamp=args.timestamp,
        services=args.services,
        container_src=args.container,
        container_dest=args.destination_container,
    )
