import argparse
import os
import re
from tempfile import TemporaryDirectory

import orjson as json
import unicodecsv as csv
from azure.storage.blob import BlobClient, ContainerClient
from dotenv import load_dotenv

from utils import configure_logging

# Load environment variables.
load_dotenv()
# Assumes a connection string secret present as an environment variable.
CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

# Configure logging.
LOGGER = configure_logging()

# Download access log for the nominated time, for each Nginx host
hosts = ["az-nginx-003", "az-nginx-005"]


def download_json_logs(datestamp, hosts, destination_dir, container_name="access-logs-json"):
    """Given the passed in datestamp, hosts list and destination directory, download logs from blob storage."""
    json_blobs = []
    container_client = ContainerClient.from_connection_string(CONN_STR, container_name)
    hosts_list = hosts.split(",")
    for host in hosts_list:
        # list_blobs returns a list of BlobProperties objects.
        blob_list = container_client.list_blobs(name_starts_with=host)
        for blob in blob_list:
            if datestamp in blob.name:
                json_blobs.append(blob)

    if not json_blobs:  # Nil source data, abort.
        LOGGER.info(f"No source data for datestamp {datestamp}, hosts {hosts}")
        return

    # Download JSON
    for blob in json_blobs:
        host, _ = blob.name.split("/")
        # Download the blob locally with the name <datestamp>.<host>.access.json
        dest_path = os.path.join(destination_dir, f"{datestamp}.{host}.access.json")
        LOGGER.info(f"Downloading blob {blob.name} ({container_name} container) to {dest_path}")
        blob_client = BlobClient.from_connection_string(CONN_STR, container_name, blob.name)

        try:
            with open(dest_path, "wb") as downloaded_blob:
                download_stream = blob_client.download_blob()
                downloaded_blob.write(download_stream.readall())
        except Exception as e:
            LOGGER.error(f"Exception during download of {blob.name}, aborting")
            LOGGER.exception(e)
            return

    return True


def delete_json_logs(datestamp, hosts, container_name="access-logs-json"):
    """Given the passed in datestamp and hosts list, delete blobs from the container."""
    json_blobs = []
    container_client = ContainerClient.from_connection_string(CONN_STR, container_name)
    hosts_list = hosts.split(",")
    for host in hosts_list:
        blob_list = container_client.list_blobs(name_starts_with=host)
        for blob in blob_list:
            if datestamp in blob.name:
                json_blobs.append(blob)

    for blob in json_blobs:
        blob_client = BlobClient.from_connection_string(CONN_STR, container_name, blob.name)
        LOGGER.info(f"Deleting blob {blob.name} from {container_name} container")
        try:
            blob_client.delete_blob()
        except Exception as e:
            LOGGER.error(f"Exception during deletion of {blob.name}, aborting")
            LOGGER.exception(e)
            return


def consolidate_json_access_requests(datestamp, source_dir, destination_dir):
    """Given the passed-in datestamp, source_dir and destination_file, consolidate all access requests
    in JSON logs in the source into a single CSV file in destination."""
    loglist = []
    source_json_files = os.listdir(source_dir)
    source_json_files = [f for f in source_json_files if f.startswith(datestamp)]

    for f in source_json_files:
        source = os.path.join(source_dir, f)
        LOGGER.info(f"Processing {source} access requests")
        log = open(source, "r", errors="replace")
        for line in log.readlines():
            d = json.loads(line)
            if d["source"] == "stdout" and "log" not in d:  # Skips errors and non-Nginx logs.
                try:
                    loglist.append(
                        [
                            d["timestamp"],
                            d["remote_ip"],
                            d["host"],
                            d["uri"],
                            d["query"],
                            d["method"],
                            d["protocol"],
                            int(d["status"]),  # Transform str to int
                            int(float(d["request_time"]) * 1000 * 1000),  # Transform to microseconds
                            int(d["bytes_sent"]),  # Transform str to int
                            re.sub(
                                r"\r|\n|\"|'", "", d["user_agent"]
                            ),  # Remove quotes and newlines in the User-Agent string
                            d["ssouser"],
                        ]
                    )
                except:  # Occasionally, parsing the log dict might throw an exception. Skip this logs.
                    pass

    out_log = os.path.join(destination_dir, f"{datestamp}.nginx.access.csv")
    LOGGER.info(f"Exporting to {out_log}")
    f = open(out_log, "wb")
    writer = csv.writer(f)
    for row in sorted(loglist):
        writer.writerow(row)

    return out_log


def consolidate_json_errors(datestamp, source_dir, destination_dir):
    """Given the passed-in datestamp, source_dir and destination_file, consolidate all errors
    in JSON logs in `source_dir` into a single CSV file in `destination_dir`"""
    loglist = []
    source_json_files = os.listdir(source_dir)
    source_json_files = [f for f in source_json_files if f.startswith(datestamp)]
    pattern = re.compile(r"^(?P<datestamp>[\d\/]+\s[\d:]+)\s\[(?P<level>[a-z]+)\]\s(?P<message>.+$)")

    for f in source_json_files:
        source = os.path.join(source_dir, f)
        LOGGER.info(f"Processing {source} errors")
        log = open(source, "r", errors="replace")
        for line in log.readlines():
            d = json.loads(line)
            if d["source"] == "stderr":  # Only process error logs.
                log_line = d["log"]
                try:
                    data = pattern.match(log_line).groupdict()
                    loglist.append([data["datestamp"], data["level"], data["message"]])
                except:
                    pass

    out_log = os.path.join(destination_dir, f"{datestamp}.nginx.errors.csv")
    LOGGER.info(f"Exporting to {out_log}")
    f = open(out_log, "wb")
    writer = csv.writer(f)
    for row in sorted(loglist):
        writer.writerow(row)

    return out_log


def upload_log(source_path, container_name="access-logs", overwrite=True):
    """
    Upload a single log at `source_path` to Azure blob storage.
    """
    blob_name = os.path.basename(source_path)
    blob_client = BlobClient.from_connection_string(CONN_STR, container_name, blob_name)

    LOGGER.info(f"Uploading {source_path} to {container_name}")
    with open(file=source_path, mode="rb") as data:
        blob_client.upload_blob(data, overwrite=overwrite, validate_content=True)


def consolidate_logs(datestamp, hosts, container_name_json="access-logs-json", delete_source=False):
    # Use a temporary directory to download JSON logs into.
    temp_dir = TemporaryDirectory()
    # Download JSON logs.
    download_json_logs(datestamp, hosts, destination_dir=temp_dir.name, container_name=container_name_json)
    # Consolidate JSON access request logs into one CSV file.
    out_log = consolidate_json_access_requests(datestamp, source_dir=temp_dir.name, destination_dir=temp_dir.name)
    # Upload consolidated CSV log to blob storage.
    upload_log(out_log)
    # Consolidate JSON error logs into one CSV file.
    out_log_errors = consolidate_json_errors(datestamp, source_dir=temp_dir.name, destination_dir=temp_dir.name)
    # Upload consolidated CSV errors to blob storage.
    upload_log(out_log_errors)
    # Optionally deleting JSON logs.
    if delete_source:
        delete_json_logs(datestamp, hosts, container_name=container_name_json)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="A script to consolidate multiple JSON-formatted Nginx log files from different hosts into a single CSV file."
    )
    parser.add_argument(
        "-d",
        "--datestamp",
        help="A datestamp value in the format %%Y%%m%%d%%h",
        action="store",
        required=True,
    )
    parser.add_argument(
        "-s",
        "--hosts",
        help="A comma-seperated list of hostnames to process JSON-formatted logs",
        action="store",
        required=True,
    )
    parser.add_argument(
        "-j",
        "--json-container",
        help="The source blob container name (optional)",
        default="access-logs-json",
        action="store",
        required=False,
    )
    parser.add_argument(
        "-c",
        "--container",
        help="The destination blob container name (optional)",
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
    consolidate_logs(
        datestamp=args.datestamp,
        hosts=args.hosts,
        container_name_json=args.json_container,
        delete_source=args.delete_source,
    )
