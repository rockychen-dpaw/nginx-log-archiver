import os

import unicodecsv as csv
from azure.storage.blob import BlobClient, ContainerClient
from dotenv import load_dotenv

from utils import get_blob_client, upload_log

load_dotenv()
CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER = "fastly"


def download_blob(blob_name: str, dest_path: str):
    blob_client = get_blob_client(CONN_STR, CONTAINER, blob_name)
    downloaded_blob = open(dest_path, "wb")
    download_stream = blob_client.download_blob()
    downloaded_blob.write(download_stream.readall())
    print(f"Downloaded {blob_name} to {dest_path}")
    downloaded_blob.close()


def consolidate_logs(timestamps: list):
    """Consolidate Fastly logs according to the passed-in list of timestamp prefixes.
    Timestamps will be strings in the format YYYY-mm-ddThh, in order to one single hour
    of logs at a time."""
    container_client = ContainerClient.from_connection_string(CONN_STR, CONTAINER)
    dirs = ["azure-nginx", "ria-ac3"]
    blob_dest_dir = "/tmp/blobs"
    csv_dest_dir = "/tmp/csv"

    for timestamp in timestamps:
        print(f"Processing for {timestamp}")
        blobs_ts = []

        for d in dirs:
            path = os.path.join(blob_dest_dir, d)
            if not os.path.exists(path):
                os.mkdir(path)
            prefix = f"{d}/{timestamp}"
            blob_list = container_client.list_blobs(name_starts_with=prefix)
            blobs_ts += [b.name for b in blob_list]

            # Download blobs locally for processing.
            for blob_name in blobs_ts:
                dest_path = os.path.join(blob_dest_dir, blob_name)
                download_blob(blob_name, dest_path)

        loglist = []
        source_logs = []
        for root, _, files in os.walk(blob_dest_dir):
            for filename in files:
                source_logs.append(os.path.join(root, filename))

        for f in source_logs:
            print(f"Processing {f}")
            if f.endswith(".log"):
                log = open(f, "rb")
                reader = csv.UnicodeReader(log)
                for row in reader:
                    loglist.append([row[0], row])
                log.close()

        if loglist:
            # Local output CSV.
            out_csv = os.path.join(csv_dest_dir, f"{timestamp}:00:00Z.nginx.access.csv")
            print(f"Exporting to {out_csv}")
            out_csv = open(out_csv, "wb")
            writer = csv.writer(out_csv)
            for row in sorted(loglist):
                if len(row[1]) == 12:
                    writer.writerow(row[1])
            out_csv.close()
            print(f"Uploading {out_csv} to {CONTAINER}")
            upload_log(out_csv, CONTAINER, CONN_STR)
        else:
            print(f"No data for {timestamp}")

        # Delete remote blobs which have been consolidated.
        for blob_name in blobs_ts:
            print(f"Deleting blob {blob_name}")
            blob_client = BlobClient.from_connection_string(CONN_STR, CONTAINER, blob_name)
            blob_client.delete_blob(delete_snapshots="include")

        # Delete downloaded blobs.
        for f in source_logs:
            print(f"Deleting {f}")
            os.remove(f)
