import os

from azure.storage.blob import ContainerClient
from dotenv import load_dotenv

load_dotenv()
conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
container_client = ContainerClient.from_connection_string(conn_str, "fastly")
dirs = ["azure-nginx", "ria-ac3"]
prefixes = set()

for d in dirs:
    blob_list = container_client.list_blobs(name_starts_with=d)
    for blob in blob_list:
        name = blob.name.replace(d, "").replace("/", "")
        prefixes.add(name[0:13])

f = open("fastly_log_prefixes.csv", "w")
for prefix in sorted(prefixes):
    f.write(f"{prefix}{os.linesep}")
f.close()
