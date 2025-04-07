import argparse
import os
import subprocess
from tempfile import NamedTemporaryFile, TemporaryDirectory

import crossplane
import unicodecsv as csv
from dotenv import load_dotenv

from utils import configure_logging, upload_log

# Load environment variables.
load_dotenv()
# Assumes an Azure storage connection string is defined as an environment variable.
CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
# Assumes an access token secret is present as an environment variable.
GITHUB_ACCESS_TOKEN = os.getenv("GITHUB_ACCESS_TOKEN")
GITHUB_REPO = os.getenv("GITHUB_REPO")

# Configure logging.
LOGGER = configure_logging()


def clone_repo(dest_path):
    """Clone the GitHub repository to `dest_path`."""
    cmd = f"git clone --depth=1 https://{GITHUB_ACCESS_TOKEN}@{GITHUB_REPO} {dest_path}"
    LOGGER.info(f"Cloning https://{GITHUB_REPO} to {dest_path}")

    try:
        subprocess.check_call(cmd, shell=True)
        return os.listdir(dest_path)
    except subprocess.CalledProcessError as e:
        LOGGER.warning("Error cloning the repository")
        LOGGER.exception(e)
        return False


def parse_nginx_config(dest_path):
    """Parse the Nginx configuration at `dest_path` and return a summary CSV."""
    nginx_conf = crossplane.parse(dest_path)
    config_list = nginx_conf["config"]
    sites_enabled_configs = [c for c in config_list if "nginx/sites-enabled" in c["file"]]
    parsed = []
    for config in sites_enabled_configs:
        parsed += config["parsed"]

    out_csv = NamedTemporaryFile(suffix=".csv")
    LOGGER.info(f"Exporting to {out_csv.name}")
    writer = csv.writer(out_csv)

    for p in parsed:
        block = p["block"]
        server_name = None
        server_name_includes = []
        locations = []
        for part in block:
            if part["directive"] == "server_name":
                server_name = part["args"][0]
            if part["directive"] == "include":
                server_name_includes.append(" ".join(part["args"]))
            if part["directive"] == "location":
                # Skip redirects
                if part["block"][0]["directive"] == "return":
                    continue
                path = " ".join(part["args"])
                includes = []
                for subpart in part["block"]:
                    if subpart["directive"] == "proxy_pass":
                        args = "".join(subpart["args"])
                    if subpart["directive"] == "include":
                        args = " ".join(subpart["args"])
                        if "auth2" in args:
                            includes.append(args)
                locations.append((path, includes))

        for loc in locations:
            writer.writerow([server_name, ", ".join(server_name_includes), loc[0], ", ".join(loc[1])])

    return out_csv


def parse_nginx(container_dest="analytics"):
    """Clone the Nginx config to a temporary directory, parse it and upload the summary to a container."""
    repo_dest = TemporaryDirectory()
    cloned_repo = clone_repo(repo_dest.name)
    if cloned_repo:
        # Parse the Nginx conf to a temporary file.
        nginx_conf = parse_nginx_config(os.path.join(repo_dest.name, "nginx", "nginx.conf"))
        # Upload the temp file to blob storage.
        upload_log(nginx_conf.name, container_dest, CONN_STR, blob_name="dbca_subdomains_auth.csv")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A script to parse and summarise DBCA Nginx configuration.")
    parser.add_argument(
        "-c",
        "--container",
        help="The destination container name (optional, default 'analytics')",
        default="analytics",
        action="store",
        required=False,
    )
    args = parser.parse_args()
    parse_nginx(
        container_dest=args.container,
    )
