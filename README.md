# Nginx access log archiver

This project contains a utility for archiving the department's preserved Nginx access logs in the
[Parquet](https://parquet.apache.org/) column-oriented data file format for efficient storage.

A number of OSS libraries exist for interacting with Parquet storage; this project makes use of PyArrow:

- <https://arrow.apache.org/docs/python/index.html>

## Installation

The recommended way to set up this project for development is using
[Poetry](https://python-poetry.org/docs/) to install and manage a virtual Python
environment. With Poetry installed, change into the project directory and run:

    poetry install

Activate the virtualenv like so:

    poetry shell

To run Python commands in the activated virtualenv, thereafter run them as normal:

    python archiver.py --help

## Environment variables

This project uses **python-dotenv** to set environment variables (in a `.env` file).
The following variables are required for the project to run:

    AZURE_STORAGE_CONNECTION_STRING=MySecretAzureStorageAccountConnectionString

## Querying / filtering Parquet files

Example using PyArrow:

```python
import pyarrow.parquet as pq
import pyarrow.compute as pc

table = pq.read_table("nginx_access_logs.parquet")
expr = pc.field("host") == "www.dbca.wa.gov.au"
len(table.filter(expr))
```
