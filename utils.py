import logging
import sys


def configure_logging(logfile=None, azure_logfile=None):
    """
    Configure logging (stdout and file) for the default logger and for the `azure` logger.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    if logfile:
        file_handler = logging.FileHandler(logfile)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # Set the logging level for all azure-* libraries (the azure-storage-blob library uses this one).
    # Reference: https://learn.microsoft.com/en-us/azure/developer/python/sdk/azure-sdk-logging
    azure_logger = logging.getLogger("azure")
    azure_logger.setLevel(logging.WARNING)
    if azure_logfile:
        file_handler = logging.FileHandler(azure_logfile)
        file_handler.setLevel(logging.WARNING)
        file_handler.setFormatter(formatter)
        azure_logger.addHandler(file_handler)

    return logger
