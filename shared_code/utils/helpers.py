import logging
import os
import shutil
import requests
from requests.adapters import HTTPAdapter, Retry

# Delete path if exists and recreate it
def check_if_path_exists(path):
    """
    Check if the specified path exists.
    If it exists, delete it and recreate it.
    """
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path)

    return True

def setup_logging():
    """
    Set up logging configuration.
    """
    # Customize the logging format for all loggers
    FORMAT = "%(asctime)s UTC - %(levelname)s - %(message)s (%(name)s)"
    formatter = logging.Formatter(fmt=FORMAT)
    for handler in logging.getLogger().handlers:
        handler.setFormatter(formatter)

    # Customize log level for all loggers
    logging.getLogger().setLevel(logging.INFO)

def config_requests(retry_limit=5, retry_delay=2):
    """
    Set up requests configuration with things like retry logic.
    """
    s = requests.Session()

    retries = Retry(total=retry_limit,
                backoff_factor=retry_delay,
                status_forcelist= [ 
                    #400, 401, 402, 403, 404, 
                    500, 502, 503, 504 
                ]
            )

    s.mount('http://', HTTPAdapter(max_retries=retries))

    return s

# Set up logging configuration during import
setup_logging()