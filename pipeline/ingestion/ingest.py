"""Ingestion pipeline"""

import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_ingestions(ingestions):
    """
    Execute a series of data ingestion operations.

    Iterates through a list of ingestion classes, instantiates each one,
    and runs the ingestion pipeline by fetching configuration, configuring
    parameters, and retrieving data from the API.

    Args:
        ingestions (list): A list of ingestion class types

    Returns:
        None
    """
    for ingestion_class in ingestions:
        ingestion = ingestion_class()
        ingestion.fetch_config()
        ingestion.configure_params()
        ingestion.get_data_from_api()
