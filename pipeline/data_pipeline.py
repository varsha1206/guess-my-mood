"""
Main Pipeline for ingesting and staging data
"""

import logging

from ingestion.ingest import run_ingestions
from ingestion.music import Music_Ingestion
from ingestion.weather import Weather_Ingestion
from staging.stager import Staging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """
    Execute the complete data pipeline workflow.
    This function orchestrates the following steps:
    1. Ingests data from multiple sources (Weather and Music)
    2. Stages the ingested data by:
       - Fetching configuration settings
       - Setting up Spark environment
       - Transferring data from Google Cloud Storage to BigQuery
    Logs the progress of each major stage and confirms completion of all processes.
    """
    logger.info("Ingesting Data")
    ingestion_sources = [Weather_Ingestion, Music_Ingestion]
    run_ingestions(ingestion_sources)

    logger.info("Staging Data")
    stg = Staging()
    stg.fetch_config()
    stg.set_up_spark()
    stg.gcs_to_bigquery()

    logger.info("All processes completed")


if __name__ == "__main__":
    main()
