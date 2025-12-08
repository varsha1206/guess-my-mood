"""Ingestin Module for Last FM Music Data"""

import json
import logging
import os
from datetime import datetime, timedelta

import pytz
import requests
import yaml
from google.cloud import storage

from .abstract_ingestion import BaseIngestion

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Music_Ingestion(BaseIngestion):
    """
    Music_Ingestion class for fetching and storing music data from Last.fm API.
    This class extends BaseIngestion and handles the complete pipeline for ingesting
    music listening history from Last.fm, including configuration management, API requests,
    and data storage to Google Cloud Storage.

    Attributes:
        PARAMS (dict): Dictionary containing API request parameters.
        config (dict): Configuration dictionary loaded from config.yaml file.

    Methods:
        fetch_config(): Loads configuration from config.yaml file located in project root.
        configure_params(): Sets up API parameters including timestamp conversion from
            Berlin timezone to UTC for querying yesterday's music data.
        get_data_from_api(): Fetches recent tracks from Last.fm API using configured parameters
            and triggers storage to GCS.
        write_to_gcs(data): Uploads the fetched music data as JSON to Google Cloud Storage bucket.
    """
    PARAMS = {}
    config = None

    def fetch_config(self) -> None:
        project_root = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..")
        )
        config_path = os.path.join(project_root, "config.yaml")
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)
        logger.info("Fetched config")

    def configure_params(self) -> None:
        today_local = datetime.now().astimezone().date()
        yesterday_local = today_local - timedelta(days=1)
        start_local = datetime(
            yesterday_local.year, yesterday_local.month, yesterday_local.day, 0, 0, 0
        )
        end_local = datetime(
            yesterday_local.year, yesterday_local.month, yesterday_local.day, 23, 59, 59
        )

        # convert local Berlin to UTC

        berlin = pytz.timezone("Europe/Berlin")
        start_utc = int(berlin.localize(start_local).astimezone(pytz.UTC).timestamp())
        end_utc = int(berlin.localize(end_local).astimezone(pytz.UTC).timestamp())

        self.PARAMS = {
            "method": "user.getRecentTracks",
            "user": "Varshaww",
            "format": "json",
            "from": start_utc,
            "to": end_utc,
            "limit": 200,
            "api_key": os.getenv("LAST_FM_API_KEY"),
        }

    def get_data_from_api(self) -> None:
        r = requests.get(self.config["sources"]["music"]["api_url"], params=self.PARAMS)
        r.raise_for_status()
        payload = r.json()
        logger.info("Recieved data from last_fm")
        self.write_to_gcs(payload)

    def write_to_gcs(self, data: dict) -> None:
        client = storage.Client()
        bucket = client.bucket(self.config["sources"]["music"]["bucket"])
        bucket.blob(self.config["sources"]["music"]["json_path"]).upload_from_string(
            json.dumps(data), content_type="application/json"
        )
        logger.info("Uploaded %s", self.config["sources"]["music"]["json_path"])
