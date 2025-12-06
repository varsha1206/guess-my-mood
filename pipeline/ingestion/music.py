# ingest_open_meteo.py
import json
import logging
import os
from datetime import datetime, timedelta

import pytz
import requests
from google.cloud import storage

from abstract_ingestion import BaseIngestion
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Music_Ingestion(BaseIngestion):
    BUCKET = "last_fm_music_data_dm2"
    PARAMS = {}
    URL = "https://ws.audioscrobbler.com/2.0/"
    JSON_PATH = "last_fm_music_data.json"

    def configure_params(self):
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
            "api_key": os.getenv("LAST_FM_API_KEY")
        }

    def get_data_from_api(self):
        r = requests.get("https://ws.audioscrobbler.com/2.0/", params=self.PARAMS)
        r.raise_for_status()
        payload = r.json()
        logger.info("Recieved data from last_fm")
        self.write_to_gcs(payload)

    def write_to_gcs(self, data):
        client = storage.Client()
        bucket = client.bucket(self.BUCKET)
        bucket.blob(self.JSON_PATH).upload_from_string(
            json.dumps(data), content_type="application/json"
        )
        logger.info("Uploaded %s",self.JSON_PATH)
