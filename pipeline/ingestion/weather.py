"""Ingestion Module for Open Meteo Weather Data"""

import json
import logging
import os
from datetime import datetime, timedelta

import openmeteo_requests
import pandas as pd
import requests_cache
import yaml
from google.cloud import storage
from retry_requests import retry

from .abstract_ingestion import BaseIngestion

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Weather_Ingestion(BaseIngestion):
    """
    Weather_Ingestion class for fetching and storing weather data.
    This class extends BaseIngestion and handles the complete workflow of fetching weather data
    from the Open-Meteo API, processing hourly weather information, and uploading the results to
    Google Cloud Storage.

    Attributes:
        YESTERDAY (date): The date from the previous day, used for API queries.
        PARAMS (dict): Parameters to be sent to the Open-Meteo API.
        config (dict): Configuration dictionary loaded from config.yaml file.

    Methods:
        fetch_config(): Loads configuration settings from the config.yaml file located in the project root.
        configure_params(): Initializes PARAMS with latitude, longitude, hourly metrics, date range,
                           timezone, and other required parameters for the Open-Meteo API call.
        get_data_from_api(): Fetches weather data from the Open-Meteo API using a cached session with
                            retry logic. Extracts hourly weather variables including temperature,
                            weather code, humidity, cloud cover, and wind speed. Creates a pandas
                            DataFrame with timestamps and uploads it to GCS.
        write_to_gcs(dataframe): Converts the pandas DataFrame to JSON format with ISO-formatted
                                timestamps and uploads it to the specified Google Cloud Storage bucket.
    """
    YESTERDAY = datetime.now().astimezone().date() - timedelta(days=1)
    PARAMS = {}
    config = None

    def fetch_config(self) -> None:
        project_root = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..")
        )
        config_path = os.path.join(project_root, "config.yaml")
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)
        logger.info("Fetched config")

    def configure_params(self) -> None:
        self.PARAMS = {
            "latitude": self.config["sources"]["weather"]["latitude"],
            "longitude": self.config["sources"]["weather"]["longitude"],
            "hourly": "temperature_2m,apparent_temperature,cloudcover,relativehumidity_2m,precipitation,weathercode",
            "start_date": self.YESTERDAY.isoformat(),
            "end_date": self.YESTERDAY.isoformat(),
            "timezone": self.config["sources"]["weather"]["timezone"],
        }

    def get_data_from_api(self) -> None:
        # Setup the Open-Meteo API client with cache and retry on error
        cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        openmeteo = openmeteo_requests.Client(session=retry_session)

        responses = openmeteo.weather_api(
            self.config["sources"]["weather"]["api_url"], params=self.PARAMS
        )

        response = responses[0]
        hourly = response.Hourly()

        temperature_2m = hourly.Variables(0).ValuesAsNumpy()
        weathercode = hourly.Variables(1).ValuesAsNumpy()
        humidity = hourly.Variables(2).ValuesAsNumpy()
        cloudcover = hourly.Variables(3).ValuesAsNumpy()
        windspeed_10m = hourly.Variables(4).ValuesAsNumpy()

        # Create hourly timestamps
        hourly_data = {
            "timestamp": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left",
            ),
            "temperature_2m": temperature_2m,
            "weathercode": weathercode,
            "relativehumidity_2m": humidity,
            "cloudcover": cloudcover,
            "windspeed_10m": windspeed_10m,
        }

        # Final dataframe
        hourly_df = pd.DataFrame(hourly_data)
        logger.info(
            "Length of hourly data received for %s is %s",
            self.YESTERDAY,
            len(hourly_df),
        )
        self.write_to_gcs(hourly_df)

    def write_to_gcs(self, dataframe: pd.DataFrame) -> None:
        logger.info("Writing to GCS")
        # Convert Timestamp to ISO string for JSON
        payload = dataframe.copy()
        payload["timestamp"] = payload["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        # Convert to list of dicts
        payload = payload.to_dict(orient="records")
        # Convert pandas DF to Spark DF
        client = storage.Client()
        bucket = client.bucket(self.config["sources"]["weather"]["bucket"])

        bucket.blob(self.config["sources"]["weather"]["json_path"]).upload_from_string(
            json.dumps(payload), content_type="application/json"
        )
        logger.info("Uploaded %s", self.config["sources"]["weather"]["json_path"])
