# ingest_open_meteo.py
import json
import logging
from datetime import datetime, timedelta

import openmeteo_requests
import pandas as pd
import requests_cache
from google.cloud import storage
from retry_requests import retry

from abstract_ingestion import BaseIngestion
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Weather_Ingestion(BaseIngestion):
    BUCKET = "open_meteo_weather_data_dm2"
    LAT = 49.3988
    LON =  8.6724 
    TIMEZONE = "Europe/Berlin"
    YESTERDAY = datetime.now().astimezone().date() - timedelta(days=1)
    PARAMS = {}
    URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"
    JSON_PATH = "open_meteo_weather_data.json"

    def configure_params(self):
        self.PARAMS = {
            "latitude": self.LAT,
            "longitude": self.LON,
            "hourly": "temperature_2m,apparent_temperature,cloudcover,relativehumidity_2m,precipitation,weathercode",
            "start_date": self.YESTERDAY.isoformat(),
            "end_date": self.YESTERDAY.isoformat(),
            "timezone": self.TIMEZONE,
        }

    def get_data_from_api(self):
        # Setup the Open-Meteo API client with cache and retry on error
        cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        openmeteo = openmeteo_requests.Client(session=retry_session)

        responses = openmeteo.weather_api(self.URL, params=self.PARAMS)

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
            "Length of hourly data received for %s is %s", self.YESTERDAY, len(hourly_df)
        )
        self.write_to_gcs(hourly_df)

    def write_to_gcs(self,dataframe):
        logger.info("Writing to GCS")
        # Convert Timestamp to ISO string for JSON
        payload = dataframe.copy()
        payload["timestamp"] = payload["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        # Convert to list of dicts
        payload = payload.to_dict(orient="records")
        # Convert pandas DF to Spark DF
        client = storage.Client()
        bucket = client.bucket(self.BUCKET)

        bucket.blob(self.JSON_PATH).upload_from_string(json.dumps(payload), content_type="application/json")
        logger.info("Uploaded %s", self.JSON_PATH)
