import json
import logging
import os
import tempfile
from datetime import datetime, timedelta

import pandas as pd
import pytz
import requests
from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

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
        ingestion.configure_params()
        ingestion.get_data_from_api()


class Music_Ingestion:
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
            "api_key": "22e9ae9e2ba74c23ab34e3ac2cd98823",
        }

    def get_data_from_api(self) -> None:
        r = requests.get("https://ws.audioscrobbler.com/2.0/", params=self.PARAMS)
        r.raise_for_status()
        payload = r.json()
        logger.info("Recieved data from last_fm")
        self.write_to_gcs(payload)

    def write_to_gcs(self, data: dict) -> None:
        client = storage.Client()
        bucket = client.bucket("last_fm_music_data_dm2")
        bucket.blob("last_fm_music_data.json").upload_from_string(
            json.dumps(data), content_type="application/json"
        )
        logger.info("Uploaded last_fm_music_data.json")


class Weather_Ingestion:
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

    def configure_params(self) -> None:
        self.PARAMS = {
            "latitude": 49.3988,
            "longitude": 8.6724,
            "hourly": "temperature_2m,apparent_temperature,cloudcover,relativehumidity_2m,precipitation,weathercode",
            "start_date": self.YESTERDAY.isoformat(),
            "end_date": self.YESTERDAY.isoformat(),
            "timezone": "Europe/Berlin",
        }

    def get_data_from_api(self) -> None:
        response = requests.get(
            "https://historical-forecast-api.open-meteo.com/v1/forecast",
            params=self.PARAMS,
            timeout=10,
        )

        response.raise_for_status()
        data = response.json()

        # Extract hourly data
        hourly = data["hourly"]
        hourly_df = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(hourly["time"], utc=True),
                "temperature_2m": hourly["temperature_2m"],
                "weathercode": hourly["weathercode"],
                "relativehumidity_2m": hourly["relativehumidity_2m"],
                "cloudcover": hourly["cloudcover"],
                "apparent_temperature": hourly["apparent_temperature"],
                "precipitation": hourly["precipitation"],
            }
        )
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
        bucket = client.bucket("open_meteo_weather_data_dm2")

        bucket.blob("open_meteo_weather_data.json").upload_from_string(
            json.dumps(payload), content_type="application/json"
        )
        logger.info("Uploaded open_meteo_weather_data.json")


class Staging:
    """
    Staging class for processing data from various sources to BigQuery.
    This class handles the complete ETL pipeline for staging data from multiple sources
    (weather, music) stored in Google Cloud Storage (GCS) to Google BigQuery. It manages
    Spark session configuration, GCS connectivity, data transformation, and data loading.

    Attributes:
        config (dict): Configuration dictionary loaded from config.yaml file.
        spark (SparkSession): Spark session instance for distributed data processing.
        sources (list): List of data sources to process. Currently supports "weather" and "music".

    Methods:
        fetch_config(): Loads configuration from config.yaml file located at project root.
        set_up_spark(): Initializes and configures Spark session with GCS connectivity,
            including Hadoop configuration for Google Cloud Storage access.
        gcs_to_bigquery(): Reads JSON data from GCS for each source, applies source-specific
            transformations (flattening for music data), removes duplicates, and writes
            the processed data to BigQuery tables.
    """

    spark = None
    sources = ["weather", "music"]

    def set_up_spark(self) -> None:
        tmp_dir = os.path.join(tempfile.gettempdir(), "spark_tmp")
        os.makedirs(tmp_dir, exist_ok=True)

        self.spark = (
            SparkSession.builder.appName("Staging")
            .config("spark.executor.memory", "2g")    # 1.8GB
        .config("spark.driver.memory", "1g")       # 900MB  
        # .config("spark.driver.maxResultSize", "512m")
        .config("spark.sql.shuffle.partitions", "4")   # Reduce from 4 to 2
        .config("spark.local.dir", tmp_dir)
            # .config(
            #     "spark.jars",
            #     ",".join(
            #         [
            #             r"C:\Program Files\spark-3.5.5-bin-hadoop3\spark-3.5.5-bin-hadoop3\jars\gcs-connector-hadoop3-2.2.9-shaded.jar",
            #             r"C:\Program Files\spark-3.5.5-bin-hadoop3\spark-3.5.5-bin-hadoop3\jars\guice-5.1.0.jar",
            #             r"C:\Program Files\spark-3.5.5-bin-hadoop3\spark-3.5.5-bin-hadoop3\jars\javax.inject-1.jar",
            #         ]
            #     ),
            # )
            # .config(
            #     "spark.jars.packages",
            #     "com.google.cloud.spark:spark-3.5-bigquery:0.43.1",
            # )
            # GARBAGE COLLECTION OPTIMIZATION
            .config("spark.executor.extraJavaOptions", 
                "-XX:+UseG1GC -XX:+UnlockExperimentalVMOptions "
                "-XX:G1NewSizePercent=20 -XX:G1ReservePercent=20 "
                "-XX:MaxGCPauseMillis=50 -XX:G1HeapRegionSize=16M")
            .getOrCreate()
        )

        # Hadoop config for GCS
        hadoop_conf = self.spark._jsc.hadoopConfiguration()
        hadoop_conf.set(
            "fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
        )
        hadoop_conf.set(
            "fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        )
        hadoop_conf.set("fs.gs.auth.service.account.enable", "true")
        logger.info("Set up spark")

    # def gcs_to_bigquery(self) -> None:
    #     # Process weather data first
    #     logger.info("Processing source weather")
    #     weather_df = self.spark.read.json("gs://open_meteo_weather_data_dm2/*.json")
    #     logger.info("Read %d rows from weather", weather_df.count())
        
    #     # Process music data second
    #     logger.info("Processing source music")
    #     music_df = self.spark.read.json("gs://last_fm_music_data_dm2/*.json")
    #     logger.info("Read %d rows from music", music_df.count())
        
    #     # Flatten music data
    #     tracks_df = music_df.select(
    #         explode(col("recenttracks.track")).alias("track")
    #     )
    #     flattened_df = tracks_df.select(
    #         col("track.name").alias("track_name"),
    #         col("track.streamable").alias("streamable"),
    #         col("track.date.#text").alias("played_at"),
    #         col("track.date.uts").alias("played_at_uts"),
    #         col("track.artist.#text").alias("artist_name"),
    #         col("track.album.#text").alias("album_name"),
    #     )
        
    #     # Remove duplicates
    #     final_music_df = flattened_df.dropDuplicates()
        
    #     # DEBUG: Log music data without show()
    #     logger.info("Music data count: %d", final_music_df.count())

    #     # Write weather data to BigQuery
    #     weather_df.write.format("bigquery").option(
    #         "table", "guess_my_mood_dataset.stg_weather_data"
    #     ).option("writeMethod", "direct").mode("overwrite").save()
        
    #     logger.info("Successfully processed weather with %d rows", weather_df.count())
                
    #     # Write music data to BigQuery
    #     final_music_df.write.format("bigquery").option(
    #         "table", "guess_my_mood_dataset.stg_music_data"
    #     ).option("writeMethod", "direct").mode("overwrite").save()
        
    #     logger.info("Successfully processed music with %d rows", final_music_df.count())
        
    #     # Stop Spark at the VERY END
    #     self.spark.stop()
    def gcs_to_bigquery(self) -> None:
        import traceback  # Add this at the top of the method
        
        try:
            logger.info("=== Starting BigQuery Load ===")
            
            # Process weather data
            logger.info("Processing source weather")
            weather_df = self.spark.read.json("gs://open_meteo_weather_data_dm2/*.json")
            logger.info("Read weather data")
            weather_count = weather_df.count()
            logger.info("Read %d rows from weather", weather_count)
            logger.info("Weather Schema: %s", weather_df.schema)
            
            # Write weather data to BigQuery
            logger.info("Writing weather data to BigQuery...")
            weather_df.write.format("bigquery") \
                .option("table", "guess_my_mood_dataset.stg_weather_data") \
                .option("writeMethod", "direct") \
                .mode("overwrite") \
                .save()
            
            logger.info("Successfully processed weather with %d rows", weather_count)
            
            # Process music data
            logger.info("Processing source music")
            music_df = self.spark.read.json("gs://last_fm_music_data_dm2/*.json")
            music_count = music_df.count()
            logger.info("Read %d rows from music", music_count)
            logger.info("Music Schema: %s", music_df.schema)
            
            if music_count > 0:
                # Flatten music data
                logger.info("Flattening music data...")
                tracks_df = music_df.select(
                    explode(col("recenttracks.track")).alias("track")
                )
                flattened_df = tracks_df.select(
                    col("track.name").alias("track_name"),
                    col("track.streamable").alias("streamable"),
                    col("track.date.#text").alias("played_at"),
                    col("track.date.uts").alias("played_at_uts"),
                    col("track.artist.#text").alias("artist_name"),
                    col("track.album.#text").alias("album_name"),
                )
                
                # Remove duplicates
                final_music_df = flattened_df.dropDuplicates()
                final_count = final_music_df.count()
                logger.info("Music data count after dedup: %d", final_count)
                
                # Write music data to BigQuery
                logger.info("Writing music data to BigQuery...")
                final_music_df.write.format("bigquery") \
                .option("table", "guess_my_mood_dataset.stg_music_data") \
                .option("writeMethod", "direct") \
                .mode("overwrite") \
                .save()
                
                logger.info("Successfully processed music with %d rows", final_count)
            else:
                logger.warning("No music data to process")
                
            logger.info("=== BigQuery Load Complete ===")
            
        except Exception as e:
            logger.error("ERROR in gcs_to_bigquery: %s", str(e))
            logger.error("Traceback: %s", traceback.format_exc())
            raise
        finally:
            # IMPORTANT: Stop Spark in finally block
            try:
                if self.spark:
                    logger.info("Stopping Spark session...")
                    self.spark.stop()
                    logger.info("Spark session stopped")
            except Exception as e:
                logger.error("Error stopping Spark: %s", str(e))


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
    stg.set_up_spark()
    stg.gcs_to_bigquery()

    logger.info("All processes completed")


if __name__ == "__main__":
    main()
