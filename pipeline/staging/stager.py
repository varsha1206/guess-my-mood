"""Staging files from GCS to Big Query using Spark"""

import logging
import os
import tempfile

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
    config = None
    spark = None
    sources = ["weather", "music"]

    def fetch_config(self) -> None:
        project_root = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..")
        )
        config_path = os.path.join(project_root, "config.yaml")

        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)
        logger.info("Fetched config")

    def set_up_spark(self)  -> None:
        tmp_dir = os.path.join(tempfile.gettempdir(), "spark_tmp")
        os.makedirs(tmp_dir, exist_ok=True)

        self.spark = (
            SparkSession.builder.appName("Staging")
            .config("spark.executor.memory", "512m")
            .config("spark.driver.memory", "512m")
            .config("spark.sql.shuffle.partitions", "4")
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
            .config(
                "spark.jars.packages",
                "com.google.cloud.spark:spark-3.5-bigquery:0.43.1",
            )
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
        hadoop_conf.set(
            "google.cloud.auth.service.account.json.keyfile",
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
        )
        logger.info("Set up spark")

    def gcs_to_bigquery(self) -> None:
        for source in self.sources:
            logger.info("Processing source %s", source)
            # Read JSON from GCS
            df = self.spark.read.json(self.config["sources"][source]["gcs_path"])
            logger.info("Read %d rows from %s", df.count(), source)

            # Flatten if rules exist for this source
            if source == "music":
                tracks_df = df.select(
                    explode(col("recenttracks.track")).alias("track")
                )  # Select and flatten required fields
                flattened_df = tracks_df.select(
                    col("track.name").alias("track_name"),
                    col("track.streamable").alias("streamable"),
                    col("track.date.#text").alias("played_at"),
                    col("track.date.uts").alias("played_at_uts"),
                    col("track.artist.#text").alias("artist_name"),
                    col("track.album.#text").alias("album_name"),
                )  # Optional: drop duplicates
                df = flattened_df.dropDuplicates()
                df.show(5, truncate=False)

            # Write to BigQuery
            df.write.format("bigquery").option(
                "table", self.config["sources"][source]["big_query_table"]
            ).option("writeMethod", "direct").mode("overwrite").save()

            logger.info(
                "Successfully processed source %s with %d rows", source, df.count()
            )

        self.spark.stop()
