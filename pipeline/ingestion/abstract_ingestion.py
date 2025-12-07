"""Abstact class for Ingesting data from various sources"""
from abc import ABC, abstractmethod

class BaseIngestion(ABC):
    """
    Abstract base class for data ingestion operations.
    
    This class defines the interface for implementing data ingestion pipelines
    that fetch data from external APIs and persist it to Google Cloud Storage (GCS).
    
    All subclasses must implement the abstract methods:
    - fetch_config: Load configuration settings
    - configure_params: Set up pipeline parameters
    - get_data_from_api: Retrieve data from external source
    - write_to_gcs: Store data to GCS
    """
    @abstractmethod
    def fetch_config(self):
        pass

    @abstractmethod
    def configure_params(self):
        pass

    @abstractmethod
    def get_data_from_api(self):
        pass

    @abstractmethod
    def write_to_gcs(self):
        pass
