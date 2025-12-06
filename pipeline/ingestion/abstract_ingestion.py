# base_ingestion.py
from abc import ABC, abstractmethod

class BaseIngestion(ABC):
    @abstractmethod
    def configure_params(self):
        pass

    @abstractmethod
    def get_data_from_api(self):
        pass

    @abstractmethod
    def write_to_gcs(self):
        pass
