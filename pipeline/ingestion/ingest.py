from music import Music_Ingestion
from weather import Weather_Ingestion
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_ingestions(ingestions):
    for ingestion_class in ingestions:
        ingestion = ingestion_class()
        ingestion.configure_params()
        ingestion.get_data_from_api()

if __name__ == "__main__":
    # Add more ingestion sources here as you add them
    ingestion_sources = [Weather_Ingestion, Music_Ingestion]
    run_ingestions(ingestion_sources)


