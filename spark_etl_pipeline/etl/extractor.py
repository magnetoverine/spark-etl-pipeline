import pandas as pd
import logging
from otis_component_core.task.task import task


logger = logging.getLogger(__name__)

class Extractor:
    def __init__(self, file_paths: list):
        self.file_paths = file_paths

    def extract(self) -> pd.DataFrame:
        """Extracts data from a list of CSV files and concatenates them."""
        dataframes = []
        for path in self.file_paths:
            logger.info(f"Extracting data from {path}")
            try:
                df = pd.read_csv(path)
                dataframes.append(df)
            except Exception as e:
                logger.error(f"Error reading {path}: {e}")
                raise
        combined_df = pd.concat(dataframes, ignore_index=True)
        logger.info("Extraction complete.")
        return combined_df