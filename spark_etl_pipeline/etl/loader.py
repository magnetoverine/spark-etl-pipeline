import pandas as pd
import logging
from otis_component_core.task.task import task
from otis_specification.common import OtisData

logger = logging.getLogger(__name__)

class Loader:
    def __init__(self, output_path: str):
        self.output_path = output_path

    def load(self, data: pd.DataFrame) -> None:
        """Loads data by writing it to a CSV file."""
        try:
            data.to_csv(self.output_path, index=False)
            logger.info(f"Data successfully loaded to {self.output_path}")
        except Exception as e:
            logger.error(f"Error writing data to {self.output_path}: {e}")
            raise