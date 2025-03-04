import pandas as pd
import logging
from otis_component_core.task.task import task


logger = logging.getLogger(__name__)

class Transformer:
    def __init__(self, config: dict):
        self.config = config

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        if self.config.get("drop_na", False):
            data = data.dropna()
        return data

        filter_config = self.config.get("filter")
        if filter_config:
            column = filter_config.get("column")
            min_value = filter_config.get("min_value")
            if column in data.columns:
                logger.info(f"Filtering rows where {column} >= {min_value}")
                data = data[data[column] >= min_value]
            else:
                logger.warning(f"Column {column} not found. Skipping filtering.")

        rename_config = self.config.get("rename_columns")
        if rename_config:
            logger.info(f"Renaming columns: {rename_config}")
            data = data.rename(columns=rename_config)

        logger.info("Transformation complete.")
        return data