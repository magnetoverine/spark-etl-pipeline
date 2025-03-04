import logging
from .extractor import Extractor
from .transformer import Transformer
from .loader import Loader


logger = logging.getLogger(__name__)

class EtlPipeline:
    @classmethod
    def create(cls, config: dict) -> "EtlPipeline":
        instance = cls()
        etl_config = config.get("etl", {})

        # Setup Extractor
        extract_config = etl_config.get("extract", {})
        input_files = extract_config.get("input_files", [])
        if not input_files:
            raise ValueError("No input files specified in the configuration.")
        instance.extractor = Extractor(input_files)

        # Setup Transformer
        transform_config = etl_config.get("transform", {})
        instance.transformer = Transformer(transform_config)

        # Setup Loader
        load_config = etl_config.get("load", {})
        output_file = load_config.get("output_file")
        if not output_file:
            raise ValueError("No output file specified in the configuration.")
        instance.loader = Loader(output_file)
        return instance

    def run(self) -> OtisData:
        """Executes the full ETL pipeline."""
        logger.info("Starting ETL pipeline.")
        data = self.extractor.extract()
        transformed_data = self.transformer.transform(data)
        self.loader.load(transformed_data)
