import argparse
import logging
from etl.pipeline import EtlPipeline
from etl.utils import load_config, setup_logging

def main():
    parser = argparse.ArgumentParser(description="Run the ETL pipeline.")
    parser.add_argument("--config", type=str, default="spark_etl_pipeline/config/config.yaml", help="Path to the configuration file.")
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        config = load_config(args.config)
        pipeline = EtlPipeline.create(config)
        pipeline.run()
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")

if __name__ == "__main__":
    main()