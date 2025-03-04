import yaml
import logging
import os

def load_config(config_path: str) -> dict:
    """Loads a YAML configuration file."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file {config_path} not found.")
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
    return config

def setup_logging(log_level=logging.INFO):
    """Sets up the logging configuration."""
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )