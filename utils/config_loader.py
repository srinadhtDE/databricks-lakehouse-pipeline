import yaml
import logging
import os

def load_config(path: str):
    with open(path, "r") as file:
        return yaml.safe_load(file)

def get_logger(name: str):

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:

        stream_handler = logging.StreamHandler()

        file_handler = logging.FileHandler(
            os.getenv("PIPELINE_LOG_FILE", "pipeline.log")
        )

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )

        stream_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)

        logger.addHandler(stream_handler)
        logger.addHandler(file_handler)

    return logger
