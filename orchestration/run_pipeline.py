import subprocess
from utils.config_loader import get_logger

logger = get_logger("pipeline_runner")

logger.info("Starting end-to-end pipeline")

steps = [
    "../ingestion/autoloader_ingestion.py",
    "../transformations/bronze_to_silver.py",
    "../transformations/silver_to_gold.py",
    "../quality_checks/checks.py"
]

for step in steps:

    logger.info(f"Running step: {step}")
    subprocess.run(["python", step], check=True)

logger.info("Pipeline completed successfully")
