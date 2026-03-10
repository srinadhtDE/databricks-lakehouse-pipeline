from pyspark.sql import SparkSession
from utils.config_loader import load_config, get_logger

logger = get_logger("bronze_ingestion")

config = load_config("../configs/pipeline_config.yaml")

spark = SparkSession.builder \
    .appName("bronze_ingestion_pipeline") \
    .getOrCreate()

logger.info("Starting Bronze ingestion pipeline")

try:

    df = spark.read \
        .option("header", True) \
        .csv(config["data_path"])

    logger.info(f"Loaded raw dataset with {df.count()} records")

    df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(config["bronze_table"])

    logger.info("Bronze table successfully created")

except Exception as e:

    logger.error("Bronze ingestion failed")
    logger.error(str(e))
    raise
