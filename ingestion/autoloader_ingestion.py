from pyspark.sql.types import *
from utils.config_loader import load_config, get_logger

# --------------------------------------------
# Initialize Logger and Config
# --------------------------------------------

logger = get_logger("bronze_ingestion")

config = load_config("../configs/pipeline_config.yaml")

logger.info("Starting Bronze layer ingestion using Databricks Auto Loader")

# --------------------------------------------
# Transaction Schema Definition
# --------------------------------------------

transaction_schema = StructType([
    StructField("transaction_id", IntegerType(), False),
    StructField("user_id", IntegerType(), True),
    StructField("merchant_id", IntegerType(), True),
    StructField("transaction_amount", DoubleType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("transaction_status", StringType(), True),
    StructField("transaction_timestamp", TimestampType(), True),
    StructField("country", StringType(), True),
    StructField("fraud_flag", IntegerType(), True)
])

logger.info("Schema defined for ingestion")

# --------------------------------------------
# Auto Loader Streaming Ingestion
# --------------------------------------------

stream_df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("header", True) \
    .schema(transaction_schema) \
    .load(config["data_path"])

logger.info("Auto Loader streaming source configured")

# --------------------------------------------
# Write to Bronze Delta Table
# --------------------------------------------

query = stream_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", config["checkpoint_path"]) \
    .outputMode("append") \
    .table(config["bronze_table"])

logger.info(f"Streaming ingestion started for Bronze table: {config['bronze_table']}")

# --------------------------------------------
# Monitor Stream
# --------------------------------------------

query.awaitTermination()
