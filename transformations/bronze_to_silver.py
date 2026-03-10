from pyspark.sql.functions import col, to_date
from utils.config_loader import load_config, get_logger

logger = get_logger("bronze_to_silver")

config = load_config("../configs/pipeline_config.yaml")

logger.info("Starting Bronze to Silver transformation")

bronze_df = spark.table(config["bronze_table"])

# --------------------------------------------
# Schema Validation
# --------------------------------------------

expected_columns = [
    "transaction_id",
    "user_id",
    "merchant_id",
    "transaction_amount",
    "transaction_type",
    "payment_method",
    "transaction_status",
    "transaction_timestamp",
    "country",
    "fraud_flag"
]

missing_columns = [c for c in expected_columns if c not in bronze_df.columns]

if missing_columns:
    logger.error(f"Missing columns detected: {missing_columns}")
    raise ValueError(f"Schema validation failed. Missing columns: {missing_columns}")

logger.info("Schema validation passed")

# --------------------------------------------
# Data Cleaning
# --------------------------------------------

clean_df = bronze_df \
    .dropDuplicates(["transaction_id"]) \
    .filter(col("transaction_status") == "completed") \
    .filter(col("transaction_amount") > 0) \
    .withColumn("transaction_date", to_date(col("transaction_timestamp")))

logger.info("Data cleaning completed")

# --------------------------------------------
# Write Silver Table
# --------------------------------------------

clean_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("transaction_date") \
    .saveAsTable(config["silver_table"])

logger.info("Silver layer table successfully created")
