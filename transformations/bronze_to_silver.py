from pyspark.sql.functions import col, to_date
from utils.config_loader import load_config, get_logger

# --------------------------------------------
# Initialize Logger and Configuration
# --------------------------------------------

logger = get_logger("bronze_to_silver")

config = load_config("../configs/pipeline_config.yaml")

logger.info("Starting Bronze → Silver transformation")

# --------------------------------------------
# Load Bronze Table
# --------------------------------------------

bronze_df = spark.table(config["bronze_table"])

logger.info(f"Bronze table loaded: {config['bronze_table']}")

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
    logger.error(f"Schema validation failed. Missing columns: {missing_columns}")
    raise ValueError(f"Missing columns detected: {missing_columns}")

logger.info("Schema validation passed")

# --------------------------------------------
# Data Quality Checks
# --------------------------------------------

logger.info("Applying data quality filters")

clean_df = bronze_df \
    .dropDuplicates(["transaction_id"]) \
    .filter(col("transaction_status") == "completed") \
    .filter(col("transaction_amount") > 0)

logger.info("Duplicate removal and status filtering complete")

# --------------------------------------------
# Feature Engineering
# --------------------------------------------

clean_df = clean_df.withColumn(
    "transaction_date",
    to_date(col("transaction_timestamp"))
)

logger.info("Derived column transaction_date created")

# --------------------------------------------
# Record Count Monitoring
# --------------------------------------------

record_count = clean_df.count()

logger.info(f"Total records in Silver layer: {record_count}")

# --------------------------------------------
# Write Silver Table
# --------------------------------------------

logger.info("Writing Silver Delta table")

clean_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("transaction_date") \
    .saveAsTable(config["silver_table"])

logger.info(f"Silver table created: {config['silver_table']}")

# --------------------------------------------
# Delta Lake Optimization
# --------------------------------------------

logger.info("Running Delta optimization")

spark.sql(f"OPTIMIZE {config['silver_table']}")

spark.sql(f"VACUUM {config['silver_table']} RETAIN 168 HOURS")

logger.info("Delta optimization completed")

logger.info("Bronze → Silver pipeline completed successfully")
