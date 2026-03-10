
from pyspark.sql.functions import col, hour, dayofweek, when
from utils.config_loader import load_config, get_logger

# --------------------------------------------
# Initialize Logger and Config
# --------------------------------------------

logger = get_logger("feature_engineering")

config = load_config("../configs/pipeline_config.yaml")

logger.info("Starting feature engineering pipeline for fraud detection")

# --------------------------------------------
# Load Silver Table
# --------------------------------------------

silver_df = spark.table(config["silver_table"])

logger.info(f"Loaded Silver table: {config['silver_table']}")

# --------------------------------------------
# Schema Validation
# --------------------------------------------

expected_columns = [
    "transaction_id",
    "user_id",
    "merchant_id",
    "transaction_amount",
    "transaction_timestamp",
    "fraud_flag"
]

missing_columns = [c for c in expected_columns if c not in silver_df.columns]

if missing_columns:
    logger.error(f"Schema validation failed. Missing columns: {missing_columns}")
    raise ValueError(f"Missing columns detected: {missing_columns}")

logger.info("Schema validation passed")

# --------------------------------------------
# Feature Engineering
# --------------------------------------------

features_df = silver_df \
    .withColumn("transaction_hour", hour(col("transaction_timestamp"))) \
    .withColumn("transaction_day_of_week", dayofweek(col("transaction_timestamp"))) \
    .withColumn(
        "high_value_transaction",
        when(col("transaction_amount") > 500, 1).otherwise(0)
    ) \
    .withColumn(
        "is_weekend_transaction",
        when(dayofweek(col("transaction_timestamp")).isin(1,7), 1).otherwise(0)
    )

logger.info("Derived fraud detection features")

# --------------------------------------------
# Record Monitoring
# --------------------------------------------

record_count = features_df.count()

logger.info(f"Total feature records generated: {record_count}")

# --------------------------------------------
# Write Feature Table
# --------------------------------------------

features_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("transaction_features")

logger.info("Feature engineering table created: transaction_features")

# --------------------------------------------
# Delta Optimization
# --------------------------------------------

spark.sql("OPTIMIZE transaction_features")

logger.info("Feature table optimization completed")

logger.info("Feature engineering pipeline completed successfully")
