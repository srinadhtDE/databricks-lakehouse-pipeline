from pyspark.sql.functions import col, to_date, countDistinct, sum as spark_sum, avg
from utils.config_loader import load_config, get_logger

# --------------------------------------------
# Initialize Logger and Config
# --------------------------------------------

logger = get_logger("silver_to_gold")

config = load_config("../configs/pipeline_config.yaml")

logger.info("Starting Silver → Gold aggregation pipeline")

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

silver_df = silver_df.withColumn(
    "transaction_date",
    to_date(col("transaction_timestamp"))
)

logger.info("Derived transaction_date column")

# --------------------------------------------
# Gold Aggregations
# --------------------------------------------

gold_df = silver_df.groupBy("transaction_date").agg(

    spark_sum("transaction_amount").alias("total_revenue"),

    countDistinct("transaction_id").alias("total_transactions"),

    countDistinct("user_id").alias("active_users"),

    avg("transaction_amount").alias("avg_transaction_value"),

    spark_sum("fraud_flag").alias("fraud_transactions")

)

logger.info("Aggregation completed")

# --------------------------------------------
# Record Monitoring
# --------------------------------------------

record_count = gold_df.count()

logger.info(f"Total records generated in Gold layer: {record_count}")

# --------------------------------------------
# Write Gold Delta Table
# --------------------------------------------

logger.info("Writing Gold Delta table")

gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("transaction_date") \
    .saveAsTable(config["gold_table"])

logger.info(f"Gold table created: {config['gold_table']}")

# --------------------------------------------
# Delta Lake Optimization
# --------------------------------------------

logger.info("Running Delta optimization")

spark.sql(f"OPTIMIZE {config['gold_table']}")

spark.sql(f"VACUUM {config['gold_table']} RETAIN 168 HOURS")

logger.info("Delta optimization completed")

logger.info("Silver → Gold pipeline completed successfully")
