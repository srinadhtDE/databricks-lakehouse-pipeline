from pyspark.sql.functions import col
from utils.config_loader import load_config, get_logger

logger = get_logger("silver_transformation")

config = load_config("../configs/pipeline_config.yaml")

logger.info("Starting Silver transformation")

bronze_df = spark.table(config["bronze_table"])

clean_df = bronze_df \
    .dropDuplicates(["transaction_id"]) \
    .filter(col("transaction_amount") > 0) \
    .filter(col("transaction_status") == "completed")

logger.info(f"Cleaned dataset count: {clean_df.count()}")

clean_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy(config["partition_column"]) \
    .saveAsTable(config["silver_table"])

logger.info("Silver table created successfully")
