from pyspark.sql.functions import col, to_date
from utils.config_loader import load_config, get_logger

logger = get_logger("bronze_to_silver")

config = load_config("../configs/pipeline_config.yaml")

logger.info("Starting Bronze to Silver transformation")

bronze_df = spark.table(config["bronze_table"])

clean_df = bronze_df \
    .dropDuplicates(["transaction_id"]) \
    .filter(col("transaction_status") == "completed") \
    .filter(col("transaction_amount") > 0) \
    .withColumn("transaction_date", to_date(col("transaction_timestamp")))

clean_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("transaction_date") \
    .saveAsTable(config["silver_table"])

logger.info("Silver layer table successfully created")
