from pyspark.sql.functions import col, to_date, countDistinct, sum as spark_sum, avg
from utils.config_loader import load_config, get_logger

logger = get_logger("gold_aggregation")

config = load_config("../configs/pipeline_config.yaml")

logger.info("Starting Gold layer aggregation")

silver_df = spark.table(config["silver_table"])

gold_df = silver_df \
    .withColumn("transaction_date", to_date(col("transaction_timestamp"))) \
    .groupBy("transaction_date") \
    .agg(
        spark_sum("transaction_amount").alias("total_revenue"),
        countDistinct("user_id").alias("active_users"),
        countDistinct("transaction_id").alias("total_transactions"),
        avg("transaction_amount").alias("avg_transaction_value"),
        spark_sum("fraud_flag").alias("fraud_transactions")
    )

gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("transaction_date") \
    .saveAsTable(config["gold_table"])

logger.info("Gold metrics table successfully created")
