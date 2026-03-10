from utils.config_loader import load_config, get_logger

logger = get_logger("gold_aggregation")

config = load_config("../configs/pipeline_config.yaml")

logger.info("Starting Gold layer aggregation")

silver_df = spark.table(config["silver_table"])

gold_df = silver_df.groupByExpr(
    "date(transaction_timestamp) as transaction_date"
).aggExpr(
    "count(*) as total_transactions",
    "sum(transaction_amount) as total_revenue",
    "count(distinct user_id) as active_users",
    "avg(transaction_amount) as avg_transaction_value",
    "sum(fraud_flag) as fraud_transactions"
)

gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(config["gold_table"])

logger.info("Gold metrics table successfully created")
