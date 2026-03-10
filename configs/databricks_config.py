# Databricks configuration for the pipeline

DATA_PATH = "/mnt/data/transactions"

BRONZE_TABLE = "bronze_transactions"
SILVER_TABLE = "silver_transactions"
GOLD_TABLE = "gold_transaction_metrics"

CHECKPOINT_PATH = "/mnt/checkpoints/transactions"

PIPELINE_NAME = "fintech_transaction_pipeline"
