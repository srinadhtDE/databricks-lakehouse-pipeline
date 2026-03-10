# Databricks notebook source

"""
Databricks Lakehouse Pipeline
Implements a Medallion Architecture for transaction analytics.

Layers
------
Bronze : Raw ingestion
Silver : Cleaned and validated transactions
Gold   : Aggregated analytics metrics
"""

from pyspark.sql.functions import col, to_date, countDistinct, sum as spark_sum, avg
from pyspark.sql.types import *
import logging

# --------------------------------------------
# Logger
# --------------------------------------------

logger = logging.getLogger("lakehouse_pipeline")
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# --------------------------------------------
# Configuration
# --------------------------------------------

DATA_PATH = "/mnt/data/transactions"

BRONZE_TABLE = "bronze_transactions"
SILVER_TABLE = "silver_transactions"
GOLD_TABLE = "gold_transaction_metrics"

# --------------------------------------------
# Transaction Schema
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

# --------------------------------------------
# Bronze Layer
# --------------------------------------------

def bronze_ingestion():

    logger.info("Starting Bronze ingestion")

    df = spark.read.format("csv") \
        .option("header", True) \
        .schema(transaction_schema) \
        .load(DATA_PATH)

    df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(BRONZE_TABLE)

    count = df.count()

    logger.info(f"Bronze table created with {count} records")


# --------------------------------------------
# Silver Layer
# --------------------------------------------

def bronze_to_silver():

    logger.info("Starting Silver transformation")

    bronze_df = spark.table(BRONZE_TABLE)

    clean_df = bronze_df \
        .dropDuplicates(["transaction_id"]) \
        .filter(col("transaction_status") == "completed") \
        .filter(col("transaction_amount") > 0)

    clean_df = clean_df.withColumn(
        "transaction_date",
        to_date(col("transaction_timestamp"))
    )

    clean_df.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("transaction_date") \
        .saveAsTable(SILVER_TABLE)

    count = clean_df.count()

    logger.info(f"Silver table created with {count} records")


# --------------------------------------------
# Gold Layer
# --------------------------------------------

def silver_to_gold():

    logger.info("Starting Gold aggregation")

    silver_df = spark.table(SILVER_TABLE)

    metrics_df = silver_df.groupBy("transaction_date").agg(

        spark_sum("transaction_amount").alias("total_revenue"),

        countDistinct("user_id").alias("active_users"),

        countDistinct("transaction_id").alias("total_transactions"),

        avg("transaction_amount").alias("avg_transaction_value"),

        spark_sum("fraud_flag").alias("fraud_transactions")

    )

    metrics_df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(GOLD_TABLE)

    logger.info("Gold metrics table created")


# --------------------------------------------
# Delta Lake Optimization
# --------------------------------------------

def optimize_tables():

    logger.info("Running Delta Lake optimization")

    spark.sql(f"OPTIMIZE {SILVER_TABLE}")
    spark.sql(f"VACUUM {SILVER_TABLE} RETAIN 168 HOURS")

    logger.info("Optimization completed")


# --------------------------------------------
# Main Pipeline Execution
# --------------------------------------------

def run_pipeline():

    logger.info("Starting Lakehouse pipeline")

    bronze_ingestion()

    bronze_to_silver()

    silver_to_gold()

    optimize_tables()

    logger.info("Pipeline completed successfully")


# --------------------------------------------
# Execute Pipeline
# --------------------------------------------

if __name__ == "__main__":

    run_pipeline()
