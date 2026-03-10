from pyspark.sql.types import *
from configs.databricks_config import *

transaction_schema = StructType([
    StructField("transaction_id", IntegerType()),
    StructField("user_id", IntegerType()),
    StructField("merchant_id", IntegerType()),
    StructField("transaction_amount", DoubleType()),
    StructField("transaction_type", StringType()),
    StructField("payment_method", StringType()),
    StructField("transaction_status", StringType()),
    StructField("transaction_timestamp", TimestampType()),
    StructField("country", StringType()),
    StructField("fraud_flag", IntegerType())
])

df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("header", True) \
    .schema(transaction_schema) \
    .load(DATA_PATH)

df.writeStream \
    .format("delta") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .table(BRONZE_TABLE)
