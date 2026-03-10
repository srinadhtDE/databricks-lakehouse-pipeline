from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("transaction_streaming_pipeline") \
    .getOrCreate()

stream_df = spark.readStream \
    .format("csv") \
    .option("header", True) \
    .schema("""
        transaction_id INT,
        user_id INT,
        merchant_id INT,
        transaction_amount DOUBLE,
        transaction_type STRING,
        payment_method STRING,
        transaction_status STRING,
        transaction_timestamp TIMESTAMP,
        country STRING,
        fraud_flag INT
    """) \
    .load("/FileStore/data/transactions")

query = stream_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/FileStore/checkpoints/transactions") \
    .table("bronze_transactions")

query.awaitTermination()
