from pyspark.sql.functions import col

bronze_df = spark.table("bronze_transactions")

clean_df = bronze_df \
    .dropDuplicates(["transaction_id"]) \
    .filter(col("transaction_status") == "completed") \
    .filter(col("transaction_amount") > 0)

clean_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("transaction_date") \
    .saveAsTable("silver_transactions")
