from pyspark.sql.functions import col

bronze_df = spark.table("bronze_transactions")

silver_df = bronze_df \
    .dropDuplicates(["transaction_id"]) \
    .filter(col("amount").isNotNull()) \
    .withColumn("amount", col("amount").cast("double"))

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_transactions")

print("Silver table created")
