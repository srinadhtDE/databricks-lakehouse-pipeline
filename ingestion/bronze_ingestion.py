from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Bronze Ingestion").getOrCreate()

df = spark.read.option("header", True).csv("/FileStore/data/transactions.csv")

df.write.format("delta") \
.mode("overwrite") \
.saveAsTable("bronze_transactions")

print("Bronze table created")
