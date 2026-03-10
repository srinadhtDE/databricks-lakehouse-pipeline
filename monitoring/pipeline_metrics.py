from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.table("gold_transaction_metrics")

metrics = {
    "row_count": df.count(),
    "total_revenue": df.groupBy().sum("total_revenue").collect()[0][0]
}

print("Pipeline Metrics")
print(metrics)
