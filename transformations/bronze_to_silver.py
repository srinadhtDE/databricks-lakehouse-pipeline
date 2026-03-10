from metadata.run_tracker import start_pipeline, end_pipeline
from metadata.lineage_tracker import record_lineage

run_id = start_pipeline("bronze_to_silver")

bronze_df = spark.table("bronze_transactions")

silver_df = bronze_df.dropDuplicates(["transaction_id"])

silver_df.write.format("delta").mode("overwrite").saveAsTable("silver_transactions")

records = silver_df.count()

record_lineage(
    "bronze_to_silver",
    "bronze_transactions",
    "silver_transactions",
    "deduplication"
)

end_pipeline(run_id, records)
