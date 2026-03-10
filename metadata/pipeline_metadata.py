from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

def create_metadata_tables():

    spark.sql("""
    CREATE TABLE IF NOT EXISTS pipeline_runs (
        run_id STRING,
        pipeline_name STRING,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        status STRING,
        records_processed BIGINT
    )
    USING DELTA
    """)

    spark.sql("""
    CREATE TABLE IF NOT EXISTS data_lineage (
        pipeline_name STRING,
        source_table STRING,
        target_table STRING,
        transformation_step STRING,
        created_at TIMESTAMP
    )
    USING DELTA
    """)

    print("Metadata tables created")
