from datetime import datetime
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def record_lineage(pipeline, source, target, step):

    timestamp = datetime.now()

    spark.sql(f"""
    INSERT INTO data_lineage
    VALUES (
        '{pipeline}',
        '{source}',
        '{target}',
        '{step}',
        TIMESTAMP '{timestamp}'
    )
    """)
