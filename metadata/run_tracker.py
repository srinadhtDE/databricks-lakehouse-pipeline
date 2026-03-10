import uuid
from datetime import datetime
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def start_pipeline(pipeline_name):

    run_id = str(uuid.uuid4())
    start_time = datetime.now()

    spark.sql(f"""
    INSERT INTO pipeline_runs
    VALUES (
        '{run_id}',
        '{pipeline_name}',
        TIMESTAMP '{start_time}',
        NULL,
        'RUNNING',
        0
    )
    """)

    return run_id

def end_pipeline(run_id, records_processed):

    end_time = datetime.now()

    spark.sql(f"""
    UPDATE pipeline_runs
    SET
        end_time = TIMESTAMP '{end_time}',
        status = 'SUCCESS',
        records_processed = {records_processed}
    WHERE run_id = '{run_id}'
    """)
