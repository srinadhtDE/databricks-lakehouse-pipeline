from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data_engineering",
    "retries": 1,
}

with DAG(
    dag_id="fintech_transaction_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    description="Lakehouse transaction pipeline",
) as dag:

   generate_data = BashOperator(
    task_id="generate_transactions",
    bash_command="python /opt/airflow/dags/sample_data/generate_transactions.py",
)

    ingest_bronze = BashOperator(
        task_id="bronze_ingestion",
        bash_command="python /opt/airflow/dags/ingestion/autoloader_ingestion.py"
    )

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command="python /opt/airflow/dags/transformations/bronze_to_silver.py"
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command="python /opt/airflow/dags/transformations/silver_to_gold.py"
    )

    data_quality = BashOperator(
        task_id="data_quality_checks",
        bash_command="python /opt/airflow/dags/quality_checks/checks.py"
    )

    pipeline_metrics = BashOperator(
        task_id="pipeline_metrics",
        bash_command="python /opt/airflow/dags/monitoring/pipeline_metrics.py"
    )

    generate_data >> ingest_bronze >> bronze_to_silver >> silver_to_gold >> data_quality >> pipeline_metrics
