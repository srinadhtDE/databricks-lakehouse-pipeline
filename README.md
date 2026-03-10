# Databricks Lakehouse Pipeline

## Project Overview

This project demonstrates a production-style data engineering pipeline built using a Lakehouse architecture.

The pipeline processes fintech transaction data and produces curated analytics datasets that can power reporting dashboards and machine learning workflows such as fraud detection.

---
## Metadata & Lineage Tracking

The pipeline tracks execution metadata and table lineage using Delta tables.

Metadata tables:

pipeline_runs
Tracks pipeline executions, status, and records processed.

data_lineage
Tracks source → target transformations across pipeline stages.

## Tech Stack

- PySpark
- Delta Lake
- dbt
- GitHub Actions (CI/CD)
- Python

---
## Architecture


                ┌─────────────────────┐
                │   Airflow Scheduler │
                │(Pipeline Orchestration)│
                └──────────┬──────────┘
                           │
                           ▼
                ┌─────────────────────┐
                │ Transaction Source  │
                │ CSV / API / Stream  │
                └──────────┬──────────┘
                           │
                           ▼
                ┌─────────────────────┐
                │ Bronze Layer        │
                │ Raw Data Ingestion  │
                │ PySpark / Streaming │
                └──────────┬──────────┘
                           │
                           ▼
                ┌─────────────────────┐
                │ Delta Lake Storage  │
                │ ACID + Schema       │
                └──────────┬──────────┘
                           │
                           ▼
                ┌─────────────────────┐
                │ Silver Layer        │
                │ Data Cleaning       │
                │ Validation          │
                └──────────┬──────────┘
                           │
                           ▼
                ┌─────────────────────┐
                │ Gold Layer          │
                │ Business Metrics    │
                │ Aggregations        │
                └──────────┬──────────┘
                           │
                           ▼
                ┌─────────────────────┐
                │ dbt Transformation  │
                │ Analytics Models    │
                └──────────┬──────────┘
                           │
                           ▼
                ┌─────────────────────┐
                │ BI / ML Consumers   │
                │ Dashboards / Models │
                └─────────────────────┘


---

## Project Structure
databricks-lakehouse-pipeline
├ ingestion
├ transformations
├ orchestration
├ quality_checks
├ configs
├ dbt_project
├ sample_data
├ utils
├ architecture
├ .github/workflows
├ requirements.txt
└ README.md

---

## Data Model

Transaction dataset contains:

- transaction_id
- user_id
- merchant_id
- transaction_amount
- transaction_type
- payment_method
- transaction_status
- transaction_timestamp
- country
- fraud_flag

---

## Running the Pipeline

1 Generate sample transaction data

python sample_data/generate_transactions.py

2 Run Bronze ingestion

python ingestion/autoloader_ingestion.py

3 Transform Bronze → Silver

python transformations/bronze_to_silver.py

4 Generate Gold analytics metrics

python transformations/silver_to_gold.py

5 Run data quality validation

python quality_checks/checks.py

---

## CI/CD Pipeline

GitHub Actions automatically runs pipeline validation when code is pushed to the main branch.

---

## Sample Pipeline Output

Example aggregated transaction metrics produced by the Gold layer.

| transaction_date | total_transactions | total_revenue | active_users | avg_transaction_value | fraud_transactions |
|------------------|-------------------|---------------|-------------|-----------------------|-------------------|
| 2024-01-01 | 1245 | 185432.50 | 432 | 149.02 | 12 |
| 2024-01-02 | 1310 | 197882.75 | 451 | 151.04 | 9 |
| 2024-01-03 | 1188 | 176543.90 | 418 | 148.59 | 11 |
| 2024-01-04 | 1399 | 204321.60 | 472 | 146.04 | 10 |
| 2024-01-05 | 1422 | 215987.10 | 489 | 151.87 | 14 |

These metrics are generated from the **Gold analytics layer** after processing transaction data through the Bronze and Silver layers.

## Future Improvements

- Fraud detection model using ML features
- Monitoring dashboards for pipeline health
