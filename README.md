# Databricks Lakehouse Pipeline

## Project Overview

This project demonstrates a production-style data engineering pipeline built using a Lakehouse architecture.

The pipeline processes fintech transaction data and produces curated analytics datasets that can power reporting dashboards and machine learning workflows such as fraud detection.

---

## Architecture Diagram

```mermaid
flowchart TD

A[Transaction Data Source CSV API Streaming]

A --> B[Bronze Layer Raw Data Ingestion PySpark Streaming]

B --> C[Delta Lake Storage ACID Transactions Schema Enforcement]

C --> D[Silver Layer Data Cleaning Deduplication Validation]

D --> E[Gold Layer Business Metrics Revenue Analytics Fraud Monitoring]

E --> F[dbt Transformation Layer Analytics Models]

F --> G[Data Consumers Dashboards BI ML Models]

---

## Tech Stack

- PySpark
- Delta Lake
- dbt
- GitHub Actions (CI/CD)
- Python

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

## Future Improvements

- Real-time streaming ingestion using Spark Structured Streaming
- Fraud detection model using ML features
- Monitoring dashboards for pipeline health
