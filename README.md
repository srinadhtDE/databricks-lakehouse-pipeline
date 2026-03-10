# Databricks Lakehouse Pipeline

## Project Overview

This project demonstrates a production-style data engineering pipeline built using a Lakehouse architecture.

The pipeline processes fintech transaction data and produces curated analytics datasets that can power reporting dashboards and machine learning workflows such as fraud detection.

---

## Architecture

## Architecture Diagram

```mermaid
flowchart TD

A[Transaction Data Source<br>CSV / API / Streaming Events]

A --> B[Bronze Layer<br>Raw Data Ingestion<br>PySpark / Structured Streaming]

B --> C[Delta Lake Storage<br>ACID Transactions<br>Schema Enforcement]

C --> D[Silver Layer<br>Data Cleaning<br>Deduplication<br>Validation]

D --> E[Gold Layer<br>Business Metrics<br>Revenue Analytics<br>Fraud Monitoring]

E --> F[dbt Transformation Layer<br>Analytics Data Models]

F --> G[Data Consumers<br>Dashboards / BI Tools / ML Models]

subgraph Platform
B
C
D
E
end

subgraph Engineering
F
end

subgraph Consumption
G
end

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
