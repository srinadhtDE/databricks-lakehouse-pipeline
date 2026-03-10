- Architecture :

Data Source -> Bronze Layer (Raw Ingestion) -> Silver Layer (Data Cleaning & Validation) -> Gold Layer (Analytics Metrics)

- Tech Stack:

Databricks
Apache Spark (PySpark)
Delta Lake
Medallion Architecture

- Pipeline Steps:

Raw transactions ingested into Bronze Delta table
Silver layer cleans and deduplicates data
Gold layer creates analytics metrics like revenue and active users
