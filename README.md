# databricks-lakehouse-pipeline
End-to-end Databricks Lakehouse pipeline implementing Medallion architecture (Bronze, Silver, Gold) using PySpark and Delta Lake for scalable analytics.

Data Source
     ↓
Bronze Layer (Raw Ingestion)
     ↓
Silver Layer (Data Cleaning & Validation)
     ↓
Gold Layer (Analytics Metrics)
