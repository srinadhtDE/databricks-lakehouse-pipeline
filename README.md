# Databricks Lakehouse Pipeline

## Project Overview

This project demonstrates a production-style data engineering pipeline built using a Lakehouse architecture.

The pipeline processes fintech transaction data and produces curated analytics datasets that can power reporting dashboards and machine learning workflows such as fraud detection.

---

## Architecture

Transaction Data Source  
↓  
Bronze Layer – Raw ingestion using PySpark  
↓  
Silver Layer – Data cleaning, deduplication and validation  
↓  
Gold Layer – Aggregated business metrics  
↓  
Analytics / Machine Learning

---

## Tech Stack

- PySpark
- Delta Lake
- dbt
- GitHub Actions (CI/CD)
- Python

---

## Project Structure
