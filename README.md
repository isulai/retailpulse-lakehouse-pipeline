# RetailPulse Lakehouse Pipeline

End-to-End Data Engineering Project built using Databricks, Delta Lake, Apache Spark and Medallion Architecture.

## Architecture

![RetailPulse Architecture](images/retailpulse_architecture.png)

---

## Project Overview

This project demonstrates an end-to-end Lakehouse architecture using Databricks and Delta Lake.

### Bronze Layer
- Raw data ingestion
- Delta table creation
- Schema enforcement

### Silver Layer
- Data cleansing
- Data enrichment
- Revenue calculation

### Gold Layer
- Business aggregations
- Region-wise revenue
- Category-wise revenue
- Top customer analysis

### Delta Lake Features
- MERGE INTO
- UPDATE / DELETE
- Time Travel
- DESCRIBE HISTORY
- RESTORE
- OPTIMIZE

### Workflow Orchestration
- Bronze Ingestion
- Silver Transformation
- Gold Aggregation
- Delta Operations
- Project Validation

## Technologies Used

- Databricks
- Delta Lake
- Apache Spark
- PySpark
- GitHub

## Repository Structure

```text
01_Bronze_Ingestion.ipynb
02_Silver_Transformation.ipynb
03_Gold_Aggregation.ipynb
04_Delta_Operations.ipynb
05_Project_Validation.ipynb
```

## Key Outcomes

- Built Medallion Architecture
- Implemented Delta Lake transactions
- Performed MERGE operations
- Demonstrated Time Travel and Restore
- Created Databricks Job Workflow
- Version controlled using GitHub
