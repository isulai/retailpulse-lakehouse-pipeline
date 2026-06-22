# Setup Guide

## Prerequisites

- Databricks workspace
- Unity Catalog enabled
- Permission to create schemas, tables, volumes, and Lakeflow pipelines
- Source CSV files for customers, orders, products, and customer CDC events

## Suggested Catalog and Schema

The project scripts currently reference:

```text
workspace.retailpulse_project
```

You can update the catalog and schema names in the Python files if your Databricks workspace uses a different naming convention.

## Suggested Volume Layout

```text
/Volumes/workspace/retailpulse_project/retailpulse_stream/customers
/Volumes/workspace/retailpulse_project/retailpulse_stream/orders
/Volumes/workspace/retailpulse_project/retailpulse_stream/products
/Volumes/workspace/retailpulse_project/retailpulse_cdc
```

## Run Steps

1. Upload the source CSV files to the Unity Catalog Volume paths.
2. Open Databricks Lakeflow Declarative Pipelines.
3. Create a pipeline for the batch implementation.
4. Attach `src/lakeflow/batch/retailpulse_batch_pipeline.py`.
5. Create a pipeline for the streaming implementation.
6. Attach `src/lakeflow/streaming/retailpulse_streaming_pipeline.py`.
7. Create a pipeline for the CDC implementation.
8. Attach `src/lakeflow/cdc/retail_pulse_cdc_pipeline.py`.
9. Run each pipeline and inspect the DAG.
10. Query the Gold tables from Databricks SQL.

## GitHub Push Commands

Run these commands from the repository root after Git is available locally:

```bash
git init
git add .
git commit -m "Initial RetailPulse lakehouse pipeline project"
git branch -M main
git remote add origin https://github.com/<your-user>/retailpulse-lakehouse-pipeline.git
git push -u origin main
```
