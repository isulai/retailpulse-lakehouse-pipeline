# Phase 2 - Lakeflow Declarative Pipelines Batch

This phase builds a batch medallion pipeline for retail customers, orders, and products.

## Bronze Layer

Bronze tables ingest source data with minimal transformation:

- `bronze_customers`
- `bronze_orders`
- `bronze_products`

## Silver Layer

Silver tables standardize data for trusted analytics:

- Type casting
- Column renaming
- Data validation
- Quality expectations
- Joining normalized entities into an enriched sales view

## Gold Layer

Gold tables expose business aggregates:

- `gold_city_revenue`
- `gold_product_revenue`
- `gold_top_customers`

## Pipeline Features

- Declarative table definitions
- Materialized views
- Data quality expectations
- Clear pipeline DAG from raw data to analytics

## Reference

Batch pipeline file:

`src/lakeflow/batch/retailpulse_batch_pipeline.py`
