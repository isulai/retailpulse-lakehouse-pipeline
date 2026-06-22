# Phase 3 - Streaming Pipeline

This phase uses Auto Loader and Lakeflow streaming tables to process new retail CSV files as they arrive in Unity Catalog Volumes.

## Source Paths

- `/Volumes/workspace/retailpulse_project/retailpulse_stream/customers`
- `/Volumes/workspace/retailpulse_project/retailpulse_stream/orders`
- `/Volumes/workspace/retailpulse_project/retailpulse_stream/products`

## Pipeline Flow

1. Auto Loader detects new CSV files.
2. Bronze streaming tables capture raw records and file metadata.
3. Silver streaming tables cast fields and standardize column names.
4. The enriched sales materialized view joins customers, orders, and products.
5. Gold materialized views aggregate revenue by city, product, and customer.

## Metadata Columns

- `source_file`
- `file_modified_ts`
- `ingestion_ts`

## Reference

Streaming pipeline file:

`src/lakeflow/streaming/retailpulse_streaming_pipeline.py`
