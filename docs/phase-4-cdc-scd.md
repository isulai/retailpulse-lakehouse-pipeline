# Phase 4 - CDC Pipeline and SCD

This phase processes customer change events and applies them to SCD Type 1 and SCD Type 2 tables.

## Source

`customers_cdc.csv`

Expected columns:

- `customer_id`
- `name`
- `city`
- `operation`
- `updated_at`

## CDC Flow

1. Auto Loader ingests raw CDC files into a Bronze streaming table.
2. Silver standardizes CDC records and casts data types.
3. `create_auto_cdc_flow()` applies change events into target tables.
4. SCD Type 1 keeps only the latest customer state.
5. SCD Type 2 keeps historical customer versions.

## SCD Type 1

SCD Type 1 overwrites existing customer attributes when newer changes arrive. It is useful when only the current customer state is required.

## SCD Type 2

SCD Type 2 preserves history by maintaining version ranges. Databricks manages validity columns such as `__START_AT` and `__END_AT` for historical tracking.

## Delete Handling

Delete events are applied using:

```python
apply_as_deletes=expr("operation = 'DELETE'")
```

## Sequence Logic

CDC ordering is controlled by:

```python
sequence_by=col("updated_at")
```

## Reference

CDC pipeline file:

`src/lakeflow/cdc/retail_pulse_cdc_pipeline.py`
