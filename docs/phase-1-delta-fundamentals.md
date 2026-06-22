# Phase 1 - Delta Lake Fundamentals

This phase demonstrates the core Delta Lake operations that make lakehouse tables reliable and maintainable.

## Concepts Covered

- Creating Delta tables
- `MERGE` for upserts
- `UPDATE` for corrections
- `DELETE` for removals
- `DESCRIBE HISTORY` for auditability
- Time travel queries
- `RESTORE` for rollback
- `OPTIMIZE` for file compaction
- `VACUUM` for cleanup

## Why It Matters

Delta Lake adds ACID transactions, scalable metadata handling, and versioned data management on top of cloud object storage. These capabilities are essential for production pipelines where data must be recoverable, traceable, and query efficient.

## Reference

SQL examples are available in:

`src/delta_lake/delta_lake_fundamentals.sql`
