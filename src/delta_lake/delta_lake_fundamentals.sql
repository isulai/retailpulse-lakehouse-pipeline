-- RetailPulse Delta Lake Fundamentals
-- Update catalog and schema names before running.

USE CATALOG workspace;
USE SCHEMA retailpulse_project;

-- 1. Create a Delta table
CREATE TABLE IF NOT EXISTS delta_customers_demo (
  customer_id INT,
  name STRING,
  city STRING,
  updated_at DATE
)
USING DELTA;

-- 2. Insert sample records
INSERT INTO delta_customers_demo VALUES
  (1, 'Aarav', 'Bengaluru', DATE '2026-06-01'),
  (2, 'Meera', 'Chennai', DATE '2026-06-02');

-- 3. Update records
UPDATE delta_customers_demo
SET city = 'Hyderabad',
    updated_at = current_date()
WHERE customer_id = 2;

-- 4. Delete records
DELETE FROM delta_customers_demo
WHERE customer_id = 999;

-- 5. Merge/upsert records
MERGE INTO delta_customers_demo AS target
USING (
  SELECT 1 AS customer_id, 'Aarav Sharma' AS name, 'Bengaluru' AS city, current_date() AS updated_at
  UNION ALL
  SELECT 3 AS customer_id, 'Nisha' AS name, 'Mumbai' AS city, current_date() AS updated_at
) AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN UPDATE SET
  target.name = source.name,
  target.city = source.city,
  target.updated_at = source.updated_at
WHEN NOT MATCHED THEN INSERT *;

-- 6. Inspect table history
DESCRIBE HISTORY delta_customers_demo;

-- 7. Time travel by version
SELECT *
FROM delta_customers_demo VERSION AS OF 0;

-- 8. Time travel by timestamp
-- SELECT *
-- FROM delta_customers_demo TIMESTAMP AS OF '2026-06-01T00:00:00.000Z';

-- 9. Restore to an earlier version
-- RESTORE TABLE delta_customers_demo TO VERSION AS OF 0;

-- 10. Optimize files
OPTIMIZE delta_customers_demo;

-- 11. Vacuum old files
-- Keep retention settings aligned with your workspace governance policy.
-- VACUUM delta_customers_demo RETAIN 168 HOURS;
