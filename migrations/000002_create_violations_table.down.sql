-- Remove retention policy
SELECT remove_retention_policy('violation_logs', if_exists => TRUE);

-- Remove compression policy
SELECT remove_compression_policy('violation_logs', if_exists => TRUE);

-- Drop table (this also removes the hypertable)
DROP TABLE IF EXISTS violation_logs CASCADE;
