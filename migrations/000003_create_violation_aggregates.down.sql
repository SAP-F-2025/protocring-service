-- Drop continuous aggregates in reverse order

-- Drop user patterns view (continuous aggregate)
DROP MATERIALIZED VIEW IF EXISTS violation_user_patterns CASCADE;

-- Drop attempt summary view (regular view, not continuous aggregate)
DROP VIEW IF EXISTS violation_attempt_summary CASCADE;

-- Drop daily stats view (continuous aggregate)
DROP MATERIALIZED VIEW IF EXISTS violation_stats_daily CASCADE;

-- Drop hourly stats view (continuous aggregate)
DROP MATERIALIZED VIEW IF EXISTS violation_stats_hourly CASCADE;
