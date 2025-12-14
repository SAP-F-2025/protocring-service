-- ============================================================================
-- Fix: Remove NOW() from Continuous Aggregate Definition
-- ============================================================================
-- NOW() in continuous aggregate WHERE clause is evaluated at creation time,
-- not at query time, which causes inconsistent data.
-- 
-- The filter should be applied at query time instead of in the view definition.
-- Data older than 90 days is already handled by the retention policy on violation_logs.

-- Step 1: Drop the old continuous aggregate with the problematic WHERE clause
DROP MATERIALIZED VIEW IF EXISTS violation_user_patterns CASCADE;

-- Step 2: Recreate without WHERE clause (filter at query time instead)
CREATE MATERIALIZED VIEW violation_user_patterns
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', created_at) AS bucket,
    user_id,

    -- Counts
    COUNT(*) AS total_violations,
    COUNT(DISTINCT attempt_id) AS attempts_count,
    COUNT(DISTINCT assessment_id) AS assessments_count,

    -- Severity distribution (0=Low, 1=Medium, 2=High, 3=Critical)
    COUNT(*) FILTER (WHERE severity = 3) AS critical_count,
    COUNT(*) FILTER (WHERE severity = 2) AS high_count,

    -- Prolonged violations
    COUNT(*) FILTER (WHERE is_prolonged = TRUE) AS prolonged_count,

    -- Most common violation types
    MODE() WITHIN GROUP (ORDER BY violation_type) AS most_common_violation,
    COUNT(DISTINCT violation_type) AS unique_violation_types,

    -- Behavioral flags (using int constants)
    BOOL_OR(violation_type = 1) AS has_multiple_faces,
    BOOL_OR(violation_type = 4) AS has_hand_detected,
    BOOL_OR(violation_type = 7) AS has_switching_tab,
    BOOL_OR(violation_type = 8) AS has_fullscreen_exit,

    -- Metrics
    AVG(confidence_score) AS avg_confidence
FROM violation_logs
GROUP BY bucket, user_id
WITH NO DATA;

-- Re-add refresh policy (same as before)
SELECT add_continuous_aggregate_policy(
    'violation_user_patterns',
    start_offset => INTERVAL '7 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Re-create index for efficient queries
CREATE INDEX idx_user_patterns_user ON violation_user_patterns (user_id, bucket DESC);

-- Refresh with recent data only (last 90 days to match retention policy)
-- This populates the continuous aggregate with existing data
CALL refresh_continuous_aggregate(
    'violation_user_patterns',
    NOW() - INTERVAL '90 days',
    NOW()
);

COMMENT ON MATERIALIZED VIEW violation_user_patterns IS
    'Daily user violation patterns for behavior analysis. Filter at query time for date ranges. Data retention handled by violation_logs retention policy (90 days).';
