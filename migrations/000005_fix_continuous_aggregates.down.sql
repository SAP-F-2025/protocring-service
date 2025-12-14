-- Rollback: Restore original violation_user_patterns with WHERE clause
-- Note: This will lose any data aggregated after the fix was applied

DROP MATERIALIZED VIEW IF EXISTS violation_user_patterns CASCADE;

-- Recreate original (with WHERE clause - not recommended but needed for rollback)
CREATE MATERIALIZED VIEW violation_user_patterns
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', created_at) AS bucket,
    user_id,
    COUNT(*) AS total_violations,
    COUNT(DISTINCT attempt_id) AS attempts_count,
    COUNT(DISTINCT assessment_id) AS assessments_count,
    COUNT(*) FILTER (WHERE severity = 3) AS critical_count,
    COUNT(*) FILTER (WHERE severity = 2) AS high_count,
    COUNT(*) FILTER (WHERE is_prolonged = TRUE) AS prolonged_count,
    MODE() WITHIN GROUP (ORDER BY violation_type) AS most_common_violation,
    COUNT(DISTINCT violation_type) AS unique_violation_types,
    BOOL_OR(violation_type = 1) AS has_multiple_faces,
    BOOL_OR(violation_type = 4) AS has_hand_detected,
    BOOL_OR(violation_type = 7) AS has_switching_tab,
    BOOL_OR(violation_type = 8) AS has_fullscreen_exit,
    AVG(confidence_score) AS avg_confidence
FROM violation_logs
WHERE created_at > NOW() - INTERVAL '90 days'
GROUP BY bucket, user_id
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'violation_user_patterns',
    start_offset => INTERVAL '7 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

CREATE INDEX idx_user_patterns_user ON violation_user_patterns (user_id, bucket DESC);

COMMENT ON MATERIALIZED VIEW violation_user_patterns IS
    'Daily user violation patterns for behavior analysis (original with WHERE clause)';
