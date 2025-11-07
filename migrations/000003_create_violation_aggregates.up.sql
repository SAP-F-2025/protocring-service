-- ============================================================================
-- Continuous Aggregates for Violation Analytics
-- ============================================================================
-- Continuous aggregates are materialized views that are automatically
-- refreshed by TimescaleDB. They provide fast access to pre-computed
-- aggregations without scanning the entire hypertable.

-- ----------------------------------------------------------------------------
-- 1. Hourly Violation Statistics (Global)
-- ----------------------------------------------------------------------------
-- Aggregates violations by hour with severity breakdown
-- Use case: Real-time monitoring dashboards, hourly trend analysis
CREATE MATERIALIZED VIEW violation_stats_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', created_at) AS bucket,
    COUNT(*) AS total_violations,
    COUNT(DISTINCT attempt_id) AS unique_attempts,
    COUNT(DISTINCT user_id) AS unique_users,

    -- Severity breakdown (0=Low, 1=Medium, 2=High, 3=Critical)
    COUNT(*) FILTER (WHERE severity = 3) AS critical_count,
    COUNT(*) FILTER (WHERE severity = 2) AS high_count,
    COUNT(*) FILTER (WHERE severity = 1) AS medium_count,
    COUNT(*) FILTER (WHERE severity = 0) AS low_count,

    -- Violation type breakdown (top types)
    COUNT(*) FILTER (WHERE violation_type = 0) AS face_not_detected_count,
    COUNT(*) FILTER (WHERE violation_type = 1) AS multiple_faces_count,
    COUNT(*) FILTER (WHERE violation_type = 2) AS looking_away_count,
    COUNT(*) FILTER (WHERE violation_type = 4) AS hand_detected_count,
    COUNT(*) FILTER (WHERE violation_type = 7) AS switching_tab_count,
    COUNT(*) FILTER (WHERE violation_type = 8) AS fullscreen_count,

    -- Prolonged violations
    COUNT(*) FILTER (WHERE is_prolonged = TRUE) AS prolonged_count,

    -- Average confidence
    AVG(confidence_score) AS avg_confidence
FROM violation_logs
GROUP BY bucket
WITH NO DATA;

-- Refresh policy: refresh last 24 hours every 30 minutes
SELECT add_continuous_aggregate_policy(
    'violation_stats_hourly',
    start_offset => INTERVAL '24 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '30 minutes',
    if_not_exists => TRUE
);

COMMENT ON MATERIALIZED VIEW violation_stats_hourly IS
    'Hourly violation statistics with severity and type breakdown';

-- ----------------------------------------------------------------------------
-- 2. Daily Violation Statistics (Global)
-- ----------------------------------------------------------------------------
-- Aggregates violations by day for long-term trend analysis
-- Use case: Weekly/monthly reports, historical analysis
CREATE MATERIALIZED VIEW violation_stats_daily
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', created_at) AS bucket,
    COUNT(*) AS total_violations,
    COUNT(DISTINCT attempt_id) AS unique_attempts,
    COUNT(DISTINCT user_id) AS unique_users,
    COUNT(DISTINCT assessment_id) AS unique_assessments,

    -- Severity breakdown (0=Low, 1=Medium, 2=High, 3=Critical)
    COUNT(*) FILTER (WHERE severity = 3) AS critical_count,
    COUNT(*) FILTER (WHERE severity = 2) AS high_count,
    COUNT(*) FILTER (WHERE severity = 1) AS medium_count,
    COUNT(*) FILTER (WHERE severity = 0) AS low_count,

    -- Violation type stats (using int constants)
    COUNT(*) FILTER (WHERE violation_type = 0) AS face_not_detected_count,
    COUNT(*) FILTER (WHERE violation_type = 1) AS multiple_faces_count,
    COUNT(*) FILTER (WHERE violation_type = 2) AS looking_away_count,
    COUNT(*) FILTER (WHERE violation_type = 3) AS mouth_open_count,
    COUNT(*) FILTER (WHERE violation_type = 4) AS hand_detected_count,
    COUNT(*) FILTER (WHERE violation_type = 6) AS copy_paste_count,
    COUNT(*) FILTER (WHERE violation_type = 7) AS switching_tab_count,
    COUNT(*) FILTER (WHERE violation_type = 8) AS fullscreen_count,
    COUNT(*) FILTER (WHERE violation_type = 9) AS phone_detect_count,

    -- Prolonged violations
    COUNT(*) FILTER (WHERE is_prolonged = TRUE) AS prolonged_count,
    AVG(EXTRACT(EPOCH FROM (ended_at - created_at))) FILTER (WHERE ended_at IS NOT NULL) AS avg_duration_seconds,

    -- Detection metrics
    AVG(confidence_score) AS avg_confidence
FROM violation_logs
GROUP BY bucket
WITH NO DATA;

-- Refresh policy: refresh last 7 days daily at midnight
SELECT add_continuous_aggregate_policy(
    'violation_stats_daily',
    start_offset => INTERVAL '7 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

COMMENT ON MATERIALIZED VIEW violation_stats_daily IS
    'Daily violation statistics for trend analysis and reporting';

-- ----------------------------------------------------------------------------
-- 3. Per-Attempt Violation Summary
-- ----------------------------------------------------------------------------
-- Aggregates all violations for each attempt
-- Use case: Attempt result calculation, integrity scoring
CREATE MATERIALIZED VIEW violation_attempt_summary
WITH (timescaledb.continuous) AS
SELECT
    attempt_id,
    user_id,
    assessment_id,

    -- Time range
    MIN(created_at) AS first_violation_at,
    MAX(created_at) AS last_violation_at,
    MAX(created_at) - MIN(created_at) AS duration,

    -- Counts
    COUNT(*) AS total_violations,
    COUNT(DISTINCT violation_type) AS unique_violation_types,
    COUNT(*) FILTER (WHERE is_prolonged = TRUE) AS prolonged_violations_count,

    -- Severity breakdown (0=Low, 1=Medium, 2=High, 3=Critical)
    COUNT(*) FILTER (WHERE severity = 3) AS critical_count,
    COUNT(*) FILTER (WHERE severity = 2) AS high_count,
    COUNT(*) FILTER (WHERE severity = 1) AS medium_count,
    COUNT(*) FILTER (WHERE severity = 0) AS low_count,

    -- Most severe violation (max severity level)
    MAX(severity) AS max_severity_level,

    -- Violation types (array aggregation)
    array_agg(DISTINCT violation_type) AS violation_types,

    -- Confidence metrics
    AVG(confidence_score) AS avg_confidence,
    MAX(confidence_score) AS max_confidence,
    MIN(confidence_score) AS min_confidence
FROM violation_logs
WHERE created_at > NOW() - INTERVAL '30 days'  -- Only recent attempts
GROUP BY attempt_id, user_id, assessment_id
WITH NO DATA;

-- Refresh policy: refresh last 24 hours every hour
SELECT add_continuous_aggregate_policy(
    'violation_attempt_summary',
    start_offset => INTERVAL '24 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

COMMENT ON MATERIALIZED VIEW violation_attempt_summary IS
    'Per-attempt violation summary for integrity scoring';

-- ----------------------------------------------------------------------------
-- 4. User Violation Patterns
-- ----------------------------------------------------------------------------
-- Aggregates violations by user to identify patterns and repeat offenders
-- Use case: User behavior analysis, risk scoring
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
WHERE created_at > NOW() - INTERVAL '90 days'
GROUP BY bucket, user_id
WITH NO DATA;

-- Refresh policy: refresh last 7 days daily
SELECT add_continuous_aggregate_policy(
    'violation_user_patterns',
    start_offset => INTERVAL '7 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

COMMENT ON MATERIALIZED VIEW violation_user_patterns IS
    'Daily user violation patterns for behavior analysis';

-- ----------------------------------------------------------------------------
-- Create indexes on materialized views for faster queries
-- ----------------------------------------------------------------------------
CREATE INDEX idx_hourly_bucket ON violation_stats_hourly (bucket DESC);
CREATE INDEX idx_daily_bucket ON violation_stats_daily (bucket DESC);
CREATE INDEX idx_attempt_summary_attempt ON violation_attempt_summary (attempt_id);
CREATE INDEX idx_attempt_summary_user ON violation_attempt_summary (user_id);
CREATE INDEX idx_user_patterns_user ON violation_user_patterns (user_id, bucket DESC);

-- ----------------------------------------------------------------------------
-- Initial refresh (populate with existing data)
-- ----------------------------------------------------------------------------
-- Note: This might take some time if there's a lot of existing data
CALL refresh_continuous_aggregate('violation_stats_hourly', NULL, NULL);
CALL refresh_continuous_aggregate('violation_stats_daily', NULL, NULL);
CALL refresh_continuous_aggregate('violation_attempt_summary', NULL, NULL);
CALL refresh_continuous_aggregate('violation_user_patterns', NULL, NULL);
