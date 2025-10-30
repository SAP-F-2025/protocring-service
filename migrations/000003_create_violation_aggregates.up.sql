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
    time_bucket('1 hour', server_timestamp) AS bucket,
    COUNT(*) AS total_violations,
    COUNT(DISTINCT attempt_id) AS unique_attempts,
    COUNT(DISTINCT user_id) AS unique_users,

    -- Severity breakdown
    COUNT(*) FILTER (WHERE severity = 'critical') AS critical_count,
    COUNT(*) FILTER (WHERE severity = 'high') AS high_count,
    COUNT(*) FILTER (WHERE severity = 'medium') AS medium_count,
    COUNT(*) FILTER (WHERE severity = 'low') AS low_count,

    -- Violation type breakdown (top types)
    COUNT(*) FILTER (WHERE violation_type = 'face_not_detected') AS face_not_detected_count,
    COUNT(*) FILTER (WHERE violation_type = 'multiple_faces') AS multiple_faces_count,
    COUNT(*) FILTER (WHERE violation_type = 'looking_away') AS looking_away_count,
    COUNT(*) FILTER (WHERE violation_type = 'hand_detected') AS hand_detected_count,

    -- Average confidence
    AVG(confidence_score) AS avg_confidence,

    -- Behavioral metrics
    AVG(head_pose_yaw) AS avg_head_yaw,
    AVG(mouth_open_ratio) AS avg_mouth_ratio
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
    time_bucket('1 day', server_timestamp) AS bucket,
    COUNT(*) AS total_violations,
    COUNT(DISTINCT attempt_id) AS unique_attempts,
    COUNT(DISTINCT user_id) AS unique_users,
    COUNT(DISTINCT assessment_id) AS unique_assessments,

    -- Severity breakdown
    COUNT(*) FILTER (WHERE severity = 'critical') AS critical_count,
    COUNT(*) FILTER (WHERE severity = 'high') AS high_count,
    COUNT(*) FILTER (WHERE severity = 'medium') AS medium_count,
    COUNT(*) FILTER (WHERE severity = 'low') AS low_count,

    -- Violation type stats
    COUNT(*) FILTER (WHERE violation_type = 'face_not_detected') AS face_not_detected_count,
    COUNT(*) FILTER (WHERE violation_type = 'multiple_faces') AS multiple_faces_count,
    COUNT(*) FILTER (WHERE violation_type = 'looking_away') AS looking_away_count,
    COUNT(*) FILTER (WHERE violation_type = 'mouth_open') AS mouth_open_count,
    COUNT(*) FILTER (WHERE violation_type = 'hand_detected') AS hand_detected_count,
    COUNT(*) FILTER (WHERE violation_type = 'person_left') AS person_left_count,

    -- Detection metrics
    AVG(confidence_score) AS avg_confidence,
    AVG(face_count) AS avg_faces,
    AVG(hand_count) AS avg_hands
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
    MIN(server_timestamp) AS first_violation_at,
    MAX(server_timestamp) AS last_violation_at,
    MAX(server_timestamp) - MIN(server_timestamp) AS duration,

    -- Counts
    COUNT(*) AS total_violations,
    COUNT(DISTINCT violation_type) AS unique_violation_types,

    -- Severity breakdown
    COUNT(*) FILTER (WHERE severity = 'critical') AS critical_count,
    COUNT(*) FILTER (WHERE severity = 'high') AS high_count,
    COUNT(*) FILTER (WHERE severity = 'medium') AS medium_count,
    COUNT(*) FILTER (WHERE severity = 'low') AS low_count,

    -- Most severe violation
    MAX(CASE
        WHEN severity = 'critical' THEN 4
        WHEN severity = 'high' THEN 3
        WHEN severity = 'medium' THEN 2
        WHEN severity = 'low' THEN 1
        ELSE 0
    END) AS max_severity_level,

    -- Violation types (array aggregation)
    array_agg(DISTINCT violation_type) AS violation_types,

    -- Behavioral metrics
    AVG(confidence_score) AS avg_confidence,
    MAX(confidence_score) AS max_confidence,
    AVG(face_count) AS avg_faces,
    MAX(face_count) AS max_faces,
    AVG(hand_count) AS avg_hands,

    -- Frame coverage
    COUNT(DISTINCT frame_number) AS frames_with_violations,
    MAX(frame_number) - MIN(frame_number) AS frame_span
FROM violation_logs
WHERE server_timestamp > NOW() - INTERVAL '30 days'  -- Only recent attempts
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
    time_bucket('1 day', server_timestamp) AS bucket,
    user_id,

    -- Counts
    COUNT(*) AS total_violations,
    COUNT(DISTINCT attempt_id) AS attempts_count,
    COUNT(DISTINCT assessment_id) AS assessments_count,

    -- Severity distribution
    COUNT(*) FILTER (WHERE severity = 'critical') AS critical_count,
    COUNT(*) FILTER (WHERE severity = 'high') AS high_count,

    -- Most common violation types
    MODE() WITHIN GROUP (ORDER BY violation_type) AS most_common_violation,
    COUNT(DISTINCT violation_type) AS unique_violation_types,

    -- Behavioral flags
    BOOL_OR(violation_type = 'multiple_faces') AS has_multiple_faces,
    BOOL_OR(violation_type = 'person_left') AS has_left_screen,

    -- Metrics
    AVG(confidence_score) AS avg_confidence
FROM violation_logs
WHERE server_timestamp > NOW() - INTERVAL '90 days'
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
