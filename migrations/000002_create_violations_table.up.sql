-- Create violations table
CREATE TABLE IF NOT EXISTS violation_logs (
    id BIGSERIAL PRIMARY KEY,
    attempt_id BIGINT NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    assessment_id BIGINT NOT NULL,

    -- Classification
    violation_type INT NOT NULL,
    severity INT NOT NULL,
    confidence_score DOUBLE PRECISION,

    -- Evidence
    snapshot_url TEXT,

    -- Context
    browser_info JSONB,
    device_fingerprint VARCHAR(255),

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at TIMESTAMPTZ,
    is_prolonged BOOLEAN NOT NULL DEFAULT FALSE
);

-- Create indexes
CREATE INDEX idx_violation_attempt ON violation_logs (attempt_id, created_at DESC);
CREATE INDEX idx_violation_user ON violation_logs (user_id, created_at DESC);
CREATE INDEX idx_violation_assessment ON violation_logs (assessment_id, created_at DESC);
CREATE INDEX idx_violation_type ON violation_logs (violation_type);
CREATE INDEX idx_violation_severity ON violation_logs (severity);
CREATE INDEX idx_violation_timestamp ON violation_logs (created_at DESC);
CREATE INDEX idx_violation_prolonged ON violation_logs (is_prolonged) WHERE is_prolonged = TRUE;

-- Create GIN index for JSONB field for faster queries
CREATE INDEX idx_violation_browser_info ON violation_logs USING GIN (browser_info);

-- Convert to hypertable (partitioned by created_at)
-- Chunk interval: 1 day (violations are time-series data)
SELECT create_hypertable(
    'violation_logs',
    'created_at',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Add compression policy
-- Compress chunks older than 7 days to save storage (90-95% compression)
SELECT add_compression_policy(
    'violation_logs',
    compress_after => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- Add retention policy
-- Keep data for 90 days, then automatically delete
SELECT add_retention_policy(
    'violation_logs',
    drop_after => INTERVAL '90 days',
    if_not_exists => TRUE
);

-- Add table comment
COMMENT ON TABLE violation_logs IS 'Stores violation detection events as time-series data';
COMMENT ON COLUMN violation_logs.violation_type IS 'Type of violation (0-14): FaceNotDetected, MultipleFaces, LookingAway, etc.';
COMMENT ON COLUMN violation_logs.severity IS 'Severity level (0-3): Low, Medium, High, Critical';
COMMENT ON COLUMN violation_logs.is_prolonged IS 'Indicates if violation lasted longer than threshold';
