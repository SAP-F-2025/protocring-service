-- Create violations table
CREATE TABLE IF NOT EXISTS violation_logs (
    id BIGSERIAL PRIMARY KEY,
    attempt_id BIGINT NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    assessment_id BIGINT NOT NULL,

    -- Classification
    violation_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    confidence_score DECIMAL(5,4),

    -- MediaPipe detection data (JSONB)
    detection_data JSONB NOT NULL,

    -- Extracted metrics (denormalized for fast queries)
    face_count INT,
    hand_count INT,
    head_pose_yaw DOUBLE PRECISION,
    head_pose_pitch DOUBLE PRECISION,
    head_pose_roll DOUBLE PRECISION,
    gaze_direction VARCHAR(50),
    mouth_open_ratio DOUBLE PRECISION,

    -- Frame metadata
    frame_number INT,
    frame_timestamp BIGINT,
    fps INT,
    resolution VARCHAR(50),

    -- Evidence
    snapshot_url TEXT,
    video_segment_url TEXT,

    -- Context
    browser_info JSONB,
    device_fingerprint VARCHAR(255),

    -- Timestamps
    client_timestamp TIMESTAMPTZ NOT NULL,
    server_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Create indexes
CREATE INDEX idx_violation_attempt ON violation_logs (attempt_id, server_timestamp DESC);
CREATE INDEX idx_violation_user ON violation_logs (user_id, server_timestamp DESC);
CREATE INDEX idx_violation_type ON violation_logs (violation_type);
CREATE INDEX idx_violation_severity ON violation_logs (severity);
CREATE INDEX idx_violation_timestamp ON violation_logs (server_timestamp DESC);

-- Create GIN indexes for JSONB fields for faster queries
CREATE INDEX idx_violation_detection_data ON violation_logs USING GIN (detection_data);
CREATE INDEX idx_violation_browser_info ON violation_logs USING GIN (browser_info);

-- Convert to hypertable (partitioned by server_timestamp)
-- Chunk interval: 1 day (violations are time-series data)
SELECT create_hypertable(
    'violation_logs',
    'server_timestamp',
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

-- Add refresh policy comments for continuous aggregates (created in next migration)
COMMENT ON TABLE violation_logs IS 'Stores MediaPipe violation detection events as time-series data';
