package postgresql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"protocring-service/internal/repository"
	"time"

	"protocring-service/internal/model"

	"github.com/jmoiron/sqlx"
)

// ViolationRepository handles violation data operations
type ViolationRepository struct {
	db *sqlx.DB
}

// NewViolationRepository creates a new violation repository
func NewViolationRepository(db *sqlx.DB) *ViolationRepository {
	return &ViolationRepository{db: db}
}

// Insert inserts a single violation
func (r *ViolationRepository) Insert(ctx context.Context, violation *model.ViolationLog) error {
	// Marshal JSONB fields
	detectionDataJSON, err := json.Marshal(violation.DetectionData)
	if err != nil {
		return fmt.Errorf("failed to marshal detection_data: %w", err)
	}

	browserInfoJSON, err := json.Marshal(violation.BrowserInfo)
	if err != nil {
		return fmt.Errorf("failed to marshal browser_info: %w", err)
	}

	query := `
		INSERT INTO violation_logs (
			attempt_id, user_id, assessment_id,
			violation_type, severity, confidence_score,
			detection_data,
			face_count, hand_count,
			head_pose_yaw, head_pose_pitch, head_pose_roll,
			gaze_direction, mouth_open_ratio,
			frame_number, frame_timestamp, fps, resolution,
			snapshot_url, video_segment_url,
			browser_info, device_fingerprint,
			client_timestamp, server_timestamp
		) VALUES (
			$1, $2, $3,
			$4, $5, $6,
			$7,
			$8, $9,
			$10, $11, $12,
			$13, $14,
			$15, $16, $17, $18,
			$19, $20,
			$21, $22,
			$23, NOW()
		) RETURNING id, server_timestamp, created_at
	`

	err = r.db.QueryRowContext(
		ctx,
		query,
		violation.AttemptID, violation.UserID, violation.AssessmentID,
		violation.ViolationType, violation.Severity, violation.ConfidenceScore,
		detectionDataJSON,
		violation.FaceCount, violation.HandCount,
		violation.HeadPoseYaw, violation.HeadPosePitch, violation.HeadPoseRoll,
		violation.GazeDirection, violation.MouthOpenRatio,
		violation.FrameNumber, violation.FrameTimestamp, violation.FPS, violation.Resolution,
		violation.SnapshotURL, violation.VideoSegmentURL,
		browserInfoJSON, violation.DeviceFingerprint,
		violation.ClientTimestamp,
	).Scan(&violation.ID, &violation.ServerTimestamp, &violation.CreatedAt)

	if err != nil {
		return fmt.Errorf("failed to insert violation: %w", err)
	}

	return nil
}

// InsertBatch inserts multiple violations in a transaction
func (r *ViolationRepository) InsertBatch(ctx context.Context, violations []*model.ViolationLog) error {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	query := `
		INSERT INTO violation_logs (
			attempt_id, user_id, assessment_id,
			violation_type, severity, confidence_score,
			detection_data,
			face_count, hand_count,
			head_pose_yaw, head_pose_pitch, head_pose_roll,
			gaze_direction, mouth_open_ratio,
			frame_number, frame_timestamp, fps, resolution,
			snapshot_url, video_segment_url,
			browser_info, device_fingerprint,
			client_timestamp, server_timestamp
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
			$11, $12, $13, $14, $15, $16, $17, $18,
			$19, $20, $21, $22, $23, NOW()
		)
	`

	stmt, err := tx.PreparexContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, violation := range violations {
		detectionDataJSON, _ := json.Marshal(violation.DetectionData)
		browserInfoJSON, _ := json.Marshal(violation.BrowserInfo)

		_, err := stmt.ExecContext(
			ctx,
			violation.AttemptID, violation.UserID, violation.AssessmentID,
			violation.ViolationType, violation.Severity, violation.ConfidenceScore,
			detectionDataJSON,
			violation.FaceCount, violation.HandCount,
			violation.HeadPoseYaw, violation.HeadPosePitch, violation.HeadPoseRoll,
			violation.GazeDirection, violation.MouthOpenRatio,
			violation.FrameNumber, violation.FrameTimestamp, violation.FPS, violation.Resolution,
			violation.SnapshotURL, violation.VideoSegmentURL,
			browserInfoJSON, violation.DeviceFingerprint,
			violation.ClientTimestamp,
		)

		if err != nil {
			return fmt.Errorf("failed to insert violation: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// FindByAttemptID finds all violations for an attempt
func (r *ViolationRepository) FindByAttemptID(ctx context.Context, attemptID uint64, limit, offset int) ([]*model.ViolationLog, error) {
	query := `
		SELECT
			id, attempt_id, user_id, assessment_id,
			violation_type, severity, confidence_score,
			detection_data, browser_info,
			face_count, hand_count,
			head_pose_yaw, head_pose_pitch, head_pose_roll,
			gaze_direction, mouth_open_ratio,
			frame_number, frame_timestamp, fps, resolution,
			snapshot_url, video_segment_url,
			device_fingerprint,
			client_timestamp, server_timestamp, created_at
		FROM violation_logs
		WHERE attempt_id = $1
		ORDER BY server_timestamp DESC
		LIMIT $2 OFFSET $3
	`

	rows, err := r.db.QueryxContext(ctx, query, attemptID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to query violations: %w", err)
	}
	defer rows.Close()

	var violations []*model.ViolationLog
	for rows.Next() {
		var v model.ViolationLog
		var detectionDataJSON, browserInfoJSON []byte

		err := rows.Scan(
			&v.ID, &v.AttemptID, &v.UserID, &v.AssessmentID,
			&v.ViolationType, &v.Severity, &v.ConfidenceScore,
			&detectionDataJSON, &browserInfoJSON,
			&v.FaceCount, &v.HandCount,
			&v.HeadPoseYaw, &v.HeadPosePitch, &v.HeadPoseRoll,
			&v.GazeDirection, &v.MouthOpenRatio,
			&v.FrameNumber, &v.FrameTimestamp, &v.FPS, &v.Resolution,
			&v.SnapshotURL, &v.VideoSegmentURL,
			&v.DeviceFingerprint,
			&v.ClientTimestamp, &v.ServerTimestamp, &v.CreatedAt,
		)

		if err != nil {
			return nil, fmt.Errorf("failed to scan violation: %w", err)
		}

		// Unmarshal JSONB fields
		json.Unmarshal(detectionDataJSON, &v.DetectionData)
		json.Unmarshal(browserInfoJSON, &v.BrowserInfo)

		violations = append(violations, &v)
	}

	return violations, nil
}

// FindByUserID finds violations for a user within time range
func (r *ViolationRepository) FindByUserID(ctx context.Context, userID string, startTime, endTime time.Time, limit, offset int) ([]*model.ViolationLog, error) {
	query := `
		SELECT
			id, attempt_id, user_id, assessment_id,
			violation_type, severity, confidence_score,
			frame_number, frame_timestamp,
			snapshot_url,
			client_timestamp, server_timestamp
		FROM violation_logs
		WHERE user_id = $1
		  AND server_timestamp >= $2
		  AND server_timestamp <= $3
		ORDER BY server_timestamp DESC
		LIMIT $4 OFFSET $5
	`

	rows, err := r.db.QueryxContext(ctx, query, userID, startTime, endTime, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to query violations: %w", err)
	}
	defer rows.Close()

	var violations []*model.ViolationLog
	for rows.Next() {
		var v model.ViolationLog
		err := rows.StructScan(&v)
		if err != nil {
			return nil, fmt.Errorf("failed to scan violation: %w", err)
		}
		violations = append(violations, &v)
	}

	return violations, nil
}

// FindByTimeRange finds violations within a time range
func (r *ViolationRepository) FindByTimeRange(ctx context.Context, startTime, endTime time.Time, violationType string, severity model.Severity, limit, offset int) ([]*model.ViolationLog, error) {
	query := `
		SELECT
			id, attempt_id, user_id, assessment_id,
			violation_type, severity, confidence_score,
			frame_number, server_timestamp
		FROM violation_logs
		WHERE server_timestamp >= $1 AND server_timestamp <= $2
	`

	args := []interface{}{startTime, endTime}
	argPos := 3

	if violationType != "" {
		query += fmt.Sprintf(" AND violation_type = $%d", argPos)
		args = append(args, violationType)
		argPos++
	}

	if severity != "" {
		query += fmt.Sprintf(" AND severity = $%d", argPos)
		args = append(args, severity)
		argPos++
	}

	query += " ORDER BY server_timestamp DESC"
	query += fmt.Sprintf(" LIMIT $%d OFFSET $%d", argPos, argPos+1)
	args = append(args, limit, offset)

	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query violations: %w", err)
	}
	defer rows.Close()

	var violations []*model.ViolationLog
	for rows.Next() {
		var v model.ViolationLog
		err := rows.StructScan(&v)
		if err != nil {
			return nil, fmt.Errorf("failed to scan violation: %w", err)
		}
		violations = append(violations, &v)
	}

	return violations, nil
}

// GetLatestByAttempt gets the most recent violation for an attempt
func (r *ViolationRepository) GetLatestByAttempt(ctx context.Context, attemptID uint64) (*model.ViolationLog, error) {
	query := `
		SELECT
			id, attempt_id, user_id, assessment_id,
			violation_type, severity, confidence_score,
			frame_number, frame_timestamp,
			server_timestamp, client_timestamp
		FROM violation_logs
		WHERE attempt_id = $1
		ORDER BY server_timestamp DESC
		LIMIT 1
	`

	var v model.ViolationLog
	err := r.db.GetContext(ctx, &v, query, attemptID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get latest violation: %w", err)
	}

	return &v, nil
}

// CountByAttempt counts violations for an attempt
func (r *ViolationRepository) CountByAttempt(ctx context.Context, attemptID uint64) (int64, error) {
	query := `SELECT COUNT(*) FROM violation_logs WHERE attempt_id = $1`

	var count int64
	err := r.db.GetContext(ctx, &count, query, attemptID)
	if err != nil {
		return 0, fmt.Errorf("failed to count violations: %w", err)
	}

	return count, nil
}

// CountByType counts violations by type for an attempt
func (r *ViolationRepository) CountByType(ctx context.Context, attemptID uint64) (map[string]int64, error) {
	query := `
		SELECT violation_type, COUNT(*) as count
		FROM violation_logs
		WHERE attempt_id = $1
		GROUP BY violation_type
	`

	rows, err := r.db.QueryxContext(ctx, query, attemptID)
	if err != nil {
		return nil, fmt.Errorf("failed to count by type: %w", err)
	}
	defer rows.Close()

	counts := make(map[string]int64)
	for rows.Next() {
		var violationType string
		var count int64
		if err := rows.Scan(&violationType, &count); err != nil {
			return nil, err
		}
		counts[violationType] = count
	}

	return counts, nil
}

// GetViolationTimeline gets violation timeline with aggregations
func (r *ViolationRepository) GetViolationTimeline(ctx context.Context, attemptID uint64, bucketSize time.Duration) ([]repository.ViolationTimelineBucket, error) {
	query := `
		SELECT
			time_bucket($1, server_timestamp) AS bucket,
			COUNT(*) AS violation_count,
			COUNT(*) FILTER (WHERE severity = 'critical') AS critical_count,
			COUNT(*) FILTER (WHERE severity = 'high') AS high_count,
			COUNT(*) FILTER (WHERE severity = 'medium') AS medium_count,
			COUNT(*) FILTER (WHERE severity = 'low') AS low_count
		FROM violation_logs
		WHERE attempt_id = $2
		GROUP BY bucket
		ORDER BY bucket ASC
	`

	rows, err := r.db.QueryxContext(ctx, query, bucketSize.String(), attemptID)
	if err != nil {
		return nil, fmt.Errorf("failed to get timeline: %w", err)
	}
	defer rows.Close()

	var timeline []repository.ViolationTimelineBucket
	for rows.Next() {
		var bucket repository.ViolationTimelineBucket
		err := rows.StructScan(&bucket)
		if err != nil {
			return nil, fmt.Errorf("failed to scan bucket: %w", err)
		}
		timeline = append(timeline, bucket)
	}

	return timeline, nil
}

// GetSeverityDistribution gets severity distribution for an attempt
func (r *ViolationRepository) GetSeverityDistribution(ctx context.Context, attemptID uint64) (map[model.Severity]int64, error) {
	query := `
		SELECT severity, COUNT(*) as count
		FROM violation_logs
		WHERE attempt_id = $1
		GROUP BY severity
	`

	rows, err := r.db.QueryxContext(ctx, query, attemptID)
	if err != nil {
		return nil, fmt.Errorf("failed to get severity distribution: %w", err)
	}
	defer rows.Close()

	distribution := make(map[model.Severity]int64)
	for rows.Next() {
		var severity model.Severity
		var count int64
		if err := rows.Scan(&severity, &count); err != nil {
			return nil, err
		}
		distribution[severity] = count
	}

	return distribution, nil
}

// DeleteOldViolations deletes violations older than retention period
func (r *ViolationRepository) DeleteOldViolations(ctx context.Context, olderThan time.Duration) (int64, error) {
	query := `DELETE FROM violation_logs WHERE server_timestamp < NOW() - $1::interval`

	result, err := r.db.ExecContext(ctx, query, olderThan.String())
	if err != nil {
		return 0, fmt.Errorf("failed to delete old violations: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	return rowsAffected, nil
}
