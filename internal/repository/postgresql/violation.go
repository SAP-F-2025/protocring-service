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
	// Marshal JSONB field
	browserInfoJSON, err := json.Marshal(violation.BrowserInfo)
	if err != nil {
		return fmt.Errorf("failed to marshal browser_info: %w", err)
	}

	query := `
		INSERT INTO violation_logs (
			attempt_id, user_id, assessment_id,
			violation_type, severity, confidence_score,
			snapshot_url,
			browser_info, device_fingerprint
		) VALUES (
			$1, $2, $3,
			$4, $5, $6,
			$7,
			$8, $9
		) RETURNING id, created_at
	`

	err = r.db.QueryRowContext(
		ctx,
		query,
		violation.AttemptID, violation.UserID, violation.AssessmentID,
		violation.ViolationType, violation.Severity, violation.ConfidenceScore,
		violation.SnapshotURL,
		browserInfoJSON, violation.DeviceFingerprint,
	).Scan(&violation.ID, &violation.CreatedAt)

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
			snapshot_url,
			browser_info, device_fingerprint
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9
		)
	`

	stmt, err := tx.PreparexContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, violation := range violations {
		browserInfoJSON, _ := json.Marshal(violation.BrowserInfo)

		_, err := stmt.ExecContext(
			ctx,
			violation.AttemptID, violation.UserID, violation.AssessmentID,
			violation.ViolationType, violation.Severity, violation.ConfidenceScore,
			violation.SnapshotURL,
			browserInfoJSON, violation.DeviceFingerprint,
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
			snapshot_url,
			browser_info, device_fingerprint,
			created_at
		FROM violation_logs
		WHERE attempt_id = $1
		ORDER BY created_at DESC
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
		var browserInfoJSON []byte

		err := rows.Scan(
			&v.ID, &v.AttemptID, &v.UserID, &v.AssessmentID,
			&v.ViolationType, &v.Severity, &v.ConfidenceScore,
			&v.SnapshotURL,
			&browserInfoJSON, &v.DeviceFingerprint,
			&v.CreatedAt,
		)

		if err != nil {
			return nil, fmt.Errorf("failed to scan violation: %w", err)
		}

		// Unmarshal JSONB field
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
			snapshot_url,
			created_at
		FROM violation_logs
		WHERE user_id = $1
		  AND created_at >= $2
		  AND created_at <= $3
		ORDER BY created_at DESC
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
func (r *ViolationRepository) FindByTimeRange(ctx context.Context, startTime, endTime time.Time, violationType *int, severity *int, limit, offset int) ([]*model.ViolationLog, error) {
	query := `
		SELECT
			id, attempt_id, user_id, assessment_id,
			violation_type, severity, confidence_score,
			snapshot_url, created_at
		FROM violation_logs
		WHERE created_at >= $1 AND created_at <= $2
	`

	args := []interface{}{startTime, endTime}
	argPos := 3

	if violationType != nil {
		query += fmt.Sprintf(" AND violation_type = $%d", argPos)
		args = append(args, *violationType)
		argPos++
	}

	if severity != nil {
		query += fmt.Sprintf(" AND severity = $%d", argPos)
		args = append(args, *severity)
		argPos++
	}

	query += " ORDER BY created_at DESC"
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
			snapshot_url, created_at
		FROM violation_logs
		WHERE attempt_id = $1
		ORDER BY created_at DESC
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
			time_bucket($1, created_at) AS bucket,
			COUNT(*) AS violation_count,
			COUNT(*) FILTER (WHERE severity = 3) AS critical_count,
			COUNT(*) FILTER (WHERE severity = 2) AS high_count,
			COUNT(*) FILTER (WHERE severity = 1) AS medium_count,
			COUNT(*) FILTER (WHERE severity = 0) AS low_count
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
func (r *ViolationRepository) GetSeverityDistribution(ctx context.Context, attemptID uint64) (map[int]int64, error) {
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

	distribution := make(map[int]int64)
	for rows.Next() {
		var severity int
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
	query := `DELETE FROM violation_logs WHERE created_at < NOW() - $1::interval`

	result, err := r.db.ExecContext(ctx, query, olderThan.String())
	if err != nil {
		return 0, fmt.Errorf("failed to delete old violations: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	return rowsAffected, nil
}
