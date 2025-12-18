package postgresql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/lib/pq"
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

// InsertBatch inserts multiple violations in a transaction using bulk insert
func (r *ViolationRepository) InsertBatch(ctx context.Context, violations []*model.ViolationLog) error {
	if len(violations) == 0 {
		return nil
	}

	const batchSize = 1000 // Process in chunks to avoid hitting parameter limits

	for i := 0; i < len(violations); i += batchSize {
		end := i + batchSize
		if end > len(violations) {
			end = len(violations)
		}

		batch := violations[i:end]
		if err := r.insertBatchChunk(ctx, batch); err != nil {
			return err
		}
	}

	return nil
}

// insertBatchChunk inserts a chunk of violations using bulk insert
func (r *ViolationRepository) insertBatchChunk(ctx context.Context, violations []*model.ViolationLog) error {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Build bulk insert query with multiple VALUES
	query := `
		INSERT INTO violation_logs (
			attempt_id, user_id, assessment_id,
			violation_type, severity, confidence_score,
			snapshot_url,
			browser_info, device_fingerprint
		) VALUES `

	// Pre-allocate args slice
	args := make([]interface{}, 0, len(violations)*9)
	values := make([]string, 0, len(violations))

	// Build VALUES clauses and args
	for i, violation := range violations {
		browserInfoJSON, err := json.Marshal(violation.BrowserInfo)
		if err != nil {
			return fmt.Errorf("failed to marshal browser_info for violation %d: %w", i, err)
		}

		// Calculate parameter positions for this row
		offset := i * 9
		values = append(values, fmt.Sprintf(
			"($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			offset+1, offset+2, offset+3, offset+4, offset+5, offset+6, offset+7, offset+8, offset+9,
		))

		// Add args in order
		args = append(args,
			violation.AttemptID,
			violation.UserID,
			violation.AssessmentID,
			violation.ViolationType,
			violation.Severity,
			violation.ConfidenceScore,
			violation.SnapshotURL,
			browserInfoJSON,
			violation.DeviceFingerprint,
		)
	}

	// Combine query with all VALUES
	query += fmt.Sprintf("%s", values[0])
	for i := 1; i < len(values); i++ {
		query += fmt.Sprintf(", %s", values[i])
	}

	// Execute bulk insert
	_, err = tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to bulk insert violations: %w", err)
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

// ============================================================================
// Dashboard Analytics Methods - Query Continuous Aggregates
// ============================================================================

// GetHourlyStats gets hourly violation statistics from continuous aggregate
func (r *ViolationRepository) GetHourlyStats(ctx context.Context, startTime, endTime time.Time) ([]*model.HourlyViolationStats, error) {
	query := `
		SELECT
			bucket,
			total_violations,
			unique_attempts,
			unique_users,
			critical_count,
			high_count,
			medium_count,
			low_count,
			face_not_detected_count,
			multiple_faces_count,
			looking_away_count,
			hand_detected_count,
			switching_tab_count,
			fullscreen_count,
			prolonged_count,
			avg_confidence
		FROM violation_stats_hourly
		WHERE bucket >= $1 AND bucket < $2
		ORDER BY bucket DESC
	`

	var stats []*model.HourlyViolationStats
	err := r.db.SelectContext(ctx, &stats, query, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("failed to get hourly stats: %w", err)
	}

	return stats, nil
}

// GetDailyStats gets daily violation statistics from continuous aggregate
func (r *ViolationRepository) GetDailyStats(ctx context.Context, startTime, endTime time.Time) ([]*model.DailyViolationStats, error) {
	query := `
		SELECT
			bucket,
			total_violations,
			unique_attempts,
			unique_users,
			unique_assessments,
			critical_count,
			high_count,
			medium_count,
			low_count,
			face_not_detected_count,
			multiple_faces_count,
			looking_away_count,
			mouth_open_count,
			hand_detected_count,
			copy_paste_count,
			switching_tab_count,
			fullscreen_count,
			phone_detect_count,
			prolonged_count,
			avg_duration_seconds,
			avg_confidence
		FROM violation_stats_daily
		WHERE bucket >= $1 AND bucket < $2
		ORDER BY bucket DESC
	`

	var stats []*model.DailyViolationStats
	err := r.db.SelectContext(ctx, &stats, query, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("failed to get daily stats: %w", err)
	}

	return stats, nil
}

// GetAttemptSummary gets violation summary for a specific attempt
func (r *ViolationRepository) GetAttemptSummary(ctx context.Context, attemptID uint64) (*model.AttemptViolationSummary, error) {
	query := `
		SELECT
			attempt_id,
			user_id,
			assessment_id,
			first_violation_at,
			last_violation_at,
			duration_seconds,
			total_violations,
			unique_violation_types,
			prolonged_violations_count,
			critical_count,
			high_count,
			medium_count,
			low_count,
			max_severity_level,
			violation_types,
			avg_confidence,
			max_confidence,
			min_confidence
		FROM violation_attempt_summary
		WHERE attempt_id = $1
	`

	var summary model.AttemptViolationSummary
	err := r.db.GetContext(ctx, &summary, query, attemptID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get attempt summary: %w", err)
	}

	return &summary, nil
}

// GetAttemptSummaries gets violation summaries for multiple attempts
func (r *ViolationRepository) GetAttemptSummaries(ctx context.Context, attemptIDs []uint64) ([]*model.AttemptViolationSummary, error) {
	if len(attemptIDs) == 0 {
		return []*model.AttemptViolationSummary{}, nil
	}

	query := `
		SELECT
			attempt_id,
			user_id,
			assessment_id,
			first_violation_at,
			last_violation_at,
			duration_seconds,
			total_violations,
			unique_violation_types,
			prolonged_violations_count,
			critical_count,
			high_count,
			medium_count,
			low_count,
			max_severity_level,
			violation_types,
			avg_confidence,
			max_confidence,
			min_confidence
		FROM violation_attempt_summary
		WHERE attempt_id = ANY($1)
		ORDER BY first_violation_at DESC
	`

	var summaries []*model.AttemptViolationSummary
	err := r.db.SelectContext(ctx, &summaries, query, pq.Array(attemptIDs))
	if err != nil {
		return nil, fmt.Errorf("failed to get attempt summaries: %w", err)
	}

	return summaries, nil
}

// GetUserPatterns gets daily violation patterns for a specific user
func (r *ViolationRepository) GetUserPatterns(ctx context.Context, userID string, startTime, endTime time.Time) ([]*model.UserViolationPattern, error) {
	query := `
		SELECT
			bucket,
			user_id,
			total_violations,
			attempts_count,
			assessments_count,
			critical_count,
			high_count,
			prolonged_count,
			most_common_violation,
			unique_violation_types,
			has_multiple_faces,
			has_hand_detected,
			has_switching_tab,
			has_fullscreen_exit,
			avg_confidence
		FROM violation_user_patterns
		WHERE user_id = $1
		  AND bucket >= $2
		  AND bucket < $3
		ORDER BY bucket DESC
	`

	var patterns []*model.UserViolationPattern
	err := r.db.SelectContext(ctx, &patterns, query, userID, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("failed to get user patterns: %w", err)
	}

	return patterns, nil
}

// GetDashboardOverview gets high-level dashboard metrics
func (r *ViolationRepository) GetDashboardOverview(ctx context.Context, startTime, endTime time.Time) (*model.DashboardOverview, error) {
	// Calculate period duration for comparison
	periodDuration := endTime.Sub(startTime)
	prevStartTime := startTime.Add(-periodDuration)
	prevEndTime := startTime

	// Query current period stats
	currentQuery := `
		SELECT
			COALESCE(SUM(total_violations), 0) as total_violations,
			COALESCE(SUM(unique_attempts), 0) as total_attempts,
			COALESCE(SUM(unique_users), 0) as total_users,
			COALESCE(SUM(unique_assessments), 0) as total_assessments,
			COALESCE(SUM(critical_count), 0) as critical_count,
			COALESCE(SUM(high_count), 0) as high_count,
			COALESCE(SUM(medium_count), 0) as medium_count,
			COALESCE(SUM(low_count), 0) as low_count
		FROM violation_stats_daily
		WHERE bucket >= $1 AND bucket < $2
	`

	var overview model.DashboardOverview
	err := r.db.GetContext(ctx, &overview, currentQuery, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("failed to get current period stats: %w", err)
	}

	// Query previous period stats for comparison
	var prevStats struct {
		TotalViolations int64 `db:"total_violations"`
		TotalAttempts   int64 `db:"total_attempts"`
		TotalUsers      int64 `db:"total_users"`
	}

	prevQuery := `
		SELECT
			COALESCE(SUM(total_violations), 0) as total_violations,
			COALESCE(SUM(unique_attempts), 0) as total_attempts,
			COALESCE(SUM(unique_users), 0) as total_users
		FROM violation_stats_daily
		WHERE bucket >= $1 AND bucket < $2
	`

	err = r.db.GetContext(ctx, &prevStats, prevQuery, prevStartTime, prevEndTime)
	if err != nil {
		return nil, fmt.Errorf("failed to get previous period stats: %w", err)
	}

	// Calculate percentage changes
	if prevStats.TotalViolations > 0 {
		overview.ViolationsChange = float64(overview.TotalViolations-prevStats.TotalViolations) / float64(prevStats.TotalViolations) * 100
	}
	if prevStats.TotalAttempts > 0 {
		overview.AttemptsChange = float64(overview.TotalAttempts-prevStats.TotalAttempts) / float64(prevStats.TotalAttempts) * 100
	}
	if prevStats.TotalUsers > 0 {
		overview.UsersChange = float64(overview.TotalUsers-prevStats.TotalUsers) / float64(prevStats.TotalUsers) * 100
	}

	// Get top violation types
	topTypesQuery := `
		SELECT
			violation_type,
			COUNT(*) as count
		FROM violation_logs
		WHERE created_at >= $1 AND created_at < $2
		GROUP BY violation_type
		ORDER BY count DESC
		LIMIT 5
	`

	rows, err := r.db.QueryxContext(ctx, topTypesQuery, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("failed to get top violation types: %w", err)
	}
	defer rows.Close()

	var topTypes []model.ViolationTypeCount
	totalForPercentage := overview.TotalViolations
	if totalForPercentage == 0 {
		totalForPercentage = 1 // Avoid division by zero
	}

	for rows.Next() {
		var vt model.ViolationTypeCount
		if err := rows.Scan(&vt.ViolationType, &vt.Count); err != nil {
			return nil, fmt.Errorf("failed to scan violation type: %w", err)
		}
		vt.TypeName = getViolationTypeName(vt.ViolationType)
		vt.Percentage = float64(vt.Count) / float64(totalForPercentage) * 100
		topTypes = append(topTypes, vt)
	}

	overview.TopViolationTypes = topTypes

	return &overview, nil
}

// GetRealTimeStats gets real-time violation statistics
func (r *ViolationRepository) GetRealTimeStats(ctx context.Context) (*model.RealTimeStats, error) {
	now := time.Now()
	fiveMinAgo := now.Add(-5 * time.Minute)
	oneHourAgo := now.Add(-1 * time.Hour)

	query := `
		SELECT
			COUNT(DISTINCT attempt_id) FILTER (WHERE created_at >= $1) as active_attempts,
			COUNT(*) FILTER (WHERE created_at >= $2) as violations_last_5min,
			COUNT(*) FILTER (WHERE created_at >= $3) as violations_last_hour,
			COUNT(*) FILTER (WHERE created_at >= $2 AND severity = 3) as critical_violations
		FROM violation_logs
		WHERE created_at >= $3
	`

	var stats model.RealTimeStats
	err := r.db.GetContext(ctx, &stats, query, fiveMinAgo, fiveMinAgo, oneHourAgo)
	if err != nil {
		return nil, fmt.Errorf("failed to get real-time stats: %w", err)
	}

	stats.LastUpdated = now

	// Get recent violations
	recentQuery := `
		SELECT
			id, attempt_id, user_id, assessment_id,
			violation_type, severity, confidence_score,
			snapshot_url, created_at
		FROM violation_logs
		WHERE created_at >= $1
		ORDER BY created_at DESC
		LIMIT 10
	`

	var recentViolations []model.ViolationLog
	err = r.db.SelectContext(ctx, &recentViolations, recentQuery, fiveMinAgo)
	if err != nil {
		return nil, fmt.Errorf("failed to get recent violations: %w", err)
	}

	stats.RecentViolations = recentViolations

	return &stats, nil
}

// Helper function to get violation type name
func getViolationTypeName(violationType int) string {
	names := map[int]string{
		model.ViolationFaceNotDetected:       "Face Not Detected",
		model.ViolationMultipleFaces:         "Multiple Faces",
		model.ViolationLookingAway:           "Looking Away",
		model.ViolationMouthOpen:             "Mouth Open",
		model.ViolationHandDetected:          "Hand Detected",
		model.ViolationHeadTurnedAway:        "Head Turned Away",
		model.ViolationCopyPaste:             "Copy/Paste",
		model.ViolationSwitchingTab:          "Switching Tab",
		model.ViolationFullScreen:            "Fullscreen Exit",
		model.ViolationPhoneDetect:           "Phone Detected",
		model.ViolationVoice:                 "Voice Detected",
		model.ViolationBrowserTamper:         "Browser Tamper",
		model.ViolationVoiceChat:             "Voice Chat",
		model.ViolationFaceMismatch:          "Face Mismatch",
		model.ViolationFailLivenessChallenge: "Failed Liveness",
	}

	if name, ok := names[violationType]; ok {
		return name
	}
	return "Unknown"
}
