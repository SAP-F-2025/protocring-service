package repository

import (
	"context"
	"time"

	"protocring-service/internal/model"
)

// ViolationRepositoryInterface defines the contract for violation data operations
type ViolationRepositoryInterface interface {
	// Insert inserts a single violation
	Insert(ctx context.Context, violation *model.ViolationLog) error

	// InsertBatch inserts multiple violations in a transaction
	InsertBatch(ctx context.Context, violations []*model.ViolationLog) error

	// FindByAttemptID finds all violations for an attempt
	FindByAttemptID(ctx context.Context, attemptID uint64, limit, offset int) ([]*model.ViolationLog, error)

	// FindByUserID finds violations for a user within time range
	FindByUserID(ctx context.Context, userID string, startTime, endTime time.Time, limit, offset int) ([]*model.ViolationLog, error)

	// FindByTimeRange finds violations within a time range (optional filters)
	FindByTimeRange(ctx context.Context, startTime, endTime time.Time, violationType *int, severity *int, limit, offset int) ([]*model.ViolationLog, error)

	// GetLatestByAttempt gets the most recent violation for an attempt
	GetLatestByAttempt(ctx context.Context, attemptID uint64) (*model.ViolationLog, error)

	// CountByAttempt counts violations for an attempt
	CountByAttempt(ctx context.Context, attemptID uint64) (int64, error)

	// CountByType counts violations by type for an attempt
	CountByType(ctx context.Context, attemptID uint64) (map[string]int64, error)

	// GetViolationTimeline gets violation timeline with aggregations
	GetViolationTimeline(ctx context.Context, attemptID uint64, bucketSize time.Duration) ([]ViolationTimelineBucket, error)

	// GetSeverityDistribution gets severity distribution for an attempt
	GetSeverityDistribution(ctx context.Context, attemptID uint64) (map[int]int64, error)

	// DeleteOldViolations deletes violations older than retention period
	DeleteOldViolations(ctx context.Context, olderThan time.Duration) (int64, error)

	// Dashboard Analytics Methods using Continuous Aggregates

	// GetHourlyStats gets hourly violation statistics from continuous aggregate
	GetHourlyStats(ctx context.Context, startTime, endTime time.Time) ([]*model.HourlyViolationStats, error)

	// GetDailyStats gets daily violation statistics from continuous aggregate
	GetDailyStats(ctx context.Context, startTime, endTime time.Time) ([]*model.DailyViolationStats, error)

	// GetAttemptSummary gets violation summary for a specific attempt
	GetAttemptSummary(ctx context.Context, attemptID uint64) (*model.AttemptViolationSummary, error)

	// GetAttemptSummaries gets violation summaries for multiple attempts
	GetAttemptSummaries(ctx context.Context, attemptIDs []uint64) ([]*model.AttemptViolationSummary, error)

	// GetUserPatterns gets daily violation patterns for a specific user
	GetUserPatterns(ctx context.Context, userID string, startTime, endTime time.Time) ([]*model.UserViolationPattern, error)

	// GetDashboardOverview gets high-level dashboard metrics
	GetDashboardOverview(ctx context.Context, startTime, endTime time.Time) (*model.DashboardOverview, error)

	// GetRealTimeStats gets real-time violation statistics
	GetRealTimeStats(ctx context.Context) (*model.RealTimeStats, error)
}

// ViolationTimelineBucket represents aggregated violations in a time bucket
type ViolationTimelineBucket struct {
	Bucket         time.Time `db:"bucket" json:"bucket"`
	ViolationCount int64     `db:"violation_count" json:"violation_count"`
	CriticalCount  int64     `db:"critical_count" json:"critical_count"`
	HighCount      int64     `db:"high_count" json:"high_count"`
	MediumCount    int64     `db:"medium_count" json:"medium_count"`
	LowCount       int64     `db:"low_count" json:"low_count"`
}

// ViolationAnalytics represents aggregated analytics for violations
type ViolationAnalytics struct {
	TotalCount           int64                     `json:"total_count"`
	CountByType          map[string]int64          `json:"count_by_type"`
	SeverityDistribution map[int]int64             `json:"severity_distribution"`
	Timeline             []ViolationTimelineBucket `json:"timeline"`
	LatestViolation      *model.ViolationLog       `json:"latest_violation,omitempty"`
}
