package model

import (
	"time"

	"github.com/lib/pq"
)

// HourlyViolationStats represents hourly aggregated violation statistics
type HourlyViolationStats struct {
	Bucket          time.Time `db:"bucket" json:"bucket"`
	TotalViolations int64     `db:"total_violations" json:"total_violations"`
	UniqueAttempts  int64     `db:"unique_attempts" json:"unique_attempts"`
	UniqueUsers     int64     `db:"unique_users" json:"unique_users"`

	// Severity breakdown
	CriticalCount int64 `db:"critical_count" json:"critical_count"`
	HighCount     int64 `db:"high_count" json:"high_count"`
	MediumCount   int64 `db:"medium_count" json:"medium_count"`
	LowCount      int64 `db:"low_count" json:"low_count"`

	// Violation type breakdown
	FaceNotDetectedCount int64 `db:"face_not_detected_count" json:"face_not_detected_count"`
	MultipleFacesCount   int64 `db:"multiple_faces_count" json:"multiple_faces_count"`
	LookingAwayCount     int64 `db:"looking_away_count" json:"looking_away_count"`
	HandDetectedCount    int64 `db:"hand_detected_count" json:"hand_detected_count"`
	SwitchingTabCount    int64 `db:"switching_tab_count" json:"switching_tab_count"`
	FullscreenCount      int64 `db:"fullscreen_count" json:"fullscreen_count"`

	// Prolonged violations
	ProlongedCount int64   `db:"prolonged_count" json:"prolonged_count"`
	AvgConfidence  float64 `db:"avg_confidence" json:"avg_confidence"`
}

// DailyViolationStats represents daily aggregated violation statistics
type DailyViolationStats struct {
	Bucket            time.Time `db:"bucket" json:"bucket"`
	TotalViolations   int64     `db:"total_violations" json:"total_violations"`
	UniqueAttempts    int64     `db:"unique_attempts" json:"unique_attempts"`
	UniqueUsers       int64     `db:"unique_users" json:"unique_users"`
	UniqueAssessments int64     `db:"unique_assessments" json:"unique_assessments"`

	// Severity breakdown
	CriticalCount int64 `db:"critical_count" json:"critical_count"`
	HighCount     int64 `db:"high_count" json:"high_count"`
	MediumCount   int64 `db:"medium_count" json:"medium_count"`
	LowCount      int64 `db:"low_count" json:"low_count"`

	// Violation type stats
	FaceNotDetectedCount int64 `db:"face_not_detected_count" json:"face_not_detected_count"`
	MultipleFacesCount   int64 `db:"multiple_faces_count" json:"multiple_faces_count"`
	LookingAwayCount     int64 `db:"looking_away_count" json:"looking_away_count"`
	MouthOpenCount       int64 `db:"mouth_open_count" json:"mouth_open_count"`
	HandDetectedCount    int64 `db:"hand_detected_count" json:"hand_detected_count"`
	CopyPasteCount       int64 `db:"copy_paste_count" json:"copy_paste_count"`
	SwitchingTabCount    int64 `db:"switching_tab_count" json:"switching_tab_count"`
	FullscreenCount      int64 `db:"fullscreen_count" json:"fullscreen_count"`
	PhoneDetectCount     int64 `db:"phone_detect_count" json:"phone_detect_count"`

	// Prolonged violations
	ProlongedCount     int64   `db:"prolonged_count" json:"prolonged_count"`
	AvgDurationSeconds float64 `db:"avg_duration_seconds" json:"avg_duration_seconds"`
	AvgConfidence      float64 `db:"avg_confidence" json:"avg_confidence"`
}

// AttemptViolationSummary represents aggregated violations for a specific attempt
type AttemptViolationSummary struct {
	AttemptID    uint64 `db:"attempt_id" json:"attempt_id"`
	UserID       string `db:"user_id" json:"user_id"`
	AssessmentID uint64 `db:"assessment_id" json:"assessment_id"`

	// Time range
	FirstViolationAt time.Time `db:"first_violation_at" json:"first_violation_at"`
	LastViolationAt  time.Time `db:"last_violation_at" json:"last_violation_at"`
	DurationSeconds  float64   `db:"duration_seconds" json:"duration_seconds"`

	// Counts
	TotalViolations          int64 `db:"total_violations" json:"total_violations"`
	UniqueViolationTypes     int64 `db:"unique_violation_types" json:"unique_violation_types"`
	ProlongedViolationsCount int64 `db:"prolonged_violations_count" json:"prolonged_violations_count"`

	// Severity breakdown
	CriticalCount int64 `db:"critical_count" json:"critical_count"`
	HighCount     int64 `db:"high_count" json:"high_count"`
	MediumCount   int64 `db:"medium_count" json:"medium_count"`
	LowCount      int64 `db:"low_count" json:"low_count"`

	// Most severe violation
	MaxSeverityLevel int `db:"max_severity_level" json:"max_severity_level"`

	// Violation types array
	ViolationTypes pq.Int64Array `db:"violation_types" json:"violation_types"`

	// Confidence metrics
	AvgConfidence float64 `db:"avg_confidence" json:"avg_confidence"`
	MaxConfidence float64 `db:"max_confidence" json:"max_confidence"`
	MinConfidence float64 `db:"min_confidence" json:"min_confidence"`
}

// UserViolationPattern represents daily violation patterns for a user
type UserViolationPattern struct {
	Bucket time.Time `db:"bucket" json:"bucket"`
	UserID string    `db:"user_id" json:"user_id"`

	// Counts
	TotalViolations  int64 `db:"total_violations" json:"total_violations"`
	AttemptsCount    int64 `db:"attempts_count" json:"attempts_count"`
	AssessmentsCount int64 `db:"assessments_count" json:"assessments_count"`

	// Severity distribution
	CriticalCount int64 `db:"critical_count" json:"critical_count"`
	HighCount     int64 `db:"high_count" json:"high_count"`

	// Prolonged violations
	ProlongedCount int64 `db:"prolonged_count" json:"prolonged_count"`

	// Most common violation type
	MostCommonViolation  int   `db:"most_common_violation" json:"most_common_violation"`
	UniqueViolationTypes int64 `db:"unique_violation_types" json:"unique_violation_types"`

	// Behavioral flags
	HasMultipleFaces  bool `db:"has_multiple_faces" json:"has_multiple_faces"`
	HasHandDetected   bool `db:"has_hand_detected" json:"has_hand_detected"`
	HasSwitchingTab   bool `db:"has_switching_tab" json:"has_switching_tab"`
	HasFullscreenExit bool `db:"has_fullscreen_exit" json:"has_fullscreen_exit"`

	// Metrics
	AvgConfidence float64 `db:"avg_confidence" json:"avg_confidence"`
}

// DashboardOverview represents high-level dashboard metrics
type DashboardOverview struct {
	// Current period stats
	TotalViolations  int64 `db:"total_violations" json:"total_violations"`
	TotalAttempts    int64 `db:"total_attempts" json:"total_attempts"`
	TotalUsers       int64 `db:"total_users" json:"total_users"`
	TotalAssessments int64 `db:"total_assessments" json:"total_assessments"`

	// Severity breakdown
	CriticalCount int64 `db:"critical_count" json:"critical_count"`
	HighCount     int64 `db:"high_count" json:"high_count"`
	MediumCount   int64 `db:"medium_count" json:"medium_count"`
	LowCount      int64 `db:"low_count" json:"low_count"`

	// Change from previous period (percentage)
	ViolationsChange float64 `json:"violations_change"`
	AttemptsChange   float64 `json:"attempts_change"`
	UsersChange      float64 `json:"users_change"`

	// Top violation types
	TopViolationTypes []ViolationTypeCount `json:"top_violation_types"`
}

// ViolationTypeCount represents count for a specific violation type
type ViolationTypeCount struct {
	ViolationType int     `json:"violation_type"`
	TypeName      string  `json:"type_name"`
	Count         int64   `json:"count"`
	Percentage    float64 `json:"percentage"`
}

// RealTimeStats represents real-time violation statistics
type RealTimeStats struct {
	LastUpdated        time.Time      `json:"last_updated"`
	ActiveAttempts     int64          `db:"active_attempts" json:"active_attempts"`
	ViolationsLast5Min int64          `db:"violations_last_5min" json:"violations_last_5min"`
	ViolationsLastHour int64          `db:"violations_last_hour" json:"violations_last_hour"`
	CriticalViolations int64          `db:"critical_violations" json:"critical_violations"`
	RecentViolations   []ViolationLog `json:"recent_violations"`
}
