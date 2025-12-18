package dto

import (
	"protocring-service/internal/model"
	"time"
)

// CreateViolationRequest - API request from client
type CreateViolationRequest struct {
	AttemptID    uint64 `json:"attempt_id" binding:"required"`
	UserID       string `json:"user_id" binding:"required"`
	AssessmentID uint64 `json:"assessment_id" binding:"required"`

	// Classification
	ViolationType   int     `json:"violation_type" binding:"min=0,max=23"`
	Severity        int     `json:"severity" binding:"min=0,max=3"`
	ConfidenceScore float64 `json:"confidence_score" binding:"min=0,max=1"`

	// Evidence
	SnapshotURL string `json:"snapshot_url,omitempty"`

	// Context
	BrowserInfo       model.BrowserInfo `json:"browser_info" binding:"required"`
	DeviceFingerprint string            `json:"device_fingerprint" binding:"required"`
	CreatedAt         time.Time         `json:"created_at" binding:"required"`
	EndedAt           time.Time         `json:"ended_at" binding:"required"`
	IsProlonged       bool              `json:"is_prolonged"`
}

// BatchViolationRequest - for batching
type BatchViolationRequest struct {
	Violations []CreateViolationRequest `json:"violations" binding:"required,max=50,dive"`
}

// ViolationResponse - API response
type ViolationResponse struct {
	ID              uint64    `json:"id,omitempty"`
	AttemptID       uint64    `json:"attempt_id"`
	UserID          string    `json:"user_id"`
	AssessmentID    uint64    `json:"assessment_id"`
	ViolationType   int       `json:"violation_type"`
	ViolationName   string    `json:"violation_name"`
	Severity        int       `json:"severity"`
	SeverityName    string    `json:"severity_name"`
	ConfidenceScore float64   `json:"confidence_score"`
	CreatedAt       time.Time `json:"created_at"`
	Status          string    `json:"status"`               // "queued", "processing", "processed", "failed"
	MessageID       string    `json:"message_id,omitempty"` // Redis Stream message ID
}

// Helper functions to convert violation type and severity to names
func GetViolationTypeName(vType int) string {
	names := []string{
		"face_not_detected",
		"multiple_faces",
		"looking_away",
		"mouth_open",
		"hand_detected",
		"head_turned_away",
		"copy_paste",
		"switching_tab",
		"full_screen",
		"phone_detect",
		"voice",
		"browser_tamper",
		"voice_chat",
		"face_mismatch",
		"fail_liveness_challenge",
	}
	if vType >= 0 && vType < len(names) {
		return names[vType]
	}
	return "unknown"
}

func GetSeverityName(severity int) string {
	names := []string{"low", "medium", "high", "critical"}
	if severity >= 0 && severity < len(names) {
		return names[severity]
	}
	return "unknown"
}
