package dto

import (
	"protocring-service/internal/model"
	"time"
)

// CreateViolationRequest - API request from client
type CreateViolationRequest struct {
	AttemptID       uint64  `json:"attempt_id" binding:"required"`
	ViolationType   string  `json:"violation_type" binding:"required,oneof=face_not_detected multiple_faces looking_away mouth_open hand_detected person_left head_turned_away eyes_closed"`
	Severity        string  `json:"severity" binding:"required,oneof=low medium high critical"`
	ConfidenceScore float64 `json:"confidence_score" binding:"min=0,max=1"`

	// MediaPipe detection (will be stored in JSONB)
	DetectionData model.DetectionData `json:"detection_data" binding:"required"`

	// Frame info
	FrameMetadata FrameMetadata `json:"frame_metadata" binding:"required"`

	// Optional evidence
	SnapshotBase64 string `json:"snapshot_base64,omitempty"`

	// Context
	BrowserInfo     model.BrowserInfo `json:"browser_info" binding:"required"`
	ClientTimestamp time.Time         `json:"client_timestamp" binding:"required"`
}

type FrameMetadata struct {
	FrameNumber int    `json:"frame_number"`
	Timestamp   int64  `json:"timestamp"`
	FPS         int    `json:"fps"`
	Resolution  string `json:"resolution"`
}

// BatchViolationRequest - for batching
type BatchViolationRequest struct {
	Violations []CreateViolationRequest `json:"violations" binding:"required,max=50,dive"`
}

// ViolationResponse - API response
type ViolationResponse struct {
	ID              uint64    `json:"id"`
	AttemptID       uint64    `json:"attempt_id"`
	ViolationType   string    `json:"violation_type"`
	Severity        string    `json:"severity"`
	ServerTimestamp time.Time `json:"server_timestamp"`
	Status          string    `json:"status"`
}
