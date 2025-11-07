package model

import (
	"time"
)

// ViolationType constants
const (
	ViolationFaceNotDetected = iota
	ViolationMultipleFaces
	ViolationLookingAway
	ViolationMouthOpen
	ViolationHandDetected
	ViolationHeadTurnedAway
	ViolationCopyPaste
	ViolationSwitchingTab
	ViolationFullScreen
	ViolationPhoneDetect
	ViolationVoice
	ViolationBrowserTamper
	ViolationVoiceChat
	ViolationFaceMismatch
	ViolationFailLivenessChallenge
)

// Severity levels
const (
	SeverityLow = iota
	SeverityMedium
	SeverityHigh
	SeverityCritical
)

// ViolationLog represents a single violation event
type ViolationLog struct {
	ID           uint64 `json:"id" db:"id"`
	AttemptID    uint64 `json:"attempt_id" db:"attempt_id"`
	UserID       string `json:"user_id" db:"user_id"`
	AssessmentID uint64 `json:"assessment_id" db:"assessment_id"`

	// Classification
	ViolationType   int     `json:"violation_type" db:"violation_type"`
	Severity        int     `json:"severity" db:"severity"`
	ConfidenceScore float64 `json:"confidence_score" db:"confidence_score"`

	// Evidence
	SnapshotURL string `json:"snapshot_url,omitempty" db:"snapshot_url"`

	// Context (will be marshaled to JSONB by repository)
	BrowserInfo       BrowserInfo `json:"browser_info" db:"browser_info"`
	DeviceFingerprint string      `json:"device_fingerprint" db:"device_fingerprint"`

	// Timestamps
	CreatedAt   time.Time `json:"created_at" db:"created_at"`
	EndedAt     time.Time `json:"ended_at" db:"ended_at"`
	IsProlonged bool      `json:"is_prolonged" db:"is_prolonged"`
}

// BrowserInfo represents browser context
type BrowserInfo struct {
	UserAgent        string `json:"user_agent"`
	Platform         string `json:"platform"`
	Language         string `json:"language"`
	ScreenResolution string `json:"screen_resolution"`
	Timezone         string `json:"timezone"`
}
