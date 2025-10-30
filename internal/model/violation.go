package model

import (
	"time"
)

type ViolationType string

// ViolationType constants
const (
	ViolationFaceNotDetected ViolationType = "face_not_detected"
	ViolationMultipleFaces   ViolationType = "multiple_faces"
	ViolationLookingAway     ViolationType = "looking_away"
	ViolationMouthOpen       ViolationType = "mouth_open"
	ViolationHandDetected    ViolationType = "hand_detected"
	ViolationPersonLeft      ViolationType = "person_left"
	ViolationHeadTurnedAway  ViolationType = "head_turned_away"
	ViolationEyesClosed      ViolationType = "eyes_closed"
)

type Severity string

// Severity levels
const (
	SeverityLow      Severity = "low"
	SeverityMedium   Severity = "medium"
	SeverityHigh     Severity = "high"
	SeverityCritical Severity = "critical"
)

// ViolationLog represents a single violation event
type ViolationLog struct {
	ID           uint64 `json:"id" db:"id"`
	AttemptID    uint64 `json:"attempt_id" db:"attempt_id"`
	UserID       string `json:"user_id" db:"user_id"`
	AssessmentID uint64 `json:"assessment_id" db:"assessment_id"`

	// Classification
	ViolationType   string   `json:"violation_type" db:"violation_type"`
	Severity        Severity `json:"severity" db:"severity"`
	ConfidenceScore float64  `json:"confidence_score" db:"confidence_score"`

	// MediaPipe data (will be marshaled to JSONB by repository)
	DetectionData DetectionData `json:"detection_data" db:"detection_data"`

	// Extracted metrics (denormalized for fast queries)
	FaceCount      int      `json:"face_count,omitempty" db:"face_count"`
	HandCount      int      `json:"hand_count,omitempty" db:"hand_count"`
	HeadPoseYaw    *float64 `json:"head_pose_yaw,omitempty" db:"head_pose_yaw"`
	HeadPosePitch  *float64 `json:"head_pose_pitch,omitempty" db:"head_pose_pitch"`
	HeadPoseRoll   *float64 `json:"head_pose_roll,omitempty" db:"head_pose_roll"`
	GazeDirection  *string  `json:"gaze_direction,omitempty" db:"gaze_direction"`
	MouthOpenRatio *float64 `json:"mouth_open_ratio,omitempty" db:"mouth_open_ratio"`

	// Frame metadata
	FrameNumber    int    `json:"frame_number" db:"frame_number"`
	FrameTimestamp int64  `json:"frame_timestamp" db:"frame_timestamp"`
	FPS            int    `json:"fps" db:"fps"`
	Resolution     string `json:"resolution" db:"resolution"`

	// Evidence
	SnapshotURL     string `json:"snapshot_url,omitempty" db:"snapshot_url"`
	VideoSegmentURL string `json:"video_segment_url,omitempty" db:"video_segment_url"`

	// Context (will be marshaled to JSONB by repository)
	BrowserInfo       BrowserInfo `json:"browser_info" db:"browser_info"`
	DeviceFingerprint string      `json:"device_fingerprint" db:"device_fingerprint"`

	// Timestamps
	ClientTimestamp time.Time `json:"client_timestamp" db:"client_timestamp"`
	ServerTimestamp time.Time `json:"server_timestamp" db:"server_timestamp"`
	CreatedAt       time.Time `json:"created_at" db:"created_at"`
}

// DetectionData represents the nested JSONB structure
type DetectionData struct {
	MediaPipeVersion string `json:"mediapipe_version"`
	Model            string `json:"model"` // "face_mesh", "hands", "pose"

	// Face detection
	Faces []FaceDetection `json:"faces,omitempty"`

	// Hand detection
	Hands []HandDetection `json:"hands,omitempty"`

	// Pose detection
	Pose *PoseDetection `json:"pose,omitempty"`

	// Analysis results
	Analysis AnalysisResult `json:"analysis"`
}

type FaceDetection struct {
	BoundingBox BoundingBox        `json:"bounding_box"`
	Landmarks   []Landmark3D       `json:"landmarks,omitempty"`
	Keypoints   map[string]Point2D `json:"keypoints"`
	Score       float64            `json:"score"`

	// Computed features
	HeadPose         HeadPose `json:"head_pose"`
	EyeAspectRatio   float64  `json:"eye_aspect_ratio"` // For blink detection
	MouthAspectRatio float64  `json:"mouth_aspect_ratio"`
}

type HandDetection struct {
	Handedness    string       `json:"handedness"` // "Left" or "Right"
	Score         float64      `json:"score"`
	Landmarks     []Landmark3D `json:"landmarks"`
	InFrameBounds bool         `json:"in_frame_bounds"`
}

type PoseDetection struct {
	Landmarks     []Landmark3D `json:"landmarks"`
	Visibility    []float64    `json:"visibility"`
	PoseStability float64      `json:"pose_stability"`
}

type BoundingBox struct {
	XMin   float64 `json:"x_min"`
	YMin   float64 `json:"y_min"`
	Width  float64 `json:"width"`
	Height float64 `json:"height"`
}

type Landmark3D struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
	Z float64 `json:"z"`
}

type Point2D struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

type HeadPose struct {
	Yaw   float64 `json:"yaw"`   // Left/right rotation
	Pitch float64 `json:"pitch"` // Up/down rotation
	Roll  float64 `json:"roll"`  // Tilt rotation
}

type AnalysisResult struct {
	IsCentered        bool   `json:"is_centered"`
	IsLookingAtScreen bool   `json:"is_looking_at_screen"`
	IsTalking         bool   `json:"is_talking"`
	IsDistracted      bool   `json:"is_distracted"`
	Anomaly           bool   `json:"anomaly"`
	Reason            string `json:"reason,omitempty"`
}

// BrowserInfo represents browser context
type BrowserInfo struct {
	UserAgent        string `json:"user_agent"`
	Platform         string `json:"platform"`
	Language         string `json:"language"`
	ScreenResolution string `json:"screen_resolution"`
	Timezone         string `json:"timezone"`
}
