package dto

import "time"

// PresignedURLRequest is the request for generating a presigned upload URL
type PresignedURLRequest struct {
	AttemptID     uint64 `form:"attempt_id" binding:"required"`
	ViolationType int    `form:"violation_type" binding:"min=0,max=23"`
	ContentType   string `form:"content_type"` // Optional, defaults to "image/jpeg"
}

// PresignedURLResponse contains the presigned URL and metadata
type PresignedURLResponse struct {
	UploadURL   string    `json:"upload_url"`   // Presigned PUT URL
	ObjectKey   string    `json:"object_key"`   // e.g., "violations/123/1703123456_face_not_detected.jpg"
	PublicURL   string    `json:"public_url"`   // CDN/public URL for access after upload
	ExpiresAt   time.Time `json:"expires_at"`   // When the presigned URL expires
	ContentType string    `json:"content_type"` // Expected content type
}

// GenerateObjectKey creates an object key for a violation snapshot
// Format: violations/{attempt_id}/{timestamp}_{violation_type}.{ext}
func GenerateObjectKey(attemptID uint64, violationType int, ext string) string {
	timestamp := time.Now().UnixMilli()
	typeName := GetViolationTypeName(violationType)
	return formatObjectKey(attemptID, timestamp, typeName, ext)
}

func formatObjectKey(attemptID uint64, timestamp int64, typeName, ext string) string {
	return "violations/" + uintToString(attemptID) + "/" + intToString(timestamp) + "_" + typeName + "." + ext
}

// Helper functions to avoid fmt import for simple conversions
func uintToString(n uint64) string {
	if n == 0 {
		return "0"
	}
	var digits []byte
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}

func intToString(n int64) string {
	if n == 0 {
		return "0"
	}
	negative := n < 0
	if negative {
		n = -n
	}
	var digits []byte
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	if negative {
		digits = append([]byte{'-'}, digits...)
	}
	return string(digits)
}
