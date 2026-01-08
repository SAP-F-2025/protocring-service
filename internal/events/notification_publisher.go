package events

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"protocring-service/internal/config"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// ProctoringViolationEvent matches the expected format in notification-service
// Must include all fields from BaseEvent + ProctoringViolationEvent
type ProctoringViolationEvent struct {
	// From BaseEvent (required by notification-service)
	EventID   string    `json:"eventId"`
	Timestamp time.Time `json:"timestamp"`

	// ProctoringViolationEvent fields
	UserID        string   `json:"userId"`
	Username      string   `json:"username,omitempty"`
	SessionID     string   `json:"sessionId"` // attemptId as string
	ViolationType string   `json:"violationType"`
	Severity      string   `json:"severity"` // "LOW", "MEDIUM", "HIGH", "CRITICAL"
	ProctorIDs    []string `json:"proctorIds"`
}

// NotificationPublisher publishes violation events to Redis Stream for notification-service
type NotificationPublisher struct {
	client     *redis.Client
	logger     *zap.Logger
	streamName string
}

// NewNotificationPublisher creates a new notification publisher
func NewNotificationPublisher(
	cfg *config.NotificationConfig,
	redisClient *redis.Client,
	logger *zap.Logger,
) *NotificationPublisher {
	return &NotificationPublisher{
		client:     redisClient,
		logger:     logger,
		streamName: cfg.StreamName,
	}
}

// PublishViolation publishes a proctoring violation event to the notification stream
func (p *NotificationPublisher) PublishViolation(ctx context.Context, event *ProctoringViolationEvent) error {
	if p.client == nil {
		p.logger.Warn("Notification Redis client is nil, skipping publish")
		return nil
	}

	// Auto-generate eventId if empty
	if event.EventID == "" {
		event.EventID = uuid.NewString()
	}

	// Set timestamp if empty
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	// Marshal event to JSON
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Prepare fields for XADD
	// notification-service reads these directly from the stream
	fields := map[string]interface{}{
		"eventId":       event.EventID,
		"timestamp":     event.Timestamp.Format(time.RFC3339),
		"userId":        event.UserID,
		"username":      event.Username,
		"sessionId":     event.SessionID,
		"violationType": event.ViolationType,
		"severity":      event.Severity,
		"data":          string(eventJSON), // Full event as JSON for flexibility
	}

	// Handle proctorIds array - flatten for Redis
	for i, proctorID := range event.ProctorIDs {
		fields[fmt.Sprintf("proctorIds.[%d]", i)] = proctorID
	}

	// Use XADD to publish to the stream
	result, err := p.client.XAdd(ctx, &redis.XAddArgs{
		Stream: p.streamName,
		ID:     "*", // Auto-generate message ID
		Values: fields,
	}).Result()

	if err != nil {
		p.logger.Error("Failed to publish violation notification",
			zap.String("event_id", event.EventID),
			zap.String("session_id", event.SessionID),
			zap.String("violation_type", event.ViolationType),
			zap.String("stream", p.streamName),
			zap.Error(err))
		return fmt.Errorf("failed to publish to stream: %w", err)
	}

	p.logger.Info("Published violation notification",
		zap.String("message_id", result),
		zap.String("event_id", event.EventID),
		zap.String("session_id", event.SessionID),
		zap.String("violation_type", event.ViolationType),
		zap.String("severity", event.Severity),
		zap.Int("proctor_count", len(event.ProctorIDs)))

	return nil
}

// GetStreamName returns the configured stream name
func (p *NotificationPublisher) GetStreamName() string {
	return p.streamName
}

// Close is a no-op as Redis client lifecycle is managed externally
func (p *NotificationPublisher) Close() error {
	return nil
}
