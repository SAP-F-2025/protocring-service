package service

import (
	"context"
	"fmt"
	"time"

	"protocring-service/internal/config"
	"protocring-service/internal/dto"
	"protocring-service/internal/model"
	"protocring-service/internal/repository"
	"protocring-service/pkg/streams"

	"go.uber.org/zap"
)

// AsyncViolationService implements ViolationServiceInterface with async processing via Redis Streams
type AsyncViolationService struct {
	producer     *streams.Producer
	syncService  *ViolationService // Fallback to sync processing
	streamName   string
	streamMaxLen int64
	logger       *zap.Logger
}

// NewAsyncViolationService creates a new async violation service
func NewAsyncViolationService(
	producer *streams.Producer,
	syncService *ViolationService,
	cfg *config.Config,
	logger *zap.Logger,
) *AsyncViolationService {
	return &AsyncViolationService{
		producer:     producer,
		syncService:  syncService,
		streamName:   cfg.Worker.StreamName,
		streamMaxLen: cfg.Worker.StreamMaxLen,
		logger:       logger,
	}
}

// IngestViolation processes a single violation by publishing to Redis Stream
func (s *AsyncViolationService) IngestViolation(ctx context.Context, req *dto.CreateViolationRequest) (*dto.ViolationResponse, error) {
	// Validate request
	if err := s.syncService.ValidateRequest(req); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	// Convert DTO to model
	violation := s.syncService.dtoToModel(req)

	// Publish to Redis Stream
	if err := s.publishViolation(ctx, violation); err != nil {
		s.logger.Warn("Failed to publish violation to stream, falling back to sync",
			zap.Uint64("attempt_id", req.AttemptID),
			zap.Error(err),
		)

		// Fallback to synchronous processing
		return s.syncService.IngestViolation(ctx, req)
	}

	s.logger.Debug("Violation published to stream",
		zap.Uint64("attempt_id", violation.AttemptID),
		zap.Int("violation_type", violation.ViolationType),
		zap.Int("severity", violation.Severity),
	)

	// Return response immediately (violation will be processed asynchronously)
	return &dto.ViolationResponse{
		ID:              0, // Will be assigned by worker after DB insert
		AttemptID:       violation.AttemptID,
		UserID:          violation.UserID,
		AssessmentID:    violation.AssessmentID,
		ViolationType:   violation.ViolationType,
		ViolationName:   dto.GetViolationTypeName(violation.ViolationType),
		Severity:        violation.Severity,
		SeverityName:    dto.GetSeverityName(violation.Severity),
		ConfidenceScore: violation.ConfidenceScore,
		CreatedAt:       violation.CreatedAt,
		Status:          "queued",
	}, nil
}

// IngestBatch processes multiple violations by publishing to Redis Stream
func (s *AsyncViolationService) IngestBatch(ctx context.Context, req *dto.BatchViolationRequest) ([]dto.ViolationResponse, error) {
	if len(req.Violations) == 0 {
		return nil, fmt.Errorf("no violations to process")
	}

	if len(req.Violations) > 50 {
		return nil, fmt.Errorf("batch size exceeds maximum of 50")
	}

	// Validate all requests
	for i, vReq := range req.Violations {
		if err := s.syncService.ValidateRequest(&vReq); err != nil {
			return nil, fmt.Errorf("validation failed for violation %d: %w", i, err)
		}
	}

	// Convert DTOs to models
	violations := make([]*model.ViolationLog, len(req.Violations))
	for i, vReq := range req.Violations {
		violations[i] = s.syncService.dtoToModel(&vReq)
	}

	// Publish batch to Redis Stream
	if err := s.publishBatch(ctx, violations); err != nil {
		s.logger.Warn("Failed to publish batch to stream, falling back to sync",
			zap.Int("batch_size", len(violations)),
			zap.Error(err),
		)

		// Fallback to synchronous processing
		return s.syncService.IngestBatch(ctx, req)
	}

	s.logger.Debug("Batch published to stream",
		zap.Int("batch_size", len(violations)),
	)

	// Return responses immediately (violations will be processed asynchronously)
	responses := make([]dto.ViolationResponse, len(violations))
	for i, v := range violations {
		responses[i] = dto.ViolationResponse{
			ID:              0, // Will be assigned by worker after DB insert
			AttemptID:       v.AttemptID,
			UserID:          v.UserID,
			AssessmentID:    v.AssessmentID,
			ViolationType:   v.ViolationType,
			ViolationName:   dto.GetViolationTypeName(v.ViolationType),
			Severity:        v.Severity,
			SeverityName:    dto.GetSeverityName(v.Severity),
			ConfidenceScore: v.ConfidenceScore,
			CreatedAt:       v.CreatedAt,
			Status:          "queued",
		}
	}

	return responses, nil
}

// publishViolation publishes a single violation to Redis Stream
func (s *AsyncViolationService) publishViolation(ctx context.Context, violation *model.ViolationLog) error {
	opts := &streams.PublishOptions{
		MaxLen:      s.streamMaxLen,
		Approximate: true, // Use approximate trimming for better performance
	}

	_, err := s.producer.PublishJSON(ctx, s.streamName, violation, opts)
	if err != nil {
		return fmt.Errorf("failed to publish to stream: %w", err)
	}

	return nil
}

// publishBatch publishes multiple violations to Redis Stream
func (s *AsyncViolationService) publishBatch(ctx context.Context, violations []*model.ViolationLog) error {
	// Convert violations to messages
	messages := make([]map[string]interface{}, len(violations))
	for i, v := range violations {
		// Manually create message in same format as PublishJSON
		messages[i] = map[string]interface{}{
			"payload":   v, // Will be serialized by PublishJSON logic
			"timestamp": time.Now().Unix(),
		}
	}

	// Publish each violation individually (Redis Streams doesn't support true batch publish for JSON)
	// We use pipeline for efficiency
	for _, v := range violations {
		if err := s.publishViolation(ctx, v); err != nil {
			return err
		}
	}

	return nil
}

// Delegate read-only methods to sync service (these don't need async processing)

// GetViolationsByAttempt retrieves violations for an attempt with pagination
func (s *AsyncViolationService) GetViolationsByAttempt(ctx context.Context, attemptID uint64, limit, offset int) ([]*model.ViolationLog, error) {
	return s.syncService.GetViolationsByAttempt(ctx, attemptID, limit, offset)
}

// GetViolationsByUser retrieves violations for a user within time range
func (s *AsyncViolationService) GetViolationsByUser(ctx context.Context, userID string, startTime, endTime time.Time, limit, offset int) ([]*model.ViolationLog, error) {
	return s.syncService.GetViolationsByUser(ctx, userID, startTime, endTime, limit, offset)
}

// GetViolationAnalytics retrieves aggregated analytics for an attempt
func (s *AsyncViolationService) GetViolationAnalytics(ctx context.Context, attemptID uint64, bucketSize time.Duration) (*repository.ViolationAnalytics, error) {
	return s.syncService.GetViolationAnalytics(ctx, attemptID, bucketSize)
}

// GetLatestViolation retrieves the most recent violation for an attempt
func (s *AsyncViolationService) GetLatestViolation(ctx context.Context, attemptID uint64) (*model.ViolationLog, error) {
	return s.syncService.GetLatestViolation(ctx, attemptID)
}

// ValidateRequest validates a violation request
func (s *AsyncViolationService) ValidateRequest(req *dto.CreateViolationRequest) error {
	return s.syncService.ValidateRequest(req)
}

// Ensure AsyncViolationService implements ViolationServiceInterface
var _ ViolationServiceInterface = (*AsyncViolationService)(nil)
