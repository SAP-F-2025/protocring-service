package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"protocring-service/internal/config"
	"protocring-service/internal/dto"
	"protocring-service/internal/model"
	"protocring-service/internal/repository"
	"protocring-service/internal/storage"
	"protocring-service/pkg/streams"

	"go.uber.org/zap"
)

// ViolationServiceInterface defines the contract for violation business logic
type ViolationServiceInterface interface {
	// IngestViolation processes a single violation from MediaPipe
	IngestViolation(ctx context.Context, req *dto.CreateViolationRequest) (*dto.ViolationResponse, error)

	// IngestBatch processes multiple violations in a batch
	IngestBatch(ctx context.Context, req *dto.BatchViolationRequest) ([]dto.ViolationResponse, error)

	// GetViolationsByAttempt retrieves violations for an attempt with pagination
	GetViolationsByAttempt(ctx context.Context, attemptID uint64, limit, offset int) ([]*model.ViolationLog, error)

	// GetViolationsByUser retrieves violations for a user within time range
	GetViolationsByUser(ctx context.Context, userID string, startTime, endTime time.Time, limit, offset int) ([]*model.ViolationLog, error)

	// GetViolationAnalytics retrieves aggregated analytics for an attempt
	GetViolationAnalytics(ctx context.Context, attemptID uint64, bucketSize time.Duration) (*repository.ViolationAnalytics, error)

	// GetLatestViolation retrieves the most recent violation for an attempt
	GetLatestViolation(ctx context.Context, attemptID uint64) (*model.ViolationLog, error)

	// GenerateUploadURL generates a presigned URL for uploading violation snapshot
	GenerateUploadURL(ctx context.Context, req *dto.PresignedURLRequest) (*dto.PresignedURLResponse, error)

	// ValidateRequest validates a violation request
	ValidateRequest(req *dto.CreateViolationRequest) error
}

// ViolationService implements ViolationServiceInterface
type ViolationService struct {
	repo     repository.ViolationRepositoryInterface
	producer *streams.Producer
	storage  storage.StorageService
	config   *config.Config
	logger   *zap.Logger
}

// NewViolationService creates a new violation service
func NewViolationService(
	repo repository.ViolationRepositoryInterface,
	producer *streams.Producer,
	storageService storage.StorageService,
	config *config.Config,
	logger *zap.Logger,
) *ViolationService {
	return &ViolationService{
		repo:     repo,
		producer: producer,
		storage:  storageService,
		config:   config,
		logger:   logger,
	}
}

// IngestViolation processes a single violation from MediaPipe
func (s *ViolationService) IngestViolation(ctx context.Context, req *dto.CreateViolationRequest) (*dto.ViolationResponse, error) {
	// Validate request
	if err := s.ValidateRequest(req); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	// Publish to Redis Stream with MAXLEN to cap stream size
	opts := &streams.PublishOptions{
		MaxLen:      s.config.Worker.StreamMaxLen,
		Approximate: true,
	}
	messageID, err := s.producer.PublishJSON(ctx, s.config.Worker.StreamName, req, opts)
	if err != nil {
		s.logger.Error("Failed to publish violation to stream",
			zap.Uint64("attempt_id", req.AttemptID),
			zap.Int("violation_type", req.ViolationType),
			zap.Error(err),
		)
		return nil, fmt.Errorf("failed to publish violation: %w", err)
	}

	s.logger.Info("Violation queued successfully",
		zap.String("message_id", messageID),
		zap.Uint64("attempt_id", req.AttemptID),
		zap.Int("violation_type", req.ViolationType),
		zap.Int("severity", req.Severity),
	)

	return &dto.ViolationResponse{
		AttemptID:       req.AttemptID,
		UserID:          req.UserID,
		AssessmentID:    req.AssessmentID,
		ViolationType:   req.ViolationType,
		ViolationName:   dto.GetViolationTypeName(req.ViolationType),
		Severity:        req.Severity,
		SeverityName:    dto.GetSeverityName(req.Severity),
		ConfidenceScore: req.ConfidenceScore,
		CreatedAt:       req.CreatedAt,
		Status:          "queued",
		MessageID:       messageID,
	}, nil
}

// IngestBatch processes multiple violations in a batch
func (s *ViolationService) IngestBatch(ctx context.Context, req *dto.BatchViolationRequest) ([]dto.ViolationResponse, error) {
	if len(req.Violations) == 0 {
		return nil, fmt.Errorf("no violations to process")
	}

	//if len(req.Violations) > 50 {
	//	return nil, fmt.Errorf("batch size exceeds maximum of 50")
	//}

	// Validate all requests
	for i, vReq := range req.Violations {
		if err := s.ValidateRequest(&vReq); err != nil {
			return nil, fmt.Errorf("validation failed for violation %d: %w", i, err)
		}
	}

	// Publish batch to Redis Stream
	messages := make([]map[string]interface{}, len(req.Violations))
	for i := range req.Violations {
		payload, err := s.marshalJSON(&req.Violations[i])
		if err != nil {
			s.logger.Error("Failed to marshal violation",
				zap.Int("index", i),
				zap.Error(err),
			)
			return nil, fmt.Errorf("failed to marshal violation %d: %w", i, err)
		}
		messages[i] = map[string]interface{}{
			"payload":   payload,
			"timestamp": time.Now().Unix(),
		}
	}

	opts := &streams.PublishOptions{
		MaxLen:      s.config.Worker.StreamMaxLen,
		Approximate: true,
	}
	messageIDs, err := s.producer.PublishBatch(ctx, s.config.Worker.StreamName, messages, opts)
	if err != nil {
		s.logger.Error("Failed to publish batch to stream",
			zap.Int("batch_size", len(req.Violations)),
			zap.Error(err),
		)
		return nil, fmt.Errorf("failed to publish batch: %w", err)
	}

	s.logger.Info("Batch queued successfully",
		zap.Int("batch_size", len(req.Violations)),
	)

	// Convert to responses
	responses := make([]dto.ViolationResponse, len(req.Violations))
	for i, vReq := range req.Violations {
		responses[i] = dto.ViolationResponse{
			AttemptID:       vReq.AttemptID,
			UserID:          vReq.UserID,
			AssessmentID:    vReq.AssessmentID,
			ViolationType:   vReq.ViolationType,
			ViolationName:   dto.GetViolationTypeName(vReq.ViolationType),
			Severity:        vReq.Severity,
			SeverityName:    dto.GetSeverityName(vReq.Severity),
			ConfidenceScore: vReq.ConfidenceScore,
			CreatedAt:       vReq.CreatedAt,
			Status:          "queued",
			MessageID:       messageIDs[i],
		}
	}

	return responses, nil
}

// GetViolationsByAttempt retrieves violations for an attempt with pagination
func (s *ViolationService) GetViolationsByAttempt(ctx context.Context, attemptID uint64, limit, offset int) ([]*model.ViolationLog, error) {
	if limit <= 0 || limit > 100 {
		limit = 20 // Default limit
	}

	violations, err := s.repo.FindByAttemptID(ctx, attemptID, limit, offset)
	if err != nil {
		s.logger.Error("Failed to find violations by attempt",
			zap.Uint64("attempt_id", attemptID),
			zap.Error(err),
		)
		return nil, fmt.Errorf("failed to find violations: %w", err)
	}

	// Transform snapshot URLs to full CDN URLs
	s.transformSnapshotURLs(violations)

	return violations, nil
}

// GetViolationsByUser retrieves violations for a user within time range
func (s *ViolationService) GetViolationsByUser(ctx context.Context, userID string, startTime, endTime time.Time, limit, offset int) ([]*model.ViolationLog, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}

	violations, err := s.repo.FindByUserID(ctx, userID, startTime, endTime, limit, offset)
	if err != nil {
		s.logger.Error("Failed to find violations by user",
			zap.String("user_id", userID),
			zap.Error(err),
		)
		return nil, fmt.Errorf("failed to find violations: %w", err)
	}

	// Transform snapshot URLs to full CDN URLs
	s.transformSnapshotURLs(violations)

	return violations, nil
}

// GetViolationAnalytics retrieves aggregated analytics for an attempt
func (s *ViolationService) GetViolationAnalytics(ctx context.Context, attemptID uint64, bucketSize time.Duration) (*repository.ViolationAnalytics, error) {
	if bucketSize == 0 {
		bucketSize = 5 * time.Minute // Default bucket size
	}

	// Get total count
	totalCount, err := s.repo.CountByAttempt(ctx, attemptID)
	if err != nil {
		return nil, fmt.Errorf("failed to count violations: %w", err)
	}

	// Get count by type
	countByType, err := s.repo.CountByType(ctx, attemptID)
	if err != nil {
		return nil, fmt.Errorf("failed to count by type: %w", err)
	}

	// Get severity distribution
	severityDist, err := s.repo.GetSeverityDistribution(ctx, attemptID)
	if err != nil {
		return nil, fmt.Errorf("failed to get severity distribution: %w", err)
	}

	// Get timeline
	timeline, err := s.repo.GetViolationTimeline(ctx, attemptID, bucketSize)
	if err != nil {
		return nil, fmt.Errorf("failed to get timeline: %w", err)
	}

	// Get latest violation
	latest, err := s.repo.GetLatestByAttempt(ctx, attemptID)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest violation: %w", err)
	}

	return &repository.ViolationAnalytics{
		TotalCount:           totalCount,
		CountByType:          countByType,
		SeverityDistribution: severityDist,
		Timeline:             timeline,
		LatestViolation:      latest,
	}, nil
}

// GetLatestViolation retrieves the most recent violation for an attempt
func (s *ViolationService) GetLatestViolation(ctx context.Context, attemptID uint64) (*model.ViolationLog, error) {
	violation, err := s.repo.GetLatestByAttempt(ctx, attemptID)
	if err != nil {
		s.logger.Error("Failed to get latest violation",
			zap.Uint64("attempt_id", attemptID),
			zap.Error(err),
		)
		return nil, fmt.Errorf("failed to get latest violation: %w", err)
	}

	// Transform snapshot URL to full CDN URL
	if violation != nil {
		s.transformSnapshotURL(violation)
	}

	return violation, nil
}

// GenerateUploadURL generates a presigned URL for uploading violation snapshot
func (s *ViolationService) GenerateUploadURL(ctx context.Context, req *dto.PresignedURLRequest) (*dto.PresignedURLResponse, error) {
	// Check if storage is configured
	if s.storage == nil {
		return nil, fmt.Errorf("storage service not configured")
	}

	// Default content type
	contentType := req.ContentType
	if contentType == "" {
		contentType = "image/jpeg"
	}

	// Determine file extension from content type
	ext := "jpg"
	switch contentType {
	case "image/png":
		ext = "png"
	case "image/webp":
		ext = "webp"
	case "image/gif":
		ext = "gif"
	}

	// Generate object key
	objectKey := dto.GenerateObjectKey(req.AttemptID, req.ViolationType, ext)

	// Generate presigned PUT URL
	uploadURL, err := s.storage.GeneratePresignedPutURL(ctx, objectKey, contentType)
	if err != nil {
		s.logger.Error("Failed to generate presigned upload URL",
			zap.Uint64("attempt_id", req.AttemptID),
			zap.Int("violation_type", req.ViolationType),
			zap.Error(err),
		)
		return nil, fmt.Errorf("failed to generate upload URL: %w", err)
	}

	// Calculate expiry time
	expiry := s.config.Storage.PresignExpiry
	if expiry == 0 {
		expiry = 5 * time.Minute
	}

	s.logger.Info("Generated presigned upload URL",
		zap.Uint64("attempt_id", req.AttemptID),
		zap.String("object_key", objectKey),
		zap.Duration("expiry", expiry),
	)

	return &dto.PresignedURLResponse{
		UploadURL:   uploadURL,
		ObjectKey:   objectKey,
		PublicURL:   s.storage.GetPublicURL(objectKey),
		ExpiresAt:   time.Now().Add(expiry),
		ContentType: contentType,
	}, nil
}

// ValidateRequest validates a violation request
func (s *ViolationService) ValidateRequest(req *dto.CreateViolationRequest) error {
	if req.AttemptID == 0 {
		return fmt.Errorf("attempt_id is required")
	}

	if req.UserID == "" {
		return fmt.Errorf("user_id is required")
	}

	if req.AssessmentID == 0 {
		return fmt.Errorf("assessment_id is required")
	}

	// Validate violation type (0-23 based on constants)
	if req.ViolationType < model.ViolationFaceNotDetected || req.ViolationType > model.ViolationFailLivenessChallenge {
		return fmt.Errorf("invalid violation_type: %d", req.ViolationType)
	}

	// Validate severity
	if req.Severity < model.SeverityLow || req.Severity > model.SeverityCritical {
		return fmt.Errorf("invalid severity: %d", req.Severity)
	}

	// Validate confidence score
	if req.ConfidenceScore < 0 || req.ConfidenceScore > 1 {
		return fmt.Errorf("confidence_score must be between 0 and 1")
	}

	// Validate snapshot URL format if provided
	if req.SnapshotURL != "" {
		if !strings.HasPrefix(req.SnapshotURL, "violations/") {
			return fmt.Errorf("invalid snapshot_url format: must start with 'violations/'")
		}
	}

	if req.IsProlonged && req.EndedAt.IsZero() {
		return fmt.Errorf("ended_at is required for prolonged violations")
	}

	return nil
}

// dtoToModel converts DTO to model
func (s *ViolationService) dtoToModel(req *dto.CreateViolationRequest) *model.ViolationLog {
	return &model.ViolationLog{
		AttemptID:         req.AttemptID,
		UserID:            req.UserID,
		AssessmentID:      req.AssessmentID,
		ViolationType:     req.ViolationType,
		Severity:          req.Severity,
		ConfidenceScore:   req.ConfidenceScore,
		SnapshotURL:       req.SnapshotURL,
		BrowserInfo:       req.BrowserInfo,
		DeviceFingerprint: req.DeviceFingerprint,
		CreatedAt:         req.CreatedAt,
		EndedAt:           req.EndedAt,
		IsProlonged:       req.IsProlonged,
	}
}

// modelToResponse converts model to response DTO
func (s *ViolationService) modelToResponse(v *model.ViolationLog) *dto.ViolationResponse {
	return &dto.ViolationResponse{
		ID:              v.ID,
		AttemptID:       v.AttemptID,
		UserID:          v.UserID,
		AssessmentID:    v.AssessmentID,
		ViolationType:   v.ViolationType,
		ViolationName:   dto.GetViolationTypeName(v.ViolationType),
		Severity:        v.Severity,
		SeverityName:    dto.GetSeverityName(v.Severity),
		ConfidenceScore: v.ConfidenceScore,
		CreatedAt:       v.CreatedAt,
		Status:          "processed",
	}
}

// transformSnapshotURL converts an object key to a full CDN URL
func (s *ViolationService) transformSnapshotURL(v *model.ViolationLog) {
	if v.SnapshotURL == "" {
		return
	}

	// If storage is configured, get the public URL
	if s.storage != nil {
		v.SnapshotURL = s.storage.GetPublicURL(v.SnapshotURL)
	}
}

// transformSnapshotURLs converts object keys to full CDN URLs for a slice of violations
func (s *ViolationService) transformSnapshotURLs(violations []*model.ViolationLog) {
	for _, v := range violations {
		s.transformSnapshotURL(v)
	}
}

// marshalJSON marshals to JSON and returns error instead of panicking
func (s *ViolationService) marshalJSON(v interface{}) (string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("failed to marshal JSON: %w", err)
	}
	return string(data), nil
}

// Ensure ViolationService implements ViolationServiceInterface
var _ ViolationServiceInterface = (*ViolationService)(nil)
