package service

import (
	"context"
	"fmt"
	"time"

	"protocring-service/internal/dto"
	"protocring-service/internal/model"
	"protocring-service/internal/repository"

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

	// ValidateRequest validates a violation request
	ValidateRequest(req *dto.CreateViolationRequest) error
}

// ViolationService implements ViolationServiceInterface
type ViolationService struct {
	repo   repository.ViolationRepositoryInterface
	logger *zap.Logger
}

// NewViolationService creates a new violation service
func NewViolationService(repo repository.ViolationRepositoryInterface, logger *zap.Logger) *ViolationService {
	return &ViolationService{
		repo:   repo,
		logger: logger,
	}
}

// IngestViolation processes a single violation from MediaPipe
func (s *ViolationService) IngestViolation(ctx context.Context, req *dto.CreateViolationRequest) (*dto.ViolationResponse, error) {
	// Validate request
	if err := s.ValidateRequest(req); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	// Convert DTO to model
	violation := s.dtoToModel(req)

	// Insert to database
	if err := s.repo.Insert(ctx, violation); err != nil {
		s.logger.Error("Failed to insert violation",
			zap.Uint64("attempt_id", req.AttemptID),
			zap.String("violation_type", req.ViolationType),
			zap.Error(err),
		)
		return nil, fmt.Errorf("failed to insert violation: %w", err)
	}

	s.logger.Info("Violation ingested successfully",
		zap.Uint64("violation_id", violation.ID),
		zap.Uint64("attempt_id", violation.AttemptID),
		zap.String("violation_type", violation.ViolationType),
		zap.String("severity", string(violation.Severity)),
	)

	return s.modelToResponse(violation), nil
}

// IngestBatch processes multiple violations in a batch
func (s *ViolationService) IngestBatch(ctx context.Context, req *dto.BatchViolationRequest) ([]dto.ViolationResponse, error) {
	if len(req.Violations) == 0 {
		return nil, fmt.Errorf("no violations to process")
	}

	if len(req.Violations) > 50 {
		return nil, fmt.Errorf("batch size exceeds maximum of 50")
	}

	// Validate all requests
	for i, vReq := range req.Violations {
		if err := s.ValidateRequest(&vReq); err != nil {
			return nil, fmt.Errorf("validation failed for violation %d: %w", i, err)
		}
	}

	// Convert DTOs to models
	violations := make([]*model.ViolationLog, len(req.Violations))
	for i, vReq := range req.Violations {
		violations[i] = s.dtoToModel(&vReq)
	}

	// Insert batch
	if err := s.repo.InsertBatch(ctx, violations); err != nil {
		s.logger.Error("Failed to insert batch",
			zap.Int("batch_size", len(violations)),
			zap.Error(err),
		)
		return nil, fmt.Errorf("failed to insert batch: %w", err)
	}

	s.logger.Info("Batch ingested successfully",
		zap.Int("batch_size", len(violations)),
	)

	// Convert to responses
	responses := make([]dto.ViolationResponse, len(violations))
	for i, v := range violations {
		responses[i] = *s.modelToResponse(v)
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

	return violation, nil
}

// ValidateRequest validates a violation request
func (s *ViolationService) ValidateRequest(req *dto.CreateViolationRequest) error {
	if req.AttemptID == 0 {
		return fmt.Errorf("attempt_id is required")
	}

	if req.ViolationType == "" {
		return fmt.Errorf("violation_type is required")
	}

	if req.Severity == "" {
		return fmt.Errorf("severity is required")
	}

	// Validate severity
	severity := model.Severity(req.Severity)
	if severity != model.SeverityLow && severity != model.SeverityMedium &&
		severity != model.SeverityHigh && severity != model.SeverityCritical {
		return fmt.Errorf("invalid severity: %s", req.Severity)
	}

	// Validate confidence score
	if req.ConfidenceScore < 0 || req.ConfidenceScore > 1 {
		return fmt.Errorf("confidence_score must be between 0 and 1")
	}

	return nil
}

// dtoToModel converts DTO to model
func (s *ViolationService) dtoToModel(req *dto.CreateViolationRequest) *model.ViolationLog {
	violation := &model.ViolationLog{
		AttemptID:       req.AttemptID,
		ViolationType:   req.ViolationType,
		Severity:        model.Severity(req.Severity),
		ConfidenceScore: req.ConfidenceScore,
		DetectionData:   req.DetectionData,
		BrowserInfo:     req.BrowserInfo,
		ClientTimestamp: req.ClientTimestamp,
	}

	// Extract frame metadata
	violation.FrameNumber = req.FrameMetadata.FrameNumber
	violation.FrameTimestamp = req.FrameMetadata.Timestamp
	violation.FPS = req.FrameMetadata.FPS
	violation.Resolution = req.FrameMetadata.Resolution

	// Extract facial features from detection data
	// Extract head pose from first face if available
	if len(req.DetectionData.Faces) > 0 {
		face := req.DetectionData.Faces[0]
		violation.HeadPoseYaw = &face.HeadPose.Yaw
		violation.HeadPosePitch = &face.HeadPose.Pitch
		violation.HeadPoseRoll = &face.HeadPose.Roll

		// Extract mouth open ratio
		if face.MouthAspectRatio > 0 {
			violation.MouthOpenRatio = &face.MouthAspectRatio
		}
	}

	// Extract gaze direction from analysis if looking away
	if !req.DetectionData.Analysis.IsLookingAtScreen {
		gaze := "away"
		violation.GazeDirection = &gaze
	}

	// Extract face and hand counts
	violation.FaceCount = len(req.DetectionData.Faces)
	violation.HandCount = len(req.DetectionData.Hands)

	return violation
}

// modelToResponse converts model to response DTO
func (s *ViolationService) modelToResponse(v *model.ViolationLog) *dto.ViolationResponse {
	return &dto.ViolationResponse{
		ID:              v.ID,
		AttemptID:       v.AttemptID,
		ViolationType:   v.ViolationType,
		Severity:        string(v.Severity),
		ServerTimestamp: v.ServerTimestamp,
		Status:          "processed",
	}
}

// Ensure ViolationService implements ViolationServiceInterface
var _ ViolationServiceInterface = (*ViolationService)(nil)
