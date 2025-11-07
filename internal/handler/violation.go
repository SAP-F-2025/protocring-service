package handler

import (
	"net/http"
	"strconv"
	"time"

	"protocring-service/internal/dto"
	"protocring-service/internal/service"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// ViolationHandler handles violation-related HTTP requests
type ViolationHandler struct {
	violationService service.ViolationServiceInterface
	logger           *zap.Logger
}

// NewViolationHandler creates a new violation handler
func NewViolationHandler(
	violationService service.ViolationServiceInterface,
	logger *zap.Logger,
) *ViolationHandler {
	return &ViolationHandler{
		violationService: violationService,
		logger:           logger,
	}
}

// IngestViolation godoc
// @Summary Ingest a single violation
// @Description Process and store a violation detection event from MediaPipe
// @Tags violations
// @Accept json
// @Produce json
// @Param violation body dto.CreateViolationRequest true "Violation data"
// @Success 201 {object} dto.ViolationResponse
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/v1/violations [post]
func (h *ViolationHandler) IngestViolation(c *gin.Context) {
	var req dto.CreateViolationRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		h.logger.Error("Invalid request body", zap.Error(err))
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error:   "invalid_request",
			Message: err.Error(),
		})
		return
	}

	response, err := h.violationService.IngestViolation(c.Request.Context(), &req)
	if err != nil {
		h.logger.Error("Failed to ingest violation",
			zap.Uint64("attempt_id", req.AttemptID),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error:   "ingestion_failed",
			Message: err.Error(),
		})
		return
	}

	c.JSON(http.StatusCreated, response)
}

// IngestBatch godoc
// @Summary Ingest multiple violations in a batch
// @Description Process and store multiple violation events at once (max 50)
// @Tags violations
// @Accept json
// @Produce json
// @Param batch body dto.BatchViolationRequest true "Batch of violations"
// @Success 201 {object} BatchResponse
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/v1/violations/batch [post]
func (h *ViolationHandler) IngestBatch(c *gin.Context) {
	var req dto.BatchViolationRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		h.logger.Error("Invalid batch request", zap.Error(err))
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error:   "invalid_request",
			Message: err.Error(),
		})
		return
	}

	responses, err := h.violationService.IngestBatch(c.Request.Context(), &req)
	if err != nil {
		h.logger.Error("Failed to ingest batch",
			zap.Int("batch_size", len(req.Violations)),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error:   "batch_ingestion_failed",
			Message: err.Error(),
		})
		return
	}

	c.JSON(http.StatusCreated, BatchResponse{
		Success: true,
		Count:   len(responses),
		Data:    responses,
	})
}

// GetViolationsByAttempt godoc
// @Summary Get violations for an attempt
// @Description Retrieve all violations for a specific attempt with pagination
// @Tags violations
// @Produce json
// @Param attempt_id path int true "Attempt ID"
// @Param limit query int false "Limit (default 20, max 100)"
// @Param offset query int false "Offset (default 0)"
// @Success 200 {object} ViolationListResponse
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/v1/violations/attempt/{attempt_id} [get]
func (h *ViolationHandler) GetViolationsByAttempt(c *gin.Context) {
	attemptID, err := strconv.ParseUint(c.Param("attempt_id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error:   "invalid_attempt_id",
			Message: "attempt_id must be a valid number",
		})
		return
	}

	// Parse pagination params
	page := parseIntQuery(c, "page", 1)
	pageSize := parseIntQuery(c, "pageSize", 10)

	violations, err := h.violationService.GetViolationsByAttempt(
		c.Request.Context(),
		attemptID,
		pageSize,
		(page-1)*pageSize,
	)
	if err != nil {
		h.logger.Error("Failed to get violations",
			zap.Uint64("attempt_id", attemptID),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error:   "query_failed",
			Message: err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, ViolationListResponse{
		Data:  violations,
		Count: len(violations),
	})
}

// GetViolationAnalytics godoc
// @Summary Get violation analytics for an attempt
// @Description Get aggregated analytics including counts, severity distribution, and timeline
// @Tags violations
// @Produce json
// @Param attempt_id path int true "Attempt ID"
// @Param bucket_size query string false "Time bucket size (e.g., 5m, 1h)" default(5m)
// @Success 200 {object} repository.ViolationAnalytics
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/v1/violations/analytics/{attempt_id} [get]
func (h *ViolationHandler) GetViolationAnalytics(c *gin.Context) {
	attemptID, err := strconv.ParseUint(c.Param("attempt_id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error:   "invalid_attempt_id",
			Message: "attempt_id must be a valid number",
		})
		return
	}

	// Parse bucket size (default 5 minutes)
	bucketSizeStr := c.DefaultQuery("bucket_size", "5m")
	bucketSize, err := time.ParseDuration(bucketSizeStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error:   "invalid_bucket_size",
			Message: "bucket_size must be a valid duration (e.g., 5m, 1h)",
		})
		return
	}

	analytics, err := h.violationService.GetViolationAnalytics(
		c.Request.Context(),
		attemptID,
		bucketSize,
	)
	if err != nil {
		h.logger.Error("Failed to get analytics",
			zap.Uint64("attempt_id", attemptID),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error:   "analytics_failed",
			Message: err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, analytics)
}

// GetLatestViolation godoc
// @Summary Get latest violation for an attempt
// @Description Get the most recent violation for a specific attempt
// @Tags violations
// @Produce json
// @Param attempt_id path int true "Attempt ID"
// @Success 200 {object} model.ViolationLog
// @Failure 400 {object} ErrorResponse
// @Failure 404 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/v1/violations/attempt/{attempt_id}/latest [get]
func (h *ViolationHandler) GetLatestViolation(c *gin.Context) {
	attemptID, err := strconv.ParseUint(c.Param("attempt_id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error:   "invalid_attempt_id",
			Message: "attempt_id must be a valid number",
		})
		return
	}

	violation, err := h.violationService.GetLatestViolation(c.Request.Context(), attemptID)
	if err != nil {
		h.logger.Error("Failed to get latest violation",
			zap.Uint64("attempt_id", attemptID),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error:   "query_failed",
			Message: err.Error(),
		})
		return
	}

	if violation == nil {
		c.JSON(http.StatusNotFound, ErrorResponse{
			Error:   "not_found",
			Message: "no violations found for this attempt",
		})
		return
	}

	c.JSON(http.StatusOK, violation)
}

// Helper types for responses
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

type BatchResponse struct {
	Success bool                    `json:"success"`
	Count   int                     `json:"count"`
	Data    []dto.ViolationResponse `json:"data"`
}

type ViolationListResponse struct {
	Data   interface{} `json:"data"`
	Count  int         `json:"count"`
	Limit  int         `json:"limit"`
	Offset int         `json:"offset"`
}

// Helper function to parse int query params
func parseIntQuery(c *gin.Context, key string, defaultValue int) int {
	if value := c.Query(key); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			return parsed
		}
	}
	return defaultValue
}
