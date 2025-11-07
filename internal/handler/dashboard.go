package handler

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"protocring-service/internal/repository"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// DashboardHandler handles dashboard-related HTTP requests
type DashboardHandler struct {
	violationRepo repository.ViolationRepositoryInterface
	logger        *zap.Logger
}

// NewDashboardHandler creates a new dashboard handler
func NewDashboardHandler(
	violationRepo repository.ViolationRepositoryInterface,
	logger *zap.Logger,
) *DashboardHandler {
	return &DashboardHandler{
		violationRepo: violationRepo,
		logger:        logger,
	}
}

// GetHourlyStats godoc
// @Summary Get hourly violation statistics
// @Description Get aggregated hourly violation stats from continuous aggregate
// @Tags dashboard
// @Produce json
// @Param start_time query string false "Start time (RFC3339 format, default: 24 hours ago)"
// @Param end_time query string false "End time (RFC3339 format, default: now)"
// @Success 200 {array} model.HourlyViolationStats
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/v1/dashboard/stats/hourly [get]
func (h *DashboardHandler) GetHourlyStats(c *gin.Context) {
	startTime, endTime, err := h.parseTimeRange(c, 24*time.Hour)
	if err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error:   "invalid_time_range",
			Message: err.Error(),
		})
		return
	}

	stats, err := h.violationRepo.GetHourlyStats(c.Request.Context(), startTime, endTime)
	if err != nil {
		h.logger.Error("Failed to get hourly stats", zap.Error(err))
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error:   "query_failed",
			Message: err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"start_time": startTime,
		"end_time":   endTime,
		"data":       stats,
		"count":      len(stats),
	})
}

// GetDailyStats godoc
// @Summary Get daily violation statistics
// @Description Get aggregated daily violation stats from continuous aggregate
// @Tags dashboard
// @Produce json
// @Param start_time query string false "Start time (RFC3339 format, default: 30 days ago)"
// @Param end_time query string false "End time (RFC3339 format, default: now)"
// @Success 200 {array} model.DailyViolationStats
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/v1/dashboard/stats/daily [get]
func (h *DashboardHandler) GetDailyStats(c *gin.Context) {
	startTime, endTime, err := h.parseTimeRange(c, 30*24*time.Hour)
	if err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error:   "invalid_time_range",
			Message: err.Error(),
		})
		return
	}

	stats, err := h.violationRepo.GetDailyStats(c.Request.Context(), startTime, endTime)
	if err != nil {
		h.logger.Error("Failed to get daily stats", zap.Error(err))
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error:   "query_failed",
			Message: err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"start_time": startTime,
		"end_time":   endTime,
		"data":       stats,
		"count":      len(stats),
	})
}

// GetAttemptSummary godoc
// @Summary Get violation summary for an attempt
// @Description Get aggregated violation summary for a specific attempt from continuous aggregate
// @Tags dashboard
// @Produce json
// @Param attempt_id path int true "Attempt ID"
// @Success 200 {object} model.AttemptViolationSummary
// @Failure 400 {object} ErrorResponse
// @Failure 404 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/v1/dashboard/attempts/{attempt_id}/summary [get]
func (h *DashboardHandler) GetAttemptSummary(c *gin.Context) {
	attemptID, err := strconv.ParseUint(c.Param("attempt_id"), 10, 64)
	if err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error:   "invalid_attempt_id",
			Message: "attempt_id must be a valid number",
		})
		return
	}

	summary, err := h.violationRepo.GetAttemptSummary(c.Request.Context(), attemptID)
	if err != nil {
		h.logger.Error("Failed to get attempt summary",
			zap.Uint64("attempt_id", attemptID),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error:   "query_failed",
			Message: err.Error(),
		})
		return
	}

	if summary == nil {
		c.JSON(http.StatusNotFound, ErrorResponse{
			Error:   "not_found",
			Message: "no summary found for this attempt",
		})
		return
	}

	c.JSON(http.StatusOK, summary)
}

// GetAttemptSummaries godoc
// @Summary Get violation summaries for multiple attempts
// @Description Get aggregated violation summaries for multiple attempts (comma-separated IDs)
// @Tags dashboard
// @Produce json
// @Param attempt_ids query string true "Comma-separated attempt IDs (e.g., 1,2,3)"
// @Success 200 {array} model.AttemptViolationSummary
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/v1/dashboard/attempts/summaries [get]
func (h *DashboardHandler) GetAttemptSummaries(c *gin.Context) {
	attemptIDsStr := c.Query("attempt_ids")
	if attemptIDsStr == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error:   "missing_parameter",
			Message: "attempt_ids parameter is required",
		})
		return
	}

	// Parse comma-separated IDs
	idStrs := strings.Split(attemptIDsStr, ",")
	attemptIDs := make([]uint64, 0, len(idStrs))
	for _, idStr := range idStrs {
		id, err := strconv.ParseUint(strings.TrimSpace(idStr), 10, 64)
		if err != nil {
			c.JSON(http.StatusBadRequest, ErrorResponse{
				Error:   "invalid_attempt_id",
				Message: "all attempt_ids must be valid numbers",
			})
			return
		}
		attemptIDs = append(attemptIDs, id)
	}

	summaries, err := h.violationRepo.GetAttemptSummaries(c.Request.Context(), attemptIDs)
	if err != nil {
		h.logger.Error("Failed to get attempt summaries", zap.Error(err))
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error:   "query_failed",
			Message: err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"data":  summaries,
		"count": len(summaries),
	})
}

// GetUserPatterns godoc
// @Summary Get violation patterns for a user
// @Description Get daily violation patterns for a specific user from continuous aggregate
// @Tags dashboard
// @Produce json
// @Param user_id path string true "User ID"
// @Param start_time query string false "Start time (RFC3339 format, default: 30 days ago)"
// @Param end_time query string false "End time (RFC3339 format, default: now)"
// @Success 200 {array} model.UserViolationPattern
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/v1/dashboard/users/{user_id}/patterns [get]
func (h *DashboardHandler) GetUserPatterns(c *gin.Context) {
	userID := c.Param("user_id")
	if userID == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error:   "missing_parameter",
			Message: "user_id is required",
		})
		return
	}

	startTime, endTime, err := h.parseTimeRange(c, 30*24*time.Hour)
	if err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error:   "invalid_time_range",
			Message: err.Error(),
		})
		return
	}

	patterns, err := h.violationRepo.GetUserPatterns(c.Request.Context(), userID, startTime, endTime)
	if err != nil {
		h.logger.Error("Failed to get user patterns",
			zap.String("user_id", userID),
			zap.Error(err),
		)
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error:   "query_failed",
			Message: err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"user_id":    userID,
		"start_time": startTime,
		"end_time":   endTime,
		"data":       patterns,
		"count":      len(patterns),
	})
}

// GetDashboardOverview godoc
// @Summary Get dashboard overview
// @Description Get high-level dashboard metrics with comparison to previous period
// @Tags dashboard
// @Produce json
// @Param start_time query string false "Start time (RFC3339 format, default: 7 days ago)"
// @Param end_time query string false "End time (RFC3339 format, default: now)"
// @Success 200 {object} model.DashboardOverview
// @Failure 400 {object} ErrorResponse
// @Failure 500 {object} ErrorResponse
// @Router /api/v1/dashboard/overview [get]
func (h *DashboardHandler) GetDashboardOverview(c *gin.Context) {
	startTime, endTime, err := h.parseTimeRange(c, 7*24*time.Hour)
	if err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{
			Error:   "invalid_time_range",
			Message: err.Error(),
		})
		return
	}

	overview, err := h.violationRepo.GetDashboardOverview(c.Request.Context(), startTime, endTime)
	if err != nil {
		h.logger.Error("Failed to get dashboard overview", zap.Error(err))
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error:   "query_failed",
			Message: err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"start_time": startTime,
		"end_time":   endTime,
		"data":       overview,
	})
}

// GetRealTimeStats godoc
// @Summary Get real-time violation statistics
// @Description Get real-time violation statistics for the last 5 minutes and last hour
// @Tags dashboard
// @Produce json
// @Success 200 {object} model.RealTimeStats
// @Failure 500 {object} ErrorResponse
// @Router /api/v1/dashboard/realtime [get]
func (h *DashboardHandler) GetRealTimeStats(c *gin.Context) {
	stats, err := h.violationRepo.GetRealTimeStats(c.Request.Context())
	if err != nil {
		h.logger.Error("Failed to get real-time stats", zap.Error(err))
		c.JSON(http.StatusInternalServerError, ErrorResponse{
			Error:   "query_failed",
			Message: err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, stats)
}

// Helper function to parse time range from query parameters
func (h *DashboardHandler) parseTimeRange(c *gin.Context, defaultDuration time.Duration) (time.Time, time.Time, error) {
	endTime := time.Now()
	startTime := endTime.Add(-defaultDuration)

	// Parse start_time if provided
	if startTimeStr := c.Query("start_time"); startTimeStr != "" {
		parsed, err := time.Parse(time.RFC3339, startTimeStr)
		if err != nil {
			return time.Time{}, time.Time{}, err
		}
		startTime = parsed
	}

	// Parse end_time if provided
	if endTimeStr := c.Query("end_time"); endTimeStr != "" {
		parsed, err := time.Parse(time.RFC3339, endTimeStr)
		if err != nil {
			return time.Time{}, time.Time{}, err
		}
		endTime = parsed
	}

	return startTime, endTime, nil
}
