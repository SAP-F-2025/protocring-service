package handler

import (
	"net/http"

	"protocring-service/internal/service"

	"github.com/gin-gonic/gin"
)

type HealthHandler struct {
	healthService *service.HealthService
}

func NewHealthHandler(healthService *service.HealthService) *HealthHandler {
	return &HealthHandler{
		healthService: healthService,
	}
}

// RegisterRoutes registers health check routes
func (h *HealthHandler) RegisterRoutes(router *gin.Engine) {
	router.GET("/health", h.Health)
	router.GET("/ready", h.Ready)
}

// Health godoc
// @Summary Health check endpoint
// @Description Get health status of the service and its dependencies
// @Tags health
// @Produce json
// @Success 200 {object} service.HealthStatus
// @Failure 503 {object} service.HealthStatus
// @Router /health [get]
func (h *HealthHandler) Health(c *gin.Context) {
	status := h.healthService.Check(c.Request.Context())

	if status.Status == "healthy" {
		c.JSON(http.StatusOK, status)
	} else {
		c.JSON(http.StatusServiceUnavailable, status)
	}
}

// Ready godoc
// @Summary Readiness check endpoint
// @Description Check if the service is ready to accept requests
// @Tags health
// @Produce json
// @Success 200 {object} map[string]string
// @Router /ready [get]
func (h *HealthHandler) Ready(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status": "ready",
	})
}
