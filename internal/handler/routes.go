package handler

import (
	"github.com/gin-gonic/gin"
	"go.uber.org/fx"
)

// RegisterRoutes registers all application routes
func RegisterRoutes(
	engine *gin.Engine,
	healthHandler *HealthHandler,
	violationHandler *ViolationHandler,
) {
	// Register health routes
	healthHandler.RegisterRoutes(engine)

	// API v1 group
	v1 := engine.Group("/api/v1")
	{
		// Ping endpoint
		v1.GET("/ping", func(c *gin.Context) {
			c.JSON(200, gin.H{
				"message": "pong",
			})
		})

		// Violation routes
		violations := v1.Group("/violations")
		{
			violations.POST("", violationHandler.IngestViolation)
			violations.POST("/batch", violationHandler.IngestBatch)
			violations.GET("/attempt/:attempt_id", violationHandler.GetViolationsByAttempt)
			violations.GET("/attempt/:attempt_id/latest", violationHandler.GetLatestViolation)
			violations.GET("/analytics/:attempt_id", violationHandler.GetViolationAnalytics)
		}
	}
}

// Module exports dependency for fx
var Module = fx.Options(
	fx.Provide(NewHealthHandler),
	fx.Provide(NewViolationHandler),
	fx.Invoke(RegisterRoutes),
)
