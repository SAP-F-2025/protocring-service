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
	dashboardHandler *DashboardHandler,
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

		// Dashboard routes - Analytics using Continuous Aggregates
		dashboard := v1.Group("/dashboard")
		{
			// Time-series stats from continuous aggregates
			dashboard.GET("/stats/hourly", dashboardHandler.GetHourlyStats)
			dashboard.GET("/stats/daily", dashboardHandler.GetDailyStats)

			// Attempt analytics
			dashboard.GET("/attempts/:attempt_id/summary", dashboardHandler.GetAttemptSummary)
			dashboard.GET("/attempts/summaries", dashboardHandler.GetAttemptSummaries)

			// User analytics
			dashboard.GET("/users/:user_id/patterns", dashboardHandler.GetUserPatterns)

			// Overview and real-time
			dashboard.GET("/overview", dashboardHandler.GetDashboardOverview)
			dashboard.GET("/realtime", dashboardHandler.GetRealTimeStats)
		}
	}
}

// Module exports dependency for fx
var Module = fx.Options(
	fx.Provide(NewHealthHandler),
	fx.Provide(NewViolationHandler),
	fx.Provide(NewDashboardHandler),
	fx.Invoke(RegisterRoutes),
)
