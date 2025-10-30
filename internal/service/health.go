package service

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
)

type HealthService struct {
	db    *sqlx.DB
	redis *redis.Client
}

func NewHealthService(db *sqlx.DB, redis *redis.Client) *HealthService {
	return &HealthService{
		db:    db,
		redis: redis,
	}
}

type HealthStatus struct {
	Status       string            `json:"status"`
	Checks       map[string]string `json:"checks"`
	Timestamp    time.Time         `json:"timestamp"`
	TimescaleDB  bool              `json:"timescaledb_enabled"`
}

func (s *HealthService) Check(ctx context.Context) *HealthStatus {
	status := &HealthStatus{
		Status:    "healthy",
		Checks:    make(map[string]string),
		Timestamp: time.Now(),
	}

	// Check TimescaleDB
	if err := s.db.PingContext(ctx); err != nil {
		status.Checks["timescaledb"] = "unhealthy: " + err.Error()
		status.Status = "unhealthy"
	} else {
		status.Checks["timescaledb"] = "healthy"

		// Check if TimescaleDB extension is enabled
		var exists bool
		err := s.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')").Scan(&exists)
		status.TimescaleDB = (err == nil && exists)
	}

	// Check Redis
	if err := s.redis.Ping(ctx).Err(); err != nil {
		status.Checks["redis"] = "unhealthy: " + err.Error()
		status.Status = "unhealthy"
	} else {
		status.Checks["redis"] = "healthy"
	}

	return status
}
