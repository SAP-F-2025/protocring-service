package database

import (
	"context"
	"fmt"

	"protocring-service/internal/config"

	"github.com/redis/go-redis/v9"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// NewRedisClient creates a new Redis client
func NewRedisClient(lc fx.Lifecycle, cfg *config.Config, logger *zap.Logger) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Redis.Host, cfg.Redis.Port),
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			logger.Info("Connecting to Redis...")

			// Verify connection
			if err := client.Ping(ctx).Err(); err != nil {
				return fmt.Errorf("unable to connect to Redis: %w", err)
			}

			logger.Info("Successfully connected to Redis",
				zap.String("host", cfg.Redis.Host),
				zap.Int("port", cfg.Redis.Port),
				zap.Int("db", cfg.Redis.DB),
			)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("Closing Redis connection...")
			if err := client.Close(); err != nil {
				return fmt.Errorf("error closing Redis connection: %w", err)
			}
			logger.Info("Redis connection closed")
			return nil
		},
	})

	return client, nil
}
