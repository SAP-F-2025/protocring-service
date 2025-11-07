package database

import (
	"context"
	"fmt"
	"time"

	"protocring-service/internal/config"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// NewPostgresPool creates a new PostgreSQL connection pool
func NewPostgresPool(lc fx.Lifecycle, cfg *config.Config, logger *zap.Logger) (*pgxpool.Pool, error) {
	dsn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.Database.Host,
		cfg.Database.Port,
		cfg.Database.User,
		cfg.Database.Password,
		cfg.Database.DBName,
		cfg.Database.SSLMode,
	)

	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("unable to parse database config: %w", err)
	}

	// Configure pool settings
	poolConfig.MaxConns = int32(cfg.Database.MaxOpenConns)
	poolConfig.MinConns = int32(cfg.Database.MaxIdleConns)
	poolConfig.MaxConnLifetime = cfg.Database.ConnMaxLifetime
	poolConfig.MaxConnIdleTime = 30 * time.Minute
	poolConfig.HealthCheckPeriod = 1 * time.Minute

	var pool *pgxpool.Pool

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			logger.Info("Connecting to PostgreSQL database...")

			p, err := pgxpool.NewWithConfig(ctx, poolConfig)
			if err != nil {
				return fmt.Errorf("unable to create connection pool: %w", err)
			}

			// Verify connection
			if err := p.Ping(ctx); err != nil {
				p.Close()
				return fmt.Errorf("unable to ping database: %w", err)
			}

			pool = p
			logger.Info("Successfully connected to PostgreSQL database",
				zap.String("host", cfg.Database.Host),
				zap.Int("port", cfg.Database.Port),
				zap.String("database", cfg.Database.DBName),
			)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("Closing PostgreSQL connection pool...")
			if pool != nil {
				pool.Close()
			}
			logger.Info("PostgreSQL connection pool closed")
			return nil
		},
	})

	return pool, nil
}
