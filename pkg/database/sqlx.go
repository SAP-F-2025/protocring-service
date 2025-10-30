package database

import (
	"context"
	"fmt"

	"protocring-service/internal/config"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// NewSQLXDB creates a new sqlx database connection
func NewSQLXDB(lc fx.Lifecycle, cfg *config.Config, logger *zap.Logger) (*sqlx.DB, error) {
	dsn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.Database.Host,
		cfg.Database.Port,
		cfg.Database.User,
		cfg.Database.Password,
		cfg.Database.DBName,
		cfg.Database.SSLMode,
	)

	db, err := sqlx.Connect("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(cfg.Database.MaxOpenConns)
	db.SetMaxIdleConns(cfg.Database.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.Database.ConnMaxLifetime)

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			logger.Info("Connecting to PostgreSQL/TimescaleDB database...")

			// Verify connection
			if err := db.PingContext(ctx); err != nil {
				return fmt.Errorf("unable to ping database: %w", err)
			}

			logger.Info("Successfully connected to PostgreSQL/TimescaleDB database",
				zap.String("host", cfg.Database.Host),
				zap.Int("port", cfg.Database.Port),
				zap.String("database", cfg.Database.DBName),
			)

			// Check if TimescaleDB extension is available
			var exists bool
			err := db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')").Scan(&exists)
			if err == nil && exists {
				logger.Info("TimescaleDB extension is enabled")
			} else {
				logger.Warn("TimescaleDB extension is not enabled")
			}

			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("Closing database connection...")
			if err := db.Close(); err != nil {
				return fmt.Errorf("error closing database: %w", err)
			}
			logger.Info("Database connection closed")
			return nil
		},
	})

	return db, nil
}

// Transaction executes a function within a database transaction
func Transaction(ctx context.Context, db *sqlx.DB, fn func(*sqlx.Tx) error) error {
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
	}()

	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("failed to rollback transaction: %v (original error: %w)", rbErr, err)
		}
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// GetDBStats returns database statistics
func GetDBStats(db *sqlx.DB) map[string]interface{} {
	stats := db.Stats()
	return map[string]interface{}{
		"max_open_connections": stats.MaxOpenConnections,
		"open_connections":     stats.OpenConnections,
		"in_use":               stats.InUse,
		"idle":                 stats.Idle,
		"wait_count":           stats.WaitCount,
		"wait_duration":        stats.WaitDuration.String(),
		"max_idle_closed":      stats.MaxIdleClosed,
		"max_lifetime_closed":  stats.MaxLifetimeClosed,
	}
}
