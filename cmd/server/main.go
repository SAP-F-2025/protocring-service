package main

import (
	"context"
	"net/http"
	"protocring-service/internal/config"
	"protocring-service/internal/handler"
	"protocring-service/internal/middleware"
	"protocring-service/internal/notification"
	repository "protocring-service/internal/repository/module"
	"protocring-service/internal/service"
	"protocring-service/internal/storage"
	"protocring-service/internal/worker"
	"protocring-service/pkg/database"
	"protocring-service/pkg/server"

	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
	"go.uber.org/zap"
)

func main() {
	fx.New(
		// Provide logger
		fx.Provide(NewLogger),

		// Provide config
		fx.Provide(config.Load),

		// Provide database connections
		fx.Provide(database.NewSQLXDB),

		// Provide notification module (named Redis clients + notification components)
		notification.Module,

		// Provide repositories
		repository.Module,

		// Provide storage (optional - returns nil if not configured)
		storage.Module,

		// Provide services
		service.Module,

		// Provide workers
		worker.Module,

		// Provide Gin engine and HTTP server
		fx.Provide(server.NewGinEngine),
		fx.Provide(server.NewHTTPServer),

		// Provide casdoor
		fx.Provide(middleware.NewCasdoorAuthMiddleware),

		// Provide handlers and register routes
		handler.Module,

		// Invoke HTTP server to ensure it starts
		fx.Invoke(func(*http.Server) {}),

		// Start workers if enabled
		fx.Invoke(RegisterWorkerLifecycle),

		// Configure fx logger
		fx.WithLogger(func(logger *zap.Logger) fxevent.Logger {
			return &fxevent.ZapLogger{Logger: logger}
		}),
	).Run()
}

// NewLogger creates a new zap logger based on config
func NewLogger(cfg *config.Config) (*zap.Logger, error) {
	var logger *zap.Logger
	var err error

	if cfg.Log.Format == "json" {
		logger, err = zap.NewProduction()
	} else {
		logger, err = zap.NewDevelopment()
	}

	if err != nil {
		return nil, err
	}

	return logger, nil
}

// RegisterWorkerLifecycle registers worker lifecycle hooks
func RegisterWorkerLifecycle(
	lc fx.Lifecycle,
	worker *worker.ViolationWorker,
	cfg *config.Config,
	logger *zap.Logger,
) {
	if !cfg.Worker.Enabled {
		logger.Info("Workers disabled in configuration")
		return
	}

	var workerCtx context.Context
	var workerCancel context.CancelFunc

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			logger.Info("Starting violation workers...")

			// Create a new context for workers
			workerCtx, workerCancel = context.WithCancel(context.Background())

			// Start workers in background
			go func() {
				if err := worker.Start(workerCtx); err != nil {
					if err != context.Canceled {
						logger.Error("Worker error", zap.Error(err))
					}
				}
			}()

			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("Stopping violation workers...")

			// Cancel worker context
			if workerCancel != nil {
				workerCancel()
			}

			// Graceful shutdown with timeout from FX context
			if err := worker.Shutdown(ctx); err != nil {
				logger.Warn("Worker shutdown warning", zap.Error(err))
			}

			logger.Info("Violation workers stopped")
			return nil
		},
	})
}
