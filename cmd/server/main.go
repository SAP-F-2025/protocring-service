package main

import (
	"context"
	"net/http"
	"protocring-service/internal/config"
	"protocring-service/internal/handler"
	"protocring-service/internal/middleware"
	repository "protocring-service/internal/repository/module"
	"protocring-service/internal/service"
	"protocring-service/internal/worker"
	"protocring-service/pkg/database"
	"protocring-service/pkg/server"
	"protocring-service/pkg/streams"
	"time"

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
		fx.Provide(database.NewRedisClient),

		// Provide repositories
		repository.Module,

		// Provide streams producer
		fx.Provide(streams.NewProducer),

		// Provide worker manager
		worker.Module,

		// Provide services
		service.Module,

		// Provide Gin engine and HTTP server
		fx.Provide(server.NewGinEngine),
		fx.Provide(server.NewHTTPServer),

		// Provide casdoor
		fx.Provide(middleware.NewCasdoorAuthMiddleware),

		// Provide handlers and register routes
		handler.Module,

		// Invoke HTTP server to ensure it starts
		fx.Invoke(func(*http.Server) {}),

		// Invoke workers to start them
		fx.Invoke(startWorkers),

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

// startWorkers initializes and starts the worker manager
func startWorkers(
	lc fx.Lifecycle,
	manager *worker.WorkerManager,
	cfg *config.Config,
	logger *zap.Logger,
) {
	if !cfg.Worker.Enabled {
		logger.Info("Workers disabled by configuration")
		return
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			logger.Info("Starting workers...")

			// Start workers in a goroutine to not block application startup
			go func() {
				if err := manager.Start(context.Background()); err != nil {
					logger.Error("Worker manager failed to start", zap.Error(err))
				}
			}()

			logger.Info("Workers started successfully")
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("Shutting down workers...")

			// Create shutdown context with timeout
			shutdownCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()

			if err := manager.Shutdown(shutdownCtx); err != nil {
				logger.Error("Error during worker shutdown", zap.Error(err))
				return err
			}

			logger.Info("Workers shut down successfully")
			return nil
		},
	})
}
