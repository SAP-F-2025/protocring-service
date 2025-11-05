package main

import (
	"protocring-service/internal/config"
	"protocring-service/internal/handler"
	"protocring-service/internal/middleware"
	repository "protocring-service/internal/repository/module"
	"protocring-service/internal/service"
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
		fx.Provide(database.NewRedisClient),

		// Provide repositories
		repository.Module,

		// Provide services
		service.Module,

		// Provide Gin engine and HTTP server
		fx.Provide(server.NewGinEngine),
		fx.Provide(server.NewHTTPServer),

		// Provide casdoor
		fx.Provide(middleware.NewCasdoorAuthMiddleware),

		// Provide handlers and register routes
		handler.Module,

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
