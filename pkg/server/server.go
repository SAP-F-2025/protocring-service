package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"protocring-service/internal/config"

	"github.com/gin-gonic/gin"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// NewGinEngine creates a new Gin engine
func NewGinEngine(cfg *config.Config) *gin.Engine {
	// Set Gin mode
	gin.SetMode(cfg.Server.Mode)

	engine := gin.New()

	// Use default middleware
	engine.Use(gin.Recovery())
	engine.Use(gin.Logger())

	return engine
}

// NewHTTPServer creates a new HTTP server
func NewHTTPServer(lc fx.Lifecycle, engine *gin.Engine, cfg *config.Config, logger *zap.Logger) *http.Server {
	srv := &http.Server{
		Addr:         fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port),
		Handler:      engine,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			logger.Info("Starting HTTP server",
				zap.String("address", srv.Addr),
			)

			go func() {
				if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
					logger.Fatal("Failed to start HTTP server", zap.Error(err))
				}
			}()

			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info("Stopping HTTP server...")

			shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			if err := srv.Shutdown(shutdownCtx); err != nil {
				return fmt.Errorf("error shutting down HTTP server: %w", err)
			}

			logger.Info("HTTP server stopped")
			return nil
		},
	})

	return srv
}
