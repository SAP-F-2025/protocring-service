package service

import (
	"protocring-service/internal/config"
	"protocring-service/internal/repository"
	"protocring-service/pkg/streams"

	"go.uber.org/zap"
)

// NewViolationServiceFactory creates a ViolationServiceInterface based on configuration
// If workers are enabled, returns AsyncViolationService, otherwise returns sync ViolationService
func NewViolationServiceFactory(
	cfg *config.Config,
	repo repository.ViolationRepositoryInterface,
	producer *streams.Producer,
	logger *zap.Logger,
) ViolationServiceInterface {
	// Create sync service (always needed as fallback)
	syncService := NewViolationService(repo, logger)

	// If workers enabled, wrap with async service
	if cfg.Worker.Enabled {
		logger.Info("Using async violation service with workers")
		return NewAsyncViolationService(producer, syncService, cfg, logger)
	}

	logger.Info("Using sync violation service (workers disabled)")
	return syncService
}
