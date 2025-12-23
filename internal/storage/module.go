package storage

import (
	"protocring-service/internal/config"

	"go.uber.org/fx"
	"go.uber.org/zap"
)

// Module exports storage services for fx dependency injection
var Module = fx.Options(
	fx.Provide(
		NewStorageService,
	),
)

// NewStorageService creates a new storage service based on configuration
// Returns nil if storage is not configured (allows graceful fallback)
func NewStorageService(cfg *config.Config, logger *zap.Logger) (StorageService, error) {
	storageCfg := &StorageConfig{
		Endpoint:        cfg.Storage.Endpoint,
		Region:          cfg.Storage.Region,
		AccessKeyID:     cfg.Storage.AccessKeyID,
		SecretAccessKey: cfg.Storage.SecretAccessKey,
		BucketName:      cfg.Storage.BucketName,
		CDNEndpoint:     cfg.Storage.CDNEndpoint,
		PresignExpiry:   cfg.Storage.PresignExpiry,
	}

	// Return nil if storage is not configured (graceful degradation)
	if storageCfg.Endpoint == "" || storageCfg.BucketName == "" {
		logger.Warn("Storage not configured - presigned URL feature disabled",
			zap.String("endpoint", storageCfg.Endpoint),
			zap.String("bucket", storageCfg.BucketName),
		)
		return nil, nil
	}

	return NewS3Storage(storageCfg, logger)
}
