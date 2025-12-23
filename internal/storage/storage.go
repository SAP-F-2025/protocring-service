package storage

import (
	"context"
	"time"
)

// StorageConfig holds configuration for object storage
type StorageConfig struct {
	Endpoint        string        `mapstructure:"endpoint"`          // e.g., "sgp1.digitaloceanspaces.com"
	Region          string        `mapstructure:"region"`            // e.g., "sgp1"
	AccessKeyID     string        `mapstructure:"access_key_id"`     // DO Spaces access key
	SecretAccessKey string        `mapstructure:"secret_access_key"` // DO Spaces secret key
	BucketName      string        `mapstructure:"bucket_name"`       // e.g., "sap-violations"
	CDNEndpoint     string        `mapstructure:"cdn_endpoint"`      // e.g., "sap-violations.sgp1.cdn.digitaloceanspaces.com"
	PresignExpiry   time.Duration `mapstructure:"presign_expiry"`    // Default: 5 minutes
}

// StorageService defines the interface for object storage operations
type StorageService interface {
	// GeneratePresignedPutURL creates a presigned URL for uploading an object
	GeneratePresignedPutURL(ctx context.Context, objectKey string, contentType string) (string, error)

	// GeneratePresignedGetURL creates a presigned URL for downloading an object
	GeneratePresignedGetURL(ctx context.Context, objectKey string) (string, error)

	// DeleteObject removes an object from storage
	DeleteObject(ctx context.Context, objectKey string) error

	// GetPublicURL returns the public/CDN URL for an object
	GetPublicURL(objectKey string) string

	// ObjectExists checks if an object exists in storage
	ObjectExists(ctx context.Context, objectKey string) (bool, error)
}
