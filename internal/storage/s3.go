package storage

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.uber.org/zap"
)

// S3Storage implements StorageService using AWS SDK v2
// Compatible with DigitalOcean Spaces, MinIO, and other S3-compatible services
type S3Storage struct {
	client        *s3.Client
	presignClient *s3.PresignClient
	config        *StorageConfig
	logger        *zap.Logger
}

// normalizeEndpoint removes https:// prefix from endpoint if present
func normalizeEndpoint(endpoint string) string {
	endpoint = strings.TrimPrefix(endpoint, "https://")
	endpoint = strings.TrimPrefix(endpoint, "http://")
	return endpoint
}

// NewS3Storage creates a new S3-compatible storage service
func NewS3Storage(cfg *StorageConfig, logger *zap.Logger) (*S3Storage, error) {
	// DigitalOcean Spaces requires us-east-1 as region for AWS SDK compatibility
	// The actual datacenter region is determined by the endpoint (e.g., sgp1.digitaloceanspaces.com)
	awsRegion := cfg.Region
	if awsRegion != "us-east-1" {
		awsRegion = "us-east-1"
	}

	// Create AWS config with credentials
	awsCfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(awsRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.AccessKeyID,
			cfg.SecretAccessKey,
			"",
		)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Build the endpoint URL (ensure no duplicate https://)
	normalizedEndpoint := normalizeEndpoint(cfg.Endpoint)
	endpointURL := fmt.Sprintf("https://%s", normalizedEndpoint)

	// Create S3 client with BaseEndpoint (replaces deprecated EndpointResolverWithOptions)
	// UsePathStyle = false for virtual-hosted-style URLs (required for DO Spaces)
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpointURL)
		o.UsePathStyle = false
	})

	// Create presign client
	presignClient := s3.NewPresignClient(client)

	logger.Info("S3 storage initialized",
		zap.String("endpoint", cfg.Endpoint),
		zap.String("bucket", cfg.BucketName),
		zap.String("aws_region", awsRegion),
	)

	return &S3Storage{
		client:        client,
		presignClient: presignClient,
		config:        cfg,
		logger:        logger,
	}, nil
}

// GeneratePresignedPutURL creates a presigned URL for uploading an object
func (s *S3Storage) GeneratePresignedPutURL(ctx context.Context, objectKey string, contentType string) (string, error) {
	expiry := s.config.PresignExpiry
	if expiry == 0 {
		expiry = 5 * time.Minute
	}

	request, err := s.presignClient.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.config.BucketName),
		Key:         aws.String(objectKey),
		ContentType: aws.String(contentType),
	}, s3.WithPresignExpires(expiry))

	if err != nil {
		s.logger.Error("Failed to generate presigned PUT URL",
			zap.String("object_key", objectKey),
			zap.Error(err),
		)
		return "", fmt.Errorf("failed to generate presigned PUT URL: %w", err)
	}

	s.logger.Debug("Generated presigned PUT URL",
		zap.String("object_key", objectKey),
		zap.Duration("expiry", expiry),
	)

	return request.URL, nil
}

// GeneratePresignedGetURL creates a presigned URL for downloading an object
// Note: For CDN usage, use GeneratePresignedGetURLWithCDN instead
func (s *S3Storage) GeneratePresignedGetURL(ctx context.Context, objectKey string) (string, error) {
	expiry := s.config.PresignExpiry
	if expiry == 0 {
		expiry = 5 * time.Minute
	}

	request, err := s.presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.config.BucketName),
		Key:    aws.String(objectKey),
	}, s3.WithPresignExpires(expiry))

	if err != nil {
		s.logger.Error("Failed to generate presigned GET URL",
			zap.String("object_key", objectKey),
			zap.Error(err),
		)
		return "", fmt.Errorf("failed to generate presigned GET URL: %w", err)
	}

	return request.URL, nil
}

// GeneratePresignedGetURLWithCDN creates a presigned URL with CDN hostname replacement
// Important: DigitalOcean Spaces CDN does NOT cache presigned URLs
// Using CDN with presigned URLs may double bandwidth charges without performance benefit
// This method is provided for flexibility but direct endpoint is recommended for presigned access
func (s *S3Storage) GeneratePresignedGetURLWithCDN(ctx context.Context, objectKey string) (string, error) {
	// Generate presigned URL with origin endpoint
	url, err := s.GeneratePresignedGetURL(ctx, objectKey)
	if err != nil {
		return "", err
	}

	// If CDN endpoint is configured, replace the hostname
	if s.config.CDNEndpoint != "" {
		// Replace bucket.endpoint with CDN endpoint
		origin := fmt.Sprintf("%s.%s", s.config.BucketName, s.config.Endpoint)
		url = strings.Replace(url, origin, s.config.CDNEndpoint, 1)
	}

	return url, nil
}

// DeleteObject removes an object from storage
func (s *S3Storage) DeleteObject(ctx context.Context, objectKey string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.config.BucketName),
		Key:    aws.String(objectKey),
	})

	if err != nil {
		s.logger.Error("Failed to delete object",
			zap.String("object_key", objectKey),
			zap.Error(err),
		)
		return fmt.Errorf("failed to delete object: %w", err)
	}

	s.logger.Info("Object deleted",
		zap.String("object_key", objectKey),
	)

	return nil
}

// GetPublicURL returns the public/CDN URL for an object
func (s *S3Storage) GetPublicURL(objectKey string) string {
	// Use CDN endpoint if configured
	if s.config.CDNEndpoint != "" {
		normalizedCDN := normalizeEndpoint(s.config.CDNEndpoint)
		return fmt.Sprintf("https://%s/%s", normalizedCDN, objectKey)
	}

	// Fall back to direct bucket URL
	normalizedEndpoint := normalizeEndpoint(s.config.Endpoint)
	return fmt.Sprintf("https://%s.%s/%s", s.config.BucketName, normalizedEndpoint, objectKey)
}

// ObjectExists checks if an object exists in storage
func (s *S3Storage) ObjectExists(ctx context.Context, objectKey string) (bool, error) {
	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.config.BucketName),
		Key:    aws.String(objectKey),
	})

	if err != nil {
		// Check if it's a "not found" error
		// AWS SDK v2 doesn't export NotFound error type directly
		return false, nil
	}

	return true, nil
}

// Ensure S3Storage implements StorageService
var _ StorageService = (*S3Storage)(nil)
