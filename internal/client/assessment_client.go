package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"protocring-service/internal/config"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// AssessmentClient provides API access to assessment-service with caching
type AssessmentClient struct {
	baseURL     string
	serviceKey  string
	httpClient  *http.Client
	redisClient *redis.Client // Use worker Redis for cache
	logger      *zap.Logger
	cacheTTL    time.Duration
}

// AttemptInfo represents the response from assessment-service
type AttemptInfo struct {
	ID           uint `json:"id"`
	AssessmentID uint `json:"assessment_id"`
	Assessment   struct {
		ID        uint   `json:"id"`
		Title     string `json:"title"`
		CreatedBy string `json:"created_by"`
	} `json:"assessment"`
}

// NewAssessmentClient creates a new assessment service client
func NewAssessmentClient(
	cfg *config.AssessmentServiceConfig,
	redisClient *redis.Client,
	logger *zap.Logger,
) *AssessmentClient {
	return &AssessmentClient{
		baseURL:    cfg.BaseURL,
		serviceKey: cfg.ServiceKey,
		httpClient: &http.Client{
			Timeout: cfg.Timeout,
		},
		redisClient: redisClient,
		logger:      logger,
		cacheTTL:    cfg.CacheTTL,
	}
}

// GetAttemptCreator gets the creator (teacher) ID of an attempt's assessment
// Uses Redis caching to minimize API calls
func (c *AssessmentClient) GetAttemptCreator(ctx context.Context, attemptID uint64) (string, error) {
	cacheKey := fmt.Sprintf("attempt:%d:creator", attemptID)

	// 1. Check cache first
	if c.redisClient != nil {
		cached, err := c.redisClient.Get(ctx, cacheKey).Result()
		if err == nil && cached != "" {
			c.logger.Debug("Cache hit for attempt creator",
				zap.Uint64("attempt_id", attemptID),
				zap.String("creator_id", cached))
			return cached, nil
		}
	}

	// 2. Cache miss - call API
	url := fmt.Sprintf("%s/api/v1/attempts/%d/details", c.baseURL, attemptID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	// Set service key header for internal authentication
	if c.serviceKey != "" {
		req.Header.Set("X-Service-Key", c.serviceKey)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.logger.Warn("Failed to call assessment-service",
			zap.Uint64("attempt_id", attemptID),
			zap.String("url", url),
			zap.Error(err))
		return "", fmt.Errorf("API call failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		c.logger.Warn("Assessment-service returned non-OK status",
			zap.Uint64("attempt_id", attemptID),
			zap.Int("status_code", resp.StatusCode))
		return "", fmt.Errorf("API returned status %d", resp.StatusCode)
	}

	var info AttemptInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return "", fmt.Errorf("failed to decode response: %w", err)
	}

	creatorID := info.Assessment.CreatedBy
	if creatorID == "" {
		c.logger.Warn("Assessment has no creator_id",
			zap.Uint64("attempt_id", attemptID),
			zap.Uint("assessment_id", info.AssessmentID))
		return "", fmt.Errorf("assessment has no creator")
	}

	// 3. Store in cache (async to not block)
	if c.redisClient != nil {
		go func() {
			cacheCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := c.redisClient.Set(cacheCtx, cacheKey, creatorID, c.cacheTTL).Err(); err != nil {
				c.logger.Warn("Failed to cache creator_id",
					zap.Uint64("attempt_id", attemptID),
					zap.Error(err))
			}
		}()
	}

	c.logger.Info("Fetched attempt creator from API",
		zap.Uint64("attempt_id", attemptID),
		zap.String("creator_id", creatorID))

	return creatorID, nil
}

// InvalidateCache removes cached creator info for an attempt
func (c *AssessmentClient) InvalidateCache(ctx context.Context, attemptID uint64) error {
	if c.redisClient == nil {
		return nil
	}
	cacheKey := fmt.Sprintf("attempt:%d:creator", attemptID)
	return c.redisClient.Del(ctx, cacheKey).Err()
}
