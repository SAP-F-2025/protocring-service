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
	Student      struct {
		ID       string `json:"id"`
		FullName string `json:"full_name"`
		Email    string `json:"email"`
	} `json:"student"`
	Assessment struct {
		ID      uint   `json:"id"`
		Title   string `json:"title"`
		Creator struct {
			FullName string `json:"full_name"`
		} `json:"creator"`
		CreatedBy string `json:"created_by"`
	} `json:"assessment"`
}

// AttemptDetails contains info needed for notification
type AttemptDetails struct {
	CreatorID       string // Teacher/creator ID
	StudentUsername string // Student's full name
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

// GetAttemptDetails gets the creator ID and student username for an attempt
// Uses Redis caching to minimize API calls
func (c *AssessmentClient) GetAttemptDetails(ctx context.Context, attemptID uint64) (*AttemptDetails, error) {
	cacheKey := fmt.Sprintf("attempt:%d:details", attemptID)

	// 1. Check cache first
	if c.redisClient != nil {
		cached, err := c.redisClient.Get(ctx, cacheKey).Result()
		if err == nil && cached != "" {
			var details AttemptDetails
			if err := json.Unmarshal([]byte(cached), &details); err == nil {
				c.logger.Debug("Cache hit for attempt details",
					zap.Uint64("attempt_id", attemptID),
					zap.String("creator_id", details.CreatorID))
				return &details, nil
			}
		}
	}

	// 2. Cache miss - call API
	url := fmt.Sprintf("%s/api/v1/attempts/%d/details", c.baseURL, attemptID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
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
		return nil, fmt.Errorf("API call failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		c.logger.Warn("Assessment-service returned non-OK status",
			zap.Uint64("attempt_id", attemptID),
			zap.Int("status_code", resp.StatusCode))
		return nil, fmt.Errorf("API returned status %d", resp.StatusCode)
	}

	var info AttemptInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	creatorID := info.Assessment.CreatedBy
	if creatorID == "" {
		c.logger.Warn("Assessment has no creator_id",
			zap.Uint64("attempt_id", attemptID),
			zap.Uint("assessment_id", info.AssessmentID))
		return nil, fmt.Errorf("assessment has no creator")
	}

	details := &AttemptDetails{
		CreatorID:       creatorID,
		StudentUsername: info.Student.FullName,
	}

	// 3. Store in cache (async to not block)
	if c.redisClient != nil {
		go func() {
			cacheCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			detailsJSON, _ := json.Marshal(details)
			if err := c.redisClient.Set(cacheCtx, cacheKey, string(detailsJSON), c.cacheTTL).Err(); err != nil {
				c.logger.Warn("Failed to cache attempt details",
					zap.Uint64("attempt_id", attemptID),
					zap.Error(err))
			}
		}()
	}

	c.logger.Info("Fetched attempt details from API",
		zap.Uint64("attempt_id", attemptID),
		zap.String("creator_id", creatorID),
		zap.String("student_name", details.StudentUsername))

	return details, nil
}

// GetAttemptCreator is a convenience method that returns only the creator ID
// Deprecated: Use GetAttemptDetails for full info
func (c *AssessmentClient) GetAttemptCreator(ctx context.Context, attemptID uint64) (string, error) {
	details, err := c.GetAttemptDetails(ctx, attemptID)
	if err != nil {
		return "", err
	}
	return details.CreatorID, nil
}

// InvalidateCache removes cached details for an attempt
func (c *AssessmentClient) InvalidateCache(ctx context.Context, attemptID uint64) error {
	if c.redisClient == nil {
		return nil
	}
	cacheKey := fmt.Sprintf("attempt:%d:details", attemptID)
	return c.redisClient.Del(ctx, cacheKey).Err()
}
