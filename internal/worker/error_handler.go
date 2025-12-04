package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	// DLQStreamName is the stream name for dead letter queue
	DLQStreamName = "violations:dlq"

	// RetryKeyPrefix is the prefix for retry counter keys in Redis
	RetryKeyPrefix = "violation:retry:"
)

// ErrorHandler handles errors for failed message processing
type ErrorHandler struct {
	redisClient *redis.Client
	maxRetries  int
	logger      *zap.Logger
}

// NewErrorHandler creates a new error handler
func NewErrorHandler(redisClient *redis.Client, maxRetries int, logger *zap.Logger) *ErrorHandler {
	return &ErrorHandler{
		redisClient: redisClient,
		maxRetries:  maxRetries,
		logger:      logger,
	}
}

// ShouldRetry checks if a message should be retried
// Returns true if retry count is below max retries
func (eh *ErrorHandler) ShouldRetry(ctx context.Context, messageID string) (bool, int, error) {
	retryKey := RetryKeyPrefix + messageID
	retryCount, err := eh.redisClient.Get(ctx, retryKey).Int()

	if err != nil {
		if err == redis.Nil {
			// First failure, no retry count yet
			return true, 0, nil
		}
		return false, 0, fmt.Errorf("failed to get retry count: %w", err)
	}

	return retryCount < eh.maxRetries, retryCount, nil
}

// IncrementRetryCount increments the retry counter for a message
func (eh *ErrorHandler) IncrementRetryCount(ctx context.Context, messageID string) (int, error) {
	retryKey := RetryKeyPrefix + messageID

	// Increment with expiration (24 hours) to prevent memory leak
	retryCount, err := eh.redisClient.Incr(ctx, retryKey).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to increment retry count: %w", err)
	}

	// Set expiration on first increment
	if retryCount == 1 {
		eh.redisClient.Expire(ctx, retryKey, 24*time.Hour)
	}

	return int(retryCount), nil
}

// GetRetryBackoff calculates exponential backoff duration based on retry count
// Returns: 1s, 2s, 4s for retries 1, 2, 3
func (eh *ErrorHandler) GetRetryBackoff(retryCount int) time.Duration {
	if retryCount <= 0 {
		return 1 * time.Second
	}
	if retryCount >= 3 {
		return 4 * time.Second
	}

	backoff := time.Duration(1<<uint(retryCount-1)) * time.Second
	return backoff
}

// MoveToDLQ moves a failed message to the dead letter queue
func (eh *ErrorHandler) MoveToDLQ(ctx context.Context, msg redis.XMessage, originalStream string, err error) error {
	// Prepare DLQ message with metadata
	dlqPayload := map[string]interface{}{
		"original_stream":     originalStream,
		"original_message_id": msg.ID,
		"original_payload":    msg.Values,
		"error_message":       err.Error(),
		"failed_at":           time.Now().Unix(),
	}

	payloadJSON, marshalErr := json.Marshal(dlqPayload)
	if marshalErr != nil {
		eh.logger.Error("Failed to marshal DLQ payload",
			zap.String("message_id", msg.ID),
			zap.Error(marshalErr),
		)
		return fmt.Errorf("failed to marshal DLQ payload: %w", marshalErr)
	}

	// Add to DLQ stream
	_, err = eh.redisClient.XAdd(ctx, &redis.XAddArgs{
		Stream: DLQStreamName,
		Values: map[string]interface{}{
			"payload":   string(payloadJSON),
			"timestamp": time.Now().Unix(),
		},
	}).Result()

	if err != nil {
		eh.logger.Error("Failed to add message to DLQ",
			zap.String("message_id", msg.ID),
			zap.Error(err),
		)
		return fmt.Errorf("failed to add to DLQ: %w", err)
	}

	// Clean up retry counter
	retryKey := RetryKeyPrefix + msg.ID
	eh.redisClient.Del(ctx, retryKey)

	eh.logger.Warn("Message moved to DLQ",
		zap.String("message_id", msg.ID),
		zap.String("original_stream", originalStream),
		zap.String("error", err.Error()),
	)

	return nil
}

// HandleError handles an error for a message
// Returns true if message should be acknowledged (retry exhausted or unretryable error)
func (eh *ErrorHandler) HandleError(ctx context.Context, msg redis.XMessage, stream string, err error) bool {
	shouldRetry, retryCount, checkErr := eh.ShouldRetry(ctx, msg.ID)
	if checkErr != nil {
		eh.logger.Error("Failed to check retry status",
			zap.String("message_id", msg.ID),
			zap.Error(checkErr),
		)
		// Don't ack on Redis errors - let message be redelivered
		return false
	}

	if !shouldRetry {
		// Max retries exhausted, move to DLQ
		eh.logger.Warn("Max retries exhausted, moving to DLQ",
			zap.String("message_id", msg.ID),
			zap.Int("retry_count", retryCount),
		)

		if dlqErr := eh.MoveToDLQ(ctx, msg, stream, err); dlqErr != nil {
			// Failed to move to DLQ, don't ack - let it be redelivered
			return false
		}

		// Successfully moved to DLQ, can acknowledge
		return true
	}

	// Increment retry count
	newRetryCount, incrErr := eh.IncrementRetryCount(ctx, msg.ID)
	if incrErr != nil {
		eh.logger.Error("Failed to increment retry count",
			zap.String("message_id", msg.ID),
			zap.Error(incrErr),
		)
		// Don't ack - let message be redelivered
		return false
	}

	backoff := eh.GetRetryBackoff(newRetryCount)

	eh.logger.Info("Message will be retried",
		zap.String("message_id", msg.ID),
		zap.Int("retry_count", newRetryCount),
		zap.Int("max_retries", eh.maxRetries),
		zap.Duration("backoff", backoff),
	)

	// Don't acknowledge - message will be redelivered by Redis
	return false
}

// IsUnretryableError checks if an error should not be retried
// Examples: JSON parse errors, validation errors
func IsUnretryableError(err error) bool {
	if err == nil {
		return false
	}

	// JSON unmarshal errors are unretryable
	if _, ok := err.(*json.UnmarshalTypeError); ok {
		return true
	}
	if _, ok := err.(*json.SyntaxError); ok {
		return true
	}

	// Add more unretryable error types as needed

	return false
}
