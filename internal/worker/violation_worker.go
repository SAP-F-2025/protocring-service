package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"protocring-service/internal/config"
	"protocring-service/internal/dto"
	"protocring-service/internal/model"
	"protocring-service/internal/repository"
	"protocring-service/pkg/streams"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// ViolationWorker handles consuming violations from Redis Stream and inserting to DB
type ViolationWorker struct {
	config      *config.WorkerConfig
	redisClient *redis.Client
	producer    *streams.Producer // For DLQ
	repo        repository.ViolationRepositoryInterface
	logger      *zap.Logger

	wg       sync.WaitGroup
	shutdown chan struct{}
}

// Start starts the worker
func (w *ViolationWorker) Start(ctx context.Context) error {
	w.logger.Info("Starting violation workers",
		zap.String("stream", w.config.StreamName),
		zap.String("consumer_group", w.config.ConsumerGroup),
		zap.Int("num_workers", w.config.NumWorkers),
	)

	// Create consumer group once (before starting workers)
	// Call Redis directly instead of creating temp consumer
	err := w.redisClient.XGroupCreateMkStream(ctx, w.config.StreamName, w.config.ConsumerGroup, ">").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return fmt.Errorf("failed to create consumer group: %w", err)
	}

	hostname, _ := os.Hostname()

	// Start multiple workers, each with its own consumer
	for i := 0; i < w.config.NumWorkers; i++ {
		w.wg.Add(1)

		// Create unique consumer for this worker
		consumerName := fmt.Sprintf("worker-%s-%d-%d", hostname, os.Getpid(), i)
		consumer := streams.NewConsumer(w.redisClient, w.logger, streams.ConsumerConfig{
			Group:        w.config.ConsumerGroup,
			Consumer:     consumerName,
			Streams:      []string{w.config.StreamName},
			BatchSize:    w.config.BatchSize,
			BlockTime:    w.config.BlockTime,
			Handler:      w.processMessage,
			ErrorHandler: w.handleError,
		})

		go func(workerID int, cons *streams.Consumer) {
			defer w.wg.Done()
			w.runWorker(ctx, workerID, cons)
		}(i, consumer)
	}

	w.logger.Info("Violation workers started successfully")
	return nil
}

// runWorker runs a single worker goroutine with its own consumer
func (w *ViolationWorker) runWorker(ctx context.Context, workerID int, consumer *streams.Consumer) {
	logger := w.logger.With(zap.Int("worker_id", workerID))
	logger.Info("Worker goroutine started")

	if err := consumer.Start(ctx); err != nil {
		if !errors.Is(err, context.Canceled) {
			logger.Error("Worker stopped with error", zap.Error(err))
		}
	}

	logger.Info("Worker goroutine stopped")
}

// processMessage processes a single violation message
func (w *ViolationWorker) processMessage(ctx context.Context, msg redis.XMessage) error {
	startTime := time.Now()

	// Extract payload
	payloadStr, ok := msg.Values["payload"].(string)
	if !ok {
		return fmt.Errorf("payload field not found or not a string")
	}

	// Deserialize violation request
	var req dto.CreateViolationRequest
	if err := json.Unmarshal([]byte(payloadStr), &req); err != nil {
		return fmt.Errorf("failed to unmarshal violation: %w", err)
	}

	// Convert to model
	violation := w.dtoToModel(&req)

	// Get retry count from message
	retryCount := w.getRetryCount(msg)

	// Insert to database with retry
	if err := w.insertWithRetry(ctx, violation, retryCount); err != nil {
		// Send to DLQ if max retries exceeded
		if retryCount >= w.config.RetryAttempts {
			w.logger.Error("Max retries exceeded, sending to DLQ",
				zap.Uint64("attempt_id", violation.AttemptID),
				zap.Int("retry_count", retryCount),
				zap.Error(err),
			)

			if dlqErr := w.sendToDLQ(ctx, msg, err); dlqErr != nil {
				w.logger.Error("Failed to send message to DLQ", zap.Error(dlqErr))
			}

			// Return nil to ACK the message (it's in DLQ now)
			return nil
		}

		// Return error to trigger retry
		return fmt.Errorf("failed to insert violation (attempt %d/%d): %w",
			retryCount+1, w.config.RetryAttempts, err)
	}

	duration := time.Since(startTime)
	w.logger.Info("Violation processed successfully",
		zap.Uint64("violation_id", violation.ID),
		zap.Uint64("attempt_id", violation.AttemptID),
		zap.Int("violation_type", violation.ViolationType),
		zap.Duration("duration", duration),
	)

	return nil
}

// insertWithRetry attempts to insert violation with exponential backoff
func (w *ViolationWorker) insertWithRetry(ctx context.Context, violation *model.ViolationLog, retryCount int) error {
	var err error
	backoff := time.Duration(1<<uint(retryCount)) * time.Second // Exponential backoff: 1s, 2s, 4s...

	if retryCount > 0 {
		w.logger.Warn("Retrying insert",
			zap.Uint64("attempt_id", violation.AttemptID),
			zap.Int("retry_count", retryCount),
			zap.Duration("backoff", backoff),
		)

		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	err = w.repo.Insert(ctx, violation)
	if err != nil {
		return fmt.Errorf("database insert failed: %w", err)
	}

	return nil
}

// sendToDLQ sends a failed message to the Dead Letter Queue
func (w *ViolationWorker) sendToDLQ(ctx context.Context, msg redis.XMessage, originalErr error) error {
	dlqData := map[string]interface{}{
		"payload":       msg.Values["payload"],
		"original_id":   msg.ID,
		"timestamp":     msg.Values["timestamp"],
		"error":         originalErr.Error(),
		"retry_count":   w.getRetryCount(msg),
		"dlq_timestamp": time.Now().Unix(),
	}

	_, err := w.producer.Publish(ctx, w.config.DLQStream, dlqData, nil)
	return err
}

// getRetryCount gets the actual delivery count from Redis XPENDING
// This is the true retry count, not stored in message data
func (w *ViolationWorker) getRetryCount(msg redis.XMessage) int {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Get pending info for this specific message
	pending, err := w.redisClient.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: w.config.StreamName,
		Group:  w.config.ConsumerGroup,
		Start:  msg.ID,
		End:    msg.ID,
		Count:  1,
	}).Result()

	if err != nil || len(pending) == 0 {
		// If can't get pending info, assume first attempt
		return 0
	}

	// DeliveryCount - 1 = retry count (delivery 1 = attempt 0, delivery 2 = retry 1, etc)
	return int(pending[0].RetryCount)
}

// handleError handles errors during message processing
func (w *ViolationWorker) handleError(ctx context.Context, msg redis.XMessage, err error) {
	w.logger.Error("Error processing message",
		zap.String("message_id", msg.ID),
		zap.Error(err),
	)

	// Message will be retried automatically by Redis Streams
	// Consumer won't ACK failed messages, they stay in pending state
}

// dtoToModel converts DTO to model
func (w *ViolationWorker) dtoToModel(req *dto.CreateViolationRequest) *model.ViolationLog {
	return &model.ViolationLog{
		AttemptID:         req.AttemptID,
		UserID:            req.UserID,
		AssessmentID:      req.AssessmentID,
		ViolationType:     req.ViolationType,
		Severity:          req.Severity,
		ConfidenceScore:   req.ConfidenceScore,
		SnapshotURL:       req.SnapshotURL,
		BrowserInfo:       req.BrowserInfo,
		DeviceFingerprint: req.DeviceFingerprint,
		CreatedAt:         req.CreatedAt,
		EndedAt:           req.EndedAt,
		IsProlonged:       req.IsProlonged,
	}
}

// Shutdown gracefully shuts down the worker
func (w *ViolationWorker) Shutdown(ctx context.Context) error {
	w.logger.Info("Shutting down violation worker...")

	close(w.shutdown)

	// Wait for all workers to finish with timeout
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		w.logger.Info("Violation worker shut down gracefully")
		return nil
	case <-ctx.Done():
		w.logger.Warn("Violation worker shutdown timed out")
		return ctx.Err()
	}
}
