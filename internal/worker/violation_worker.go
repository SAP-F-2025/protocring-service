package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"protocring-service/internal/client"
	"protocring-service/internal/config"
	"protocring-service/internal/dto"
	"protocring-service/internal/events"
	"protocring-service/internal/model"
	"protocring-service/internal/repository"
	"protocring-service/pkg/streams"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// NotificationPublisherInterface defines the interface for notification publishing
type NotificationPublisherInterface interface {
	PublishViolation(ctx context.Context, event *events.ProctoringViolationEvent) error
}

// AssessmentClientInterface defines the interface for assessment service client
type AssessmentClientInterface interface {
	GetAttemptDetails(ctx context.Context, attemptID uint64) (*client.AttemptDetails, error)
}

// ViolationWorker handles consuming violations from Redis Stream and inserting to DB
type ViolationWorker struct {
	config      *config.WorkerConfig
	redisClient *redis.Client
	producer    *streams.Producer // For DLQ
	repo        repository.ViolationRepositoryInterface
	logger      *zap.Logger

	wg       sync.WaitGroup
	shutdown chan struct{}

	// Batch buffer for high-throughput insert
	batchMu     sync.Mutex
	batchBuffer []*batchItem
	flushTicker *time.Ticker

	// Notification components
	notifPublisher   NotificationPublisherInterface
	notifConfig      *config.NotificationConfig
	assessmentClient AssessmentClientInterface
	// Note: Cooldown is now handled via Redis (distributed, persistent)
}

// batchItem holds a violation and its message ID for batch processing
type batchItem struct {
	violation *model.ViolationLog
	msgID     string
}

// Start starts the worker
func (w *ViolationWorker) Start(ctx context.Context) error {
	w.logger.Info("Starting violation workers",
		zap.String("stream", w.config.StreamName),
		zap.String("consumer_group", w.config.ConsumerGroup),
		zap.Int("num_workers", w.config.NumWorkers),
		zap.Int("insert_batch_size", w.config.InsertBatchSize),
		zap.Duration("insert_flush_timeout", w.config.InsertFlushTimeout),
	)

	// Initialize batch buffer
	w.batchBuffer = make([]*batchItem, 0, w.config.InsertBatchSize)

	// Start flush ticker for time-based flush
	w.flushTicker = time.NewTicker(w.config.InsertFlushTimeout)
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.runFlushTicker(ctx)
	}()

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

	// Start dedicated retry worker if enabled
	if w.config.RetryWorkerEnabled {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			w.startRetryWorker(ctx)
		}()
	}

	w.logger.Info("Violation workers started successfully")
	return nil
}

// runFlushTicker periodically flushes the batch buffer
func (w *ViolationWorker) runFlushTicker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			// Final flush on shutdown
			w.flushBatch(context.Background())
			return
		case <-w.flushTicker.C:
			if err := w.flushBatch(ctx); err != nil {
				w.logger.Error("Failed to flush batch on ticker", zap.Error(err))
			}
		}
	}
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

// startRetryWorker runs a dedicated worker for claiming and reprocessing stale pending messages
func (w *ViolationWorker) startRetryWorker(ctx context.Context) {
	w.logger.Info("Starting dedicated retry worker",
		zap.Duration("check_interval", w.config.RetryCheckInterval),
		zap.Duration("min_idle_time", w.config.RetryMinIdleTime),
	)

	ticker := time.NewTicker(w.config.RetryCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("Retry worker stopped")
			return
		case <-ticker.C:
			w.processStaleMessages(ctx)
		}
	}
}

// processStaleMessages claims and reprocesses pending messages that have been idle for too long
func (w *ViolationWorker) processStaleMessages(ctx context.Context) {
	// Get pending messages
	pending, err := w.redisClient.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: w.config.StreamName,
		Group:  w.config.ConsumerGroup,
		Start:  "-",
		End:    "+",
		Count:  w.config.RetryBatchSize,
	}).Result()

	if err != nil {
		if !errors.Is(err, redis.Nil) {
			w.logger.Error("Failed to get pending messages", zap.Error(err))
		}
		return
	}

	if len(pending) == 0 {
		return
	}

	// Filter and claim idle messages
	staleCount := 0
	for _, p := range pending {
		if p.Idle < w.config.RetryMinIdleTime {
			continue
		}

		staleCount++

		// Claim the message
		hostname, _ := os.Hostname()
		claimed, err := w.redisClient.XClaim(ctx, &redis.XClaimArgs{
			Stream:   w.config.StreamName,
			Group:    w.config.ConsumerGroup,
			Consumer: fmt.Sprintf("retry-worker-%s-%d", hostname, os.Getpid()),
			MinIdle:  w.config.RetryMinIdleTime,
			Messages: []string{p.ID},
		}).Result()

		if err != nil {
			w.logger.Warn("Failed to claim stale message",
				zap.String("message_id", p.ID),
				zap.Int64("idle_ms", p.Idle.Milliseconds()),
				zap.Error(err),
			)
			continue
		}

		// Process claimed messages
		for _, msg := range claimed {
			w.logger.Info("Reprocessing stale message",
				zap.String("message_id", msg.ID),
				zap.Int64("idle_ms", p.Idle.Milliseconds()),
				zap.Int64("retry_count", p.RetryCount),
			)

			if err := w.processMessage(ctx, msg); err != nil {
				w.logger.Error("Failed to reprocess stale message",
					zap.String("message_id", msg.ID),
					zap.Error(err),
				)
				// Don't ACK, let it be retried again
			} else {
				// ACK successful reprocessing
				if ackErr := w.redisClient.XAck(ctx, w.config.StreamName, w.config.ConsumerGroup, msg.ID).Err(); ackErr != nil {
					w.logger.Error("Failed to ACK reprocessed message",
						zap.String("message_id", msg.ID),
						zap.Error(ackErr),
					)
				}
			}
		}
	}

	if staleCount > 0 {
		w.logger.Info("Retry worker processed stale messages",
			zap.Int("total_pending", len(pending)),
			zap.Int("stale_messages", staleCount),
		)
	}
}

// processMessage adds a violation to the batch buffer instead of immediate insert
func (w *ViolationWorker) processMessage(ctx context.Context, msg redis.XMessage) error {
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

	// Add to batch buffer
	w.batchMu.Lock()
	w.batchBuffer = append(w.batchBuffer, &batchItem{
		violation: violation,
		msgID:     msg.ID,
	})
	shouldFlush := len(w.batchBuffer) >= w.config.InsertBatchSize
	w.batchMu.Unlock()

	// Flush if buffer is full
	if shouldFlush {
		if err := w.flushBatch(ctx); err != nil {
			w.logger.Error("Failed to flush batch on size trigger", zap.Error(err))
			// Don't return error here - message is in buffer, will be retried on next flush
		}
	}

	// Return nil - ACK will happen in flushBatch after successful insert
	return nil
}

// flushBatch inserts all buffered violations and batch ACKs messages
func (w *ViolationWorker) flushBatch(ctx context.Context) error {
	w.batchMu.Lock()
	if len(w.batchBuffer) == 0 {
		w.batchMu.Unlock()
		return nil
	}

	// Take ownership of current buffer
	items := w.batchBuffer
	w.batchBuffer = make([]*batchItem, 0, w.config.InsertBatchSize)
	w.batchMu.Unlock()

	// Extract violations and message IDs
	violations := make([]*model.ViolationLog, len(items))
	msgIDs := make([]string, len(items))
	for i, item := range items {
		violations[i] = item.violation
		msgIDs[i] = item.msgID
	}

	startTime := time.Now()

	// Batch insert to DB
	if err := w.repo.InsertBatch(ctx, violations); err != nil {
		w.logger.Error("Batch insert failed, re-queuing items",
			zap.Int("count", len(violations)),
			zap.Error(err),
		)
		// Re-add items to buffer for retry
		w.batchMu.Lock()
		w.batchBuffer = append(items, w.batchBuffer...)
		w.batchMu.Unlock()
		return fmt.Errorf("batch insert failed: %w", err)
	}

	// Batch ACK all messages
	if err := w.redisClient.XAck(ctx, w.config.StreamName, w.config.ConsumerGroup, msgIDs...).Err(); err != nil {
		w.logger.Error("Failed to batch ACK messages",
			zap.Int("count", len(msgIDs)),
			zap.Error(err),
		)
		// Insert succeeded, so data is safe, but log the ACK failure
	}

	duration := time.Since(startTime)
	w.logger.Info("Batch flushed successfully",
		zap.Int("count", len(violations)),
		zap.Duration("duration", duration),
		zap.Float64("rate_per_sec", float64(len(violations))/duration.Seconds()),
	)

	// Publish notifications for high-severity violations (async)
	if w.notifConfig != nil && w.notifConfig.Enabled && w.notifPublisher != nil {
		for _, v := range violations {
			if v.Severity >= w.notifConfig.MinSeverity {
				go w.maybeNotifyViolation(context.Background(), v)
			}
		}
	}

	return nil
}

// insertWithRetry attempts to insert violation with exponential backoff (used by retry worker)
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

	// Stop flush ticker
	if w.flushTicker != nil {
		w.flushTicker.Stop()
	}

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

// ============================
// Notification Helper Functions
// ============================

// maybeNotifyViolation publishes a notification for a violation if not in cooldown
func (w *ViolationWorker) maybeNotifyViolation(ctx context.Context, v *model.ViolationLog) {
	// Try to acquire cooldown lock (atomic check-and-set)
	cooldownKey := fmt.Sprintf("%d_%d", v.AttemptID, v.ViolationType)
	if !w.tryAcquireCooldown(cooldownKey) {
		w.logger.Debug("Skipping notification, in cooldown",
			zap.Uint64("attempt_id", v.AttemptID),
			zap.Int("violation_type", v.ViolationType))
		return
	}

	// Get creator (teacher) and student info from assessment-service
	var details *client.AttemptDetails
	if w.assessmentClient != nil {
		var err error
		details, err = w.assessmentClient.GetAttemptDetails(ctx, v.AttemptID)
		if err != nil {
			w.logger.Warn("Failed to get attempt details",
				zap.Uint64("attempt_id", v.AttemptID),
				zap.Error(err))
			return
		}
	}

	if details == nil || details.CreatorID == "" {
		w.logger.Warn("No creator found for attempt, skipping notification",
			zap.Uint64("attempt_id", v.AttemptID))
		return
	}

	// Build and publish notification event
	event := &events.ProctoringViolationEvent{
		UserID:        v.UserID,
		Username:      details.StudentUsername, // Student's name from assessment-service
		SessionID:     strconv.FormatUint(v.AttemptID, 10),
		ViolationType: dto.GetViolationTypeName(v.ViolationType),
		Severity:      dto.GetSeverityName(v.Severity),
		ProctorIDs:    []string{details.CreatorID},
		Timestamp:     v.CreatedAt,
	}

	if err := w.notifPublisher.PublishViolation(ctx, event); err != nil {
		w.logger.Warn("Failed to publish violation notification",
			zap.Uint64("attempt_id", v.AttemptID),
			zap.Error(err))
		return
	}

	w.logger.Info("Violation notification published",
		zap.Uint64("attempt_id", v.AttemptID),
		zap.Int("violation_type", v.ViolationType),
		zap.String("severity", event.Severity),
		zap.String("creator_id", details.CreatorID),
		zap.String("student_name", details.StudentUsername))
}

// tryAcquireCooldown attempts to acquire a cooldown lock for the given key.
// Returns true if this is the first notification (cooldown acquired), false if in cooldown.
// Uses Redis SET NX EX for atomic check-and-set to prevent race conditions.
func (w *ViolationWorker) tryAcquireCooldown(key string) bool {
	if w.notifConfig == nil || w.notifConfig.CooldownSeconds <= 0 {
		return true // No cooldown configured, always allow
	}

	if w.redisClient == nil {
		return true // No Redis, allow (fallback)
	}

	cooldownKey := fmt.Sprintf("notif:cooldown:%s", key)
	ttl := time.Duration(w.notifConfig.CooldownSeconds) * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// SET NX EX: Set if Not Exists with Expiration (atomic operation)
	// Returns true if the key was set (first notification)
	// Returns false if the key already exists (in cooldown)
	success, err := w.redisClient.SetNX(ctx, cooldownKey, "1", ttl).Result()
	if err != nil {
		w.logger.Warn("Failed to acquire cooldown lock in Redis, allowing notification",
			zap.String("key", key),
			zap.Error(err))
		return true // On error, allow notification
	}

	if !success {
		w.logger.Debug("Notification in cooldown, skipping",
			zap.String("key", key))
	}

	return success
}
