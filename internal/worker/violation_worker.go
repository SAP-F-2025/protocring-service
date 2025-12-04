package worker

import (
	"context"
	"fmt"
	"protocring-service/internal/model"
	"protocring-service/internal/repository"
	"protocring-service/pkg/streams"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// ViolationWorker processes violation messages from Redis Stream
type ViolationWorker struct {
	id           string
	consumer     *streams.Consumer
	accumulator  *BatchAccumulator
	errorHandler *ErrorHandler
	repo         repository.ViolationRepositoryInterface
	logger       *zap.Logger
	metrics      *WorkerMetrics
	stopChan     chan struct{}
	doneChan     chan struct{}
}

// NewViolationWorker creates a new violation worker
func NewViolationWorker(
	id string,
	consumer *streams.Consumer,
	batchSize int,
	flushInterval time.Duration,
	repo repository.ViolationRepositoryInterface,
	errorHandler *ErrorHandler,
	logger *zap.Logger,
) *ViolationWorker {
	metrics := NewWorkerMetrics()

	// Create flush function for batch accumulator
	flushFunc := func(ctx context.Context, violations []*model.ViolationLog) error {
		if len(violations) == 0 {
			return nil
		}

		// Use repository batch insert
		err := repo.InsertBatch(ctx, violations)
		if err != nil {
			return fmt.Errorf("failed to insert batch: %w", err)
		}

		// Record metrics
		metrics.RecordFlush(len(violations))
		for range violations {
			metrics.IncrementSucceeded()
		}

		return nil
	}

	accumulator := NewBatchAccumulator(batchSize, flushInterval, flushFunc, logger)

	return &ViolationWorker{
		id:           id,
		consumer:     consumer,
		accumulator:  accumulator,
		errorHandler: errorHandler,
		repo:         repo,
		logger:       logger.With(zap.String("worker_id", id)),
		metrics:      metrics,
		stopChan:     make(chan struct{}),
		doneChan:     make(chan struct{}),
	}
}

// Start starts the worker
func (vw *ViolationWorker) Start(ctx context.Context) error {
	vw.logger.Info("Worker starting")

	// Start flush ticker for time-based flushing
	flushTicker := time.NewTicker(time.Second)
	defer flushTicker.Stop()

	// Start consumer in goroutine
	consumerCtx, consumerCancel := context.WithCancel(ctx)
	defer consumerCancel()

	consumerErrChan := make(chan error, 1)
	go func() {
		consumerErrChan <- vw.consumer.Start(consumerCtx)
	}()

	for {
		select {
		case <-ctx.Done():
			vw.logger.Info("Worker stopping due to context cancellation")
			close(vw.doneChan)
			return ctx.Err()

		case <-vw.stopChan:
			vw.logger.Info("Worker stopping due to stop signal")
			close(vw.doneChan)
			return nil

		case err := <-consumerErrChan:
			if err != nil && err != context.Canceled {
				vw.logger.Error("Consumer error", zap.Error(err))
				close(vw.doneChan)
				return err
			}
			close(vw.doneChan)
			return nil

		case <-flushTicker.C:
			// Periodic check for time-based flush
			if vw.accumulator.ShouldFlush() {
				if err := vw.accumulator.Flush(ctx); err != nil {
					vw.logger.Error("Failed to flush on timer", zap.Error(err))
					vw.metrics.RecordError(err)
				}
			}
		}
	}
}

// Stop stops the worker gracefully
func (vw *ViolationWorker) Stop(ctx context.Context) error {
	vw.logger.Info("Worker shutdown initiated")

	// Signal stop
	close(vw.stopChan)

	// Wait for worker to finish or timeout
	select {
	case <-vw.doneChan:
		vw.logger.Info("Worker stopped gracefully")
	case <-ctx.Done():
		vw.logger.Warn("Worker shutdown timeout")
		return ctx.Err()
	}

	// Flush any pending batches
	vw.logger.Info("Flushing pending batch on shutdown",
		zap.Int("pending", vw.accumulator.GetBatchSize()),
	)

	if err := vw.accumulator.Flush(ctx); err != nil {
		vw.logger.Error("Failed to flush on shutdown", zap.Error(err))
		return err
	}

	vw.logger.Info("Worker shutdown complete")
	return nil
}

// GetMetrics returns the worker's metrics
func (vw *ViolationWorker) GetMetrics() *WorkerMetrics {
	return vw.metrics
}

// handleMessage processes a single message from the stream
func (vw *ViolationWorker) handleMessage(ctx context.Context, msg redis.XMessage) error {
	vw.metrics.IncrementProcessed()

	// Deserialize message payload
	var violation model.ViolationLog
	if err := streams.UnmarshalJSON(msg, &violation); err != nil {
		vw.logger.Error("Failed to unmarshal violation",
			zap.String("message_id", msg.ID),
			zap.Error(err),
		)

		// Unretryable error - acknowledge to remove from stream
		if IsUnretryableError(err) {
			vw.metrics.IncrementFailed()
			return nil // Return nil to ack the message
		}

		vw.metrics.IncrementFailed()
		return err
	}

	// Add to batch accumulator
	shouldFlush := vw.accumulator.Add(&violation)

	// Flush if threshold reached
	if shouldFlush {
		if err := vw.accumulator.Flush(ctx); err != nil {
			vw.logger.Error("Failed to flush batch",
				zap.String("message_id", msg.ID),
				zap.Error(err),
			)
			vw.metrics.RecordError(err)
			return err
		}
	}

	return nil
}

// GetID returns the worker ID
func (vw *ViolationWorker) GetID() string {
	return vw.id
}
