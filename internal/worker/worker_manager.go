package worker

import (
	"context"
	"fmt"
	"protocring-service/internal/config"
	"protocring-service/internal/repository"
	"protocring-service/pkg/streams"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// WorkerManager orchestrates multiple violation workers
type WorkerManager struct {
	config       *config.WorkerConfig
	workers      []*ViolationWorker
	repo         repository.ViolationRepositoryInterface
	redisClient  *redis.Client
	logger       *zap.Logger
	wg           sync.WaitGroup
	shutdownChan chan struct{}
}

// NewWorkerManager creates a new worker manager
func NewWorkerManager(
	cfg *config.Config,
	repo repository.ViolationRepositoryInterface,
	redisClient *redis.Client,
	logger *zap.Logger,
) *WorkerManager {
	return &WorkerManager{
		config:       &cfg.Worker,
		repo:         repo,
		redisClient:  redisClient,
		logger:       logger,
		shutdownChan: make(chan struct{}),
	}
}

// Start starts all workers
func (wm *WorkerManager) Start(ctx context.Context) error {
	if !wm.config.Enabled {
		wm.logger.Info("Workers disabled by configuration")
		return nil
	}

	wm.logger.Info("Starting worker manager",
		zap.Int("num_workers", wm.config.NumWorkers),
		zap.String("stream", wm.config.StreamName),
		zap.String("consumer_group", wm.config.ConsumerGroup),
		zap.Int("batch_size", wm.config.BatchSize),
		zap.Duration("flush_interval", wm.config.FlushInterval),
	)

	// Create workers
	workers, err := wm.createWorkers()
	if err != nil {
		return fmt.Errorf("failed to create workers: %w", err)
	}
	wm.workers = workers

	// Start each worker in a goroutine
	for _, worker := range wm.workers {
		wm.wg.Add(1)
		go func(w *ViolationWorker) {
			defer wm.wg.Done()

			if err := w.Start(ctx); err != nil {
				wm.logger.Error("Worker failed",
					zap.String("worker_id", w.GetID()),
					zap.Error(err),
				)
			}
		}(worker)
	}

	wm.logger.Info("All workers started successfully")
	return nil
}

// Shutdown stops all workers gracefully
func (wm *WorkerManager) Shutdown(ctx context.Context) error {
	if !wm.config.Enabled {
		return nil
	}

	wm.logger.Info("Shutting down worker manager")

	// Signal shutdown
	close(wm.shutdownChan)

	// Stop all workers with timeout
	shutdownCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var shutdownWg sync.WaitGroup
	shutdownErrors := make([]error, 0)
	errorsMutex := sync.Mutex{}

	for _, worker := range wm.workers {
		shutdownWg.Add(1)
		go func(w *ViolationWorker) {
			defer shutdownWg.Done()

			if err := w.Stop(shutdownCtx); err != nil {
				errorsMutex.Lock()
				shutdownErrors = append(shutdownErrors, fmt.Errorf("worker %s: %w", w.GetID(), err))
				errorsMutex.Unlock()
			}
		}(worker)
	}

	// Wait for all workers to stop
	shutdownWg.Wait()

	// Wait for worker goroutines
	wm.wg.Wait()

	if len(shutdownErrors) > 0 {
		wm.logger.Error("Some workers failed to shutdown gracefully",
			zap.Int("error_count", len(shutdownErrors)),
		)
		return fmt.Errorf("shutdown errors: %v", shutdownErrors)
	}

	wm.logger.Info("Worker manager shutdown complete")
	return nil
}

// GetMetrics returns aggregated metrics from all workers
func (wm *WorkerManager) GetMetrics() *AggregatedMetrics {
	if !wm.config.Enabled || len(wm.workers) == 0 {
		return &AggregatedMetrics{
			WorkersCount: 0,
			Workers:      []WorkerMetricsSnapshot{},
		}
	}

	agg := &AggregatedMetrics{
		WorkersCount: len(wm.workers),
		Workers:      make([]WorkerMetricsSnapshot, 0, len(wm.workers)),
	}

	for _, worker := range wm.workers {
		stats := worker.GetMetrics().GetStats()

		agg.TotalMessagesProcessed += stats.MessagesProcessed
		agg.TotalMessagesSucceeded += stats.MessagesSucceeded
		agg.TotalMessagesFailed += stats.MessagesFailed
		agg.TotalBatchesFlushed += stats.BatchesFlushed
		agg.TotalProcessingErrors += stats.ProcessingErrors

		agg.Workers = append(agg.Workers, WorkerMetricsSnapshot{
			ID:                worker.GetID(),
			MessagesProcessed: stats.MessagesProcessed,
			LastFlushTime:     stats.LastFlushTime,
			LastFlushSize:     stats.LastFlushSize,
		})
	}

	// Calculate average batch size
	if agg.TotalBatchesFlushed > 0 {
		agg.AverageBatchSize = float64(agg.TotalMessagesSucceeded) / float64(agg.TotalBatchesFlushed)
	}

	return agg
}

// GetStreamInfo returns information about the Redis stream
func (wm *WorkerManager) GetStreamInfo(ctx context.Context) (map[string]interface{}, error) {
	if !wm.config.Enabled {
		return map[string]interface{}{"enabled": false}, nil
	}

	// Get stream length
	streamLen, err := wm.redisClient.XLen(ctx, wm.config.StreamName).Result()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("failed to get stream length: %w", err)
	}

	// Get pending messages count
	pending, err := wm.redisClient.XPending(ctx, wm.config.StreamName, wm.config.ConsumerGroup).Result()
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("failed to get pending info: %w", err)
	}

	info := map[string]interface{}{
		"enabled":          true,
		"stream_name":      wm.config.StreamName,
		"stream_length":    streamLen,
		"pending_messages": int64(0),
	}

	if pending != nil {
		info["pending_messages"] = pending.Count
	}

	return info, nil
}

// createWorkers creates worker instances
func (wm *WorkerManager) createWorkers() ([]*ViolationWorker, error) {
	errorHandler := NewErrorHandler(wm.redisClient, wm.config.MaxRetries, wm.logger)
	workers := make([]*ViolationWorker, 0, wm.config.NumWorkers)

	for i := 0; i < wm.config.NumWorkers; i++ {
		workerID := fmt.Sprintf("worker-%d", i)

		// Create worker first (consumer will be created after)
		worker := NewViolationWorker(
			workerID,
			nil, // Will be set below
			wm.config.BatchSize,
			wm.config.FlushInterval,
			wm.repo,
			errorHandler,
			wm.logger,
		)

		// Create consumer with worker's handler
		consumer := streams.NewConsumer(wm.redisClient, wm.logger, streams.ConsumerConfig{
			Group:     wm.config.ConsumerGroup,
			Consumer:  workerID,
			Streams:   []string{wm.config.StreamName},
			BatchSize: int64(wm.config.BatchSize),
			BlockTime: 5 * time.Second,
			Handler:   worker.handleMessage,
			ErrorHandler: func(ctx context.Context, msg redis.XMessage, err error) {
				wm.logger.Error("Message processing error",
					zap.String("worker_id", workerID),
					zap.String("message_id", msg.ID),
					zap.Error(err),
				)

				// Handle error with retry logic
				shouldAck := errorHandler.HandleError(ctx, msg, wm.config.StreamName, err)
				if shouldAck {
					// Acknowledge to remove from stream (moved to DLQ or unretryable)
					if ackErr := worker.consumer.Ack(ctx, wm.config.StreamName, msg.ID); ackErr != nil {
						wm.logger.Error("Failed to ack message",
							zap.String("message_id", msg.ID),
							zap.Error(ackErr),
						)
					}
				}
			},
		})

		// Update worker with consumer
		worker.consumer = consumer

		workers = append(workers, worker)
	}

	return workers, nil
}
