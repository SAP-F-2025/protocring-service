package worker

import (
	"context"
	"protocring-service/internal/model"
	"sync"
	"time"

	"go.uber.org/zap"
)

// FlushFunc is a function that flushes a batch of violations to the database
type FlushFunc func(ctx context.Context, violations []*model.ViolationLog) error

// BatchAccumulator accumulates violations and triggers flushes based on size or time
type BatchAccumulator struct {
	batch         []*model.ViolationLog
	batchSize     int
	flushInterval time.Duration
	lastFlush     time.Time
	mutex         sync.RWMutex
	flushFunc     FlushFunc
	logger        *zap.Logger
}

// NewBatchAccumulator creates a new batch accumulator
func NewBatchAccumulator(batchSize int, flushInterval time.Duration, flushFunc FlushFunc, logger *zap.Logger) *BatchAccumulator {
	return &BatchAccumulator{
		batch:         make([]*model.ViolationLog, 0, batchSize),
		batchSize:     batchSize,
		flushInterval: flushInterval,
		lastFlush:     time.Now(),
		flushFunc:     flushFunc,
		logger:        logger,
	}
}

// Add adds a violation to the batch
// Returns true if the batch should be flushed after this addition
func (ba *BatchAccumulator) Add(violation *model.ViolationLog) bool {
	ba.mutex.Lock()
	defer ba.mutex.Unlock()

	ba.batch = append(ba.batch, violation)

	return ba.shouldFlushLocked()
}

// ShouldFlush checks if the batch should be flushed
func (ba *BatchAccumulator) ShouldFlush() bool {
	ba.mutex.RLock()
	defer ba.mutex.RUnlock()

	return ba.shouldFlushLocked()
}

// shouldFlushLocked checks if the batch should be flushed (must be called with lock held)
func (ba *BatchAccumulator) shouldFlushLocked() bool {
	// Flush if batch size reached
	if len(ba.batch) >= ba.batchSize {
		return true
	}

	// Flush if flush interval elapsed and there are pending violations
	if len(ba.batch) > 0 && time.Since(ba.lastFlush) >= ba.flushInterval {
		return true
	}

	return false
}

// Flush flushes the current batch to the database
func (ba *BatchAccumulator) Flush(ctx context.Context) error {
	ba.mutex.Lock()

	// Check if there's anything to flush
	if len(ba.batch) == 0 {
		ba.mutex.Unlock()
		return nil
	}

	// Take ownership of current batch
	batchToFlush := ba.batch
	batchSize := len(batchToFlush)

	// Reset batch for new accumulation
	ba.batch = make([]*model.ViolationLog, 0, ba.batchSize)
	ba.lastFlush = time.Now()

	ba.mutex.Unlock()

	// Flush outside of lock to allow concurrent accumulation
	ba.logger.Debug("Flushing batch",
		zap.Int("size", batchSize),
	)

	start := time.Now()
	err := ba.flushFunc(ctx, batchToFlush)
	duration := time.Since(start)

	if err != nil {
		ba.logger.Error("Failed to flush batch",
			zap.Int("size", batchSize),
			zap.Duration("duration", duration),
			zap.Error(err),
		)

		// On error, we don't re-add to batch - violations will be redelivered by Redis
		return err
	}

	ba.logger.Info("Successfully flushed batch",
		zap.Int("size", batchSize),
		zap.Duration("duration", duration),
	)

	return nil
}

// GetBatchSize returns the current batch size
func (ba *BatchAccumulator) GetBatchSize() int {
	ba.mutex.RLock()
	defer ba.mutex.RUnlock()
	return len(ba.batch)
}

// GetPendingViolations returns a copy of pending violations (for testing/debugging)
func (ba *BatchAccumulator) GetPendingViolations() []*model.ViolationLog {
	ba.mutex.RLock()
	defer ba.mutex.RUnlock()

	pending := make([]*model.ViolationLog, len(ba.batch))
	copy(pending, ba.batch)
	return pending
}
