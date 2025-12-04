package worker

import (
	"sync/atomic"
	"time"
)

// WorkerMetrics tracks performance and health metrics for a worker
type WorkerMetrics struct {
	MessagesProcessed atomic.Int64
	MessagesSucceeded atomic.Int64
	MessagesFailed    atomic.Int64
	BatchesFlushed    atomic.Int64
	LastFlushTime     time.Time
	LastFlushSize     int
	ProcessingErrors  atomic.Int64
	LastError         error
	LastErrorTime     time.Time
}

// NewWorkerMetrics creates a new worker metrics instance
func NewWorkerMetrics() *WorkerMetrics {
	return &WorkerMetrics{}
}

// IncrementProcessed increments the messages processed counter
func (m *WorkerMetrics) IncrementProcessed() {
	m.MessagesProcessed.Add(1)
}

// IncrementSucceeded increments the messages succeeded counter
func (m *WorkerMetrics) IncrementSucceeded() {
	m.MessagesSucceeded.Add(1)
}

// IncrementFailed increments the messages failed counter
func (m *WorkerMetrics) IncrementFailed() {
	m.MessagesFailed.Add(1)
}

// IncrementBatchesFlushed increments the batches flushed counter
func (m *WorkerMetrics) IncrementBatchesFlushed() {
	m.BatchesFlushed.Add(1)
}

// IncrementErrors increments the processing errors counter
func (m *WorkerMetrics) IncrementErrors() {
	m.ProcessingErrors.Add(1)
}

// RecordFlush records metrics for a batch flush operation
func (m *WorkerMetrics) RecordFlush(size int) {
	m.LastFlushTime = time.Now()
	m.LastFlushSize = size
	m.IncrementBatchesFlushed()
}

// RecordError records an error that occurred during processing
func (m *WorkerMetrics) RecordError(err error) {
	m.LastError = err
	m.LastErrorTime = time.Now()
	m.IncrementErrors()
}

// GetAverageBatchSize calculates the average batch size
func (m *WorkerMetrics) GetAverageBatchSize() float64 {
	batches := m.BatchesFlushed.Load()
	if batches == 0 {
		return 0
	}
	succeeded := m.MessagesSucceeded.Load()
	return float64(succeeded) / float64(batches)
}

// GetStats returns a snapshot of the metrics
func (m *WorkerMetrics) GetStats() MetricsSnapshot {
	return MetricsSnapshot{
		MessagesProcessed: m.MessagesProcessed.Load(),
		MessagesSucceeded: m.MessagesSucceeded.Load(),
		MessagesFailed:    m.MessagesFailed.Load(),
		BatchesFlushed:    m.BatchesFlushed.Load(),
		LastFlushTime:     m.LastFlushTime,
		LastFlushSize:     m.LastFlushSize,
		AverageBatchSize:  m.GetAverageBatchSize(),
		ProcessingErrors:  m.ProcessingErrors.Load(),
		LastError:         m.LastError,
		LastErrorTime:     m.LastErrorTime,
	}
}

// MetricsSnapshot represents a point-in-time snapshot of metrics
type MetricsSnapshot struct {
	MessagesProcessed int64
	MessagesSucceeded int64
	MessagesFailed    int64
	BatchesFlushed    int64
	LastFlushTime     time.Time
	LastFlushSize     int
	AverageBatchSize  float64
	ProcessingErrors  int64
	LastError         error
	LastErrorTime     time.Time
}

// AggregatedMetrics aggregates metrics from multiple workers
type AggregatedMetrics struct {
	TotalMessagesProcessed int64
	TotalMessagesSucceeded int64
	TotalMessagesFailed    int64
	TotalBatchesFlushed    int64
	TotalProcessingErrors  int64
	AverageBatchSize       float64
	WorkersCount           int
	Workers                []WorkerMetricsSnapshot
}

// WorkerMetricsSnapshot represents metrics for a single worker
type WorkerMetricsSnapshot struct {
	ID                string
	MessagesProcessed int64
	LastFlushTime     time.Time
	LastFlushSize     int
}
