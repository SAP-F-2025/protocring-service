package streams

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// Consumer handles consuming messages from Redis Streams
type Consumer struct {
	client       *redis.Client
	logger       *zap.Logger
	group        string
	consumer     string
	streams      []string
	batchSize    int64
	blockTime    time.Duration
	handler      MessageHandler
	errorHandler ErrorHandler
}

// MessageHandler is a function that processes stream messages
type MessageHandler func(ctx context.Context, msg redis.XMessage) error

// ErrorHandler is a function that handles errors during message processing
type ErrorHandler func(ctx context.Context, msg redis.XMessage, err error)

// ConsumerConfig represents consumer configuration
type ConsumerConfig struct {
	Group        string
	Consumer     string
	Streams      []string
	BatchSize    int64
	BlockTime    time.Duration
	Handler      MessageHandler
	ErrorHandler ErrorHandler
}

// NewConsumer creates a new stream consumer
func NewConsumer(client *redis.Client, logger *zap.Logger, config ConsumerConfig) *Consumer {
	if config.BatchSize == 0 {
		config.BatchSize = 10
	}
	if config.BlockTime == 0 {
		config.BlockTime = 5 * time.Second
	}

	return &Consumer{
		client:       client,
		logger:       logger,
		group:        config.Group,
		consumer:     config.Consumer,
		streams:      config.Streams,
		batchSize:    config.BatchSize,
		blockTime:    config.BlockTime,
		handler:      config.Handler,
		errorHandler: config.ErrorHandler,
	}
}

// CreateConsumerGroup creates a consumer group for a stream
func (c *Consumer) CreateConsumerGroup(ctx context.Context, stream, group string, startID string) error {
	if startID == "" {
		startID = "0" // Start from beginning
	}

	err := c.client.XGroupCreateMkStream(ctx, stream, group, startID).Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return fmt.Errorf("failed to create consumer group: %w", err)
	}

	return nil
}

// Start starts consuming messages from the streams
func (c *Consumer) Start(ctx context.Context) error {
	c.logger.Info("Starting consumer",
		zap.String("group", c.group),
		zap.String("consumer", c.consumer),
		zap.Strings("streams", c.streams),
	)

	// Create consumer groups for all streams
	for _, stream := range c.streams {
		if err := c.CreateConsumerGroup(ctx, stream, c.group, ">"); err != nil {
			return err
		}
	}

	// Build stream arguments
	streams := make([]string, len(c.streams)*2)
	for i, stream := range c.streams {
		streams[i*2] = stream
		streams[i*2+1] = ">" // Only get new messages
	}

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Consumer stopping...")
			return ctx.Err()
		default:
			if err := c.consumeBatch(ctx, streams); err != nil {
				c.logger.Error("Error consuming batch", zap.Error(err))
				time.Sleep(1 * time.Second)
			}
		}
	}
}

func (c *Consumer) consumeBatch(ctx context.Context, streams []string) error {
	// Read messages from streams
	results, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    c.group,
		Consumer: c.consumer,
		Streams:  streams,
		Count:    c.batchSize,
		Block:    c.blockTime,
	}).Result()

	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil // No messages available
		}
		return fmt.Errorf("failed to read from stream: %w", err)
	}

	// Process messages
	for _, result := range results {
		for _, msg := range result.Messages {
			if err := c.processMessage(ctx, result.Stream, msg); err != nil {
				c.logger.Error("Failed to process message",
					zap.String("stream", result.Stream),
					zap.String("message_id", msg.ID),
					zap.Error(err),
				)

				if c.errorHandler != nil {
					c.errorHandler(ctx, msg, err)
				}
			}
		}
	}

	return nil
}

func (c *Consumer) processMessage(ctx context.Context, stream string, msg redis.XMessage) error {
	c.logger.Debug("Processing message",
		zap.String("stream", stream),
		zap.String("message_id", msg.ID),
	)

	// Call the message handler
	if err := c.handler(ctx, msg); err != nil {
		return err
	}

	// Acknowledge the message
	if err := c.Ack(ctx, stream, msg.ID); err != nil {
		return fmt.Errorf("failed to ack message: %w", err)
	}

	return nil
}

// Ack acknowledges a message
func (c *Consumer) Ack(ctx context.Context, stream string, ids ...string) error {
	_, err := c.client.XAck(ctx, stream, c.group, ids...).Result()
	if err != nil {
		return fmt.Errorf("failed to ack message: %w", err)
	}

	return nil
}

// ClaimPendingMessages claims pending messages that have been idle for too long
func (c *Consumer) ClaimPendingMessages(ctx context.Context, stream string, minIdleTime time.Duration) ([]redis.XMessage, error) {
	// Get pending messages
	pending, err := c.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream,
		Group:  c.group,
		Start:  "-",
		End:    "+",
		Count:  c.batchSize,
	}).Result()

	if err != nil {
		return nil, fmt.Errorf("failed to get pending messages: %w", err)
	}

	if len(pending) == 0 {
		return nil, nil
	}

	// Claim idle messages
	ids := make([]string, 0)
	for _, p := range pending {
		if p.Idle >= minIdleTime {
			ids = append(ids, p.ID)
		}
	}

	if len(ids) == 0 {
		return nil, nil
	}

	messages, err := c.client.XClaim(ctx, &redis.XClaimArgs{
		Stream:   stream,
		Group:    c.group,
		Consumer: c.consumer,
		MinIdle:  minIdleTime,
		Messages: ids,
	}).Result()

	if err != nil {
		return nil, fmt.Errorf("failed to claim messages: %w", err)
	}

	return messages, nil
}

// GetPendingInfo returns information about pending messages
func (c *Consumer) GetPendingInfo(ctx context.Context, stream string) (map[string]interface{}, error) {
	pending, err := c.client.XPending(ctx, stream, c.group).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get pending info: %w", err)
	}

	return map[string]interface{}{
		"count":     pending.Count,
		"lower":     pending.Lower,
		"higher":    pending.Higher,
		"consumers": pending.Consumers,
	}, nil
}

// DeleteConsumer removes a consumer from a consumer group
func (c *Consumer) DeleteConsumer(ctx context.Context, stream string) error {
	_, err := c.client.XGroupDelConsumer(ctx, stream, c.group, c.consumer).Result()
	if err != nil {
		return fmt.Errorf("failed to delete consumer: %w", err)
	}

	return nil
}

// DeleteConsumerGroup deletes a consumer group
func (c *Consumer) DeleteConsumerGroup(ctx context.Context, stream string) error {
	_, err := c.client.XGroupDestroy(ctx, stream, c.group).Result()
	if err != nil {
		return fmt.Errorf("failed to delete consumer group: %w", err)
	}

	return nil
}

// UnmarshalJSON is a helper to unmarshal JSON payload from a message
func UnmarshalJSON(msg redis.XMessage, dest interface{}) error {
	payload, ok := msg.Values["payload"].(string)
	if !ok {
		return fmt.Errorf("payload field not found or not a string")
	}

	if err := json.Unmarshal([]byte(payload), dest); err != nil {
		return fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	return nil
}
