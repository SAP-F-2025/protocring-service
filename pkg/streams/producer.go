package streams

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Producer handles publishing messages to Redis Streams
type Producer struct {
	client *redis.Client
}

// NewProducer creates a new stream producer
func NewProducer(client *redis.Client) *Producer {
	return &Producer{
		client: client,
	}
}

// Message represents a stream message
type Message struct {
	ID     string
	Stream string
	Data   map[string]interface{}
}

// PublishOptions represents options for publishing messages
type PublishOptions struct {
	MaxLen      int64  // Maximum stream length (0 = unlimited)
	Approximate bool   // Use approximate trimming (~)
	MinID       string // Trim by minimum ID
}

// Publish publishes a message to a stream
func (p *Producer) Publish(ctx context.Context, stream string, data map[string]interface{}, opts *PublishOptions) (string, error) {
	args := &redis.XAddArgs{
		Stream: stream,
		Values: data,
	}

	if opts != nil {
		if opts.MaxLen > 0 {
			args.MaxLen = opts.MaxLen
			args.Approx = opts.Approximate
		}
		if opts.MinID != "" {
			args.MinID = opts.MinID
		}
	}

	id, err := p.client.XAdd(ctx, args).Result()
	if err != nil {
		return "", fmt.Errorf("failed to publish message: %w", err)
	}

	return id, nil
}

// PublishJSON publishes a JSON-encoded message to a stream
func (p *Producer) PublishJSON(ctx context.Context, stream string, payload interface{}, opts *PublishOptions) (string, error) {
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("failed to marshal payload: %w", err)
	}

	data := map[string]interface{}{
		"payload":   string(jsonData),
		"timestamp": time.Now().Unix(),
	}

	return p.Publish(ctx, stream, data, opts)
}

// PublishBatch publishes multiple messages to a stream in a pipeline
func (p *Producer) PublishBatch(ctx context.Context, stream string, messages []map[string]interface{}, opts *PublishOptions) ([]string, error) {
	pipe := p.client.Pipeline()

	cmds := make([]*redis.StringCmd, len(messages))
	for i, msg := range messages {
		args := &redis.XAddArgs{
			Stream: stream,
			Values: msg,
		}

		if opts != nil {
			if opts.MaxLen > 0 {
				args.MaxLen = opts.MaxLen
				args.Approx = opts.Approximate
			}
		}

		cmds[i] = pipe.XAdd(ctx, args)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute batch: %w", err)
	}

	ids := make([]string, len(cmds))
	for i, cmd := range cmds {
		id, err := cmd.Result()
		if err != nil {
			return nil, fmt.Errorf("failed to get result for message %d: %w", i, err)
		}
		ids[i] = id
	}

	return ids, nil
}

// GetStreamLength returns the number of messages in a stream
func (p *Producer) GetStreamLength(ctx context.Context, stream string) (int64, error) {
	length, err := p.client.XLen(ctx, stream).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to get stream length: %w", err)
	}

	return length, nil
}

// TrimStream trims a stream to a specified maximum length
func (p *Producer) TrimStream(ctx context.Context, stream string, maxLen int64, approximate bool) error {
	var err error

	if approximate {
		_, err = p.client.XTrimMaxLenApprox(ctx, stream, maxLen, 0).Result()
	} else {
		_, err = p.client.XTrimMaxLen(ctx, stream, maxLen).Result()
	}

	if err != nil {
		return fmt.Errorf("failed to trim stream: %w", err)
	}

	return nil
}

// DeleteMessage deletes a message from a stream
func (p *Producer) DeleteMessage(ctx context.Context, stream string, ids ...string) error {
	_, err := p.client.XDel(ctx, stream, ids...).Result()
	if err != nil {
		return fmt.Errorf("failed to delete message: %w", err)
	}

	return nil
}

// GetStreamInfo returns information about a stream
func (p *Producer) GetStreamInfo(ctx context.Context, stream string) (map[string]interface{}, error) {
	info, err := p.client.XInfoStream(ctx, stream).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get stream info: %w", err)
	}

	return map[string]interface{}{
		"length":           info.Length,
		"radix_tree_keys":  info.RadixTreeKeys,
		"radix_tree_nodes": info.RadixTreeNodes,
		"groups":           info.Groups,
		"first_entry":      info.FirstEntry,
		"last_entry":       info.LastEntry,
	}, nil
}
