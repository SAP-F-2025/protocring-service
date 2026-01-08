package notification

import (
	"context"
	"crypto/tls"
	"fmt"

	"protocring-service/internal/client"
	"protocring-service/internal/config"
	"protocring-service/internal/events"

	"github.com/redis/go-redis/v9"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// Module provides notification dependencies
var Module = fx.Module("notification",
	// Provide named Redis clients
	fx.Provide(
		fx.Annotate(NewWorkerRedis, fx.ResultTags(`name:"workerRedis"`)),
	),
	fx.Provide(
		fx.Annotate(NewNotificationRedis, fx.ResultTags(`name:"notificationRedis"`)),
	),

	// Provide notification components
	fx.Provide(NewNotificationPublisher),
	fx.Provide(NewAssessmentClient),
)

// NewWorkerRedis creates the Redis client for worker operations (violations ingestion)
func NewWorkerRedis(lc fx.Lifecycle, cfg *config.Config, logger *zap.Logger) (*redis.Client, error) {
	return createRedisClient(lc, &cfg.Redis, logger, "worker")
}

// NewNotificationRedis creates the Redis client for notification MQ
func NewNotificationRedis(lc fx.Lifecycle, cfg *config.Config, logger *zap.Logger) (*redis.Client, error) {
	// If notification is disabled or config is empty, return nil
	if !cfg.Notification.Enabled || cfg.NotificationRedis.Host == "" {
		logger.Info("Notification Redis not configured, using worker Redis for notifications")
		return nil, nil
	}

	return createRedisClient(lc, &cfg.NotificationRedis, logger, "notification")
}

// createRedisClient is a helper to create a Redis client with lifecycle hooks
func createRedisClient(lc fx.Lifecycle, redisCfg *config.RedisConfig, logger *zap.Logger, name string) (*redis.Client, error) {
	opts := &redis.Options{
		Addr:     fmt.Sprintf("%s:%d", redisCfg.Host, redisCfg.Port),
		Password: redisCfg.Password,
		DB:       redisCfg.DB,
	}

	// Only use TLS if password is set (assuming production)
	if redisCfg.Password != "" {
		opts.TLSConfig = &tls.Config{}
	}

	client := redis.NewClient(opts)

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			logger.Info(fmt.Sprintf("Connecting to %s Redis...", name))

			if err := client.Ping(ctx).Err(); err != nil {
				return fmt.Errorf("unable to connect to %s Redis: %w", name, err)
			}

			logger.Info(fmt.Sprintf("Successfully connected to %s Redis", name),
				zap.String("host", redisCfg.Host),
				zap.Int("port", redisCfg.Port),
				zap.Int("db", redisCfg.DB),
			)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Info(fmt.Sprintf("Closing %s Redis connection...", name))
			if err := client.Close(); err != nil {
				return fmt.Errorf("error closing %s Redis connection: %w", name, err)
			}
			logger.Info(fmt.Sprintf("%s Redis connection closed", name))
			return nil
		},
	})

	return client, nil
}

// NotificationPublisherParams holds dependencies for NotificationPublisher
type NotificationPublisherParams struct {
	fx.In

	Config *config.Config
	Logger *zap.Logger

	// Use notification Redis if available, fall back to worker Redis
	NotificationRedis *redis.Client `name:"notificationRedis" optional:"true"`
	WorkerRedis       *redis.Client `name:"workerRedis"`
}

// NewNotificationPublisher creates a notification publisher
func NewNotificationPublisher(p NotificationPublisherParams) *events.NotificationPublisher {
	if !p.Config.Notification.Enabled {
		p.Logger.Info("Notification publishing is disabled")
		return nil
	}

	// Determine which Redis to use
	redisClient := p.NotificationRedis
	if redisClient == nil {
		redisClient = p.WorkerRedis
		p.Logger.Info("Using worker Redis for notifications (notification_redis not configured)")
	}

	return events.NewNotificationPublisher(
		&p.Config.Notification,
		redisClient,
		p.Logger.With(zap.String("component", "notification_publisher")),
	)
}

// AssessmentClientParams holds dependencies for AssessmentClient
type AssessmentClientParams struct {
	fx.In

	Config      *config.Config
	Logger      *zap.Logger
	WorkerRedis *redis.Client `name:"workerRedis"`
}

// NewAssessmentClient creates an assessment service client
func NewAssessmentClient(p AssessmentClientParams) *client.AssessmentClient {
	if !p.Config.Notification.Enabled {
		p.Logger.Info("Assessment client not needed (notifications disabled)")
		return nil
	}

	if p.Config.AssessmentService.BaseURL == "" {
		p.Logger.Warn("Assessment service base URL not configured")
		return nil
	}

	return client.NewAssessmentClient(
		&p.Config.AssessmentService,
		p.WorkerRedis, // Use worker Redis for caching
		p.Logger.With(zap.String("component", "assessment_client")),
	)
}
