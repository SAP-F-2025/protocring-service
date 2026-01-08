package worker

import (
	"protocring-service/internal/client"
	"protocring-service/internal/config"
	"protocring-service/internal/events"
	"protocring-service/internal/repository"
	"protocring-service/pkg/streams"

	"github.com/redis/go-redis/v9"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// Module exports worker dependency
var Module = fx.Module("worker",
	fx.Provide(NewViolationWorker),
	fx.Provide(NewProducer),
)

// NewProducer creates a Redis Stream producer
func NewProducer(client *redis.Client) *streams.Producer {
	return streams.NewProducer(client)
}

// Params for ViolationWorker constructor
type ViolationWorkerParams struct {
	fx.In

	Config      *config.Config
	RedisClient *redis.Client `name:"workerRedis"`
	Repo        repository.ViolationRepositoryInterface
	Logger      *zap.Logger

	// Optional notification components (may be nil if not configured)
	NotifPublisher   *events.NotificationPublisher `optional:"true"`
	AssessmentClient *client.AssessmentClient      `optional:"true"`
}

// NewViolationWorker creates a ViolationWorker with FX dependency injection
func NewViolationWorker(p ViolationWorkerParams) *ViolationWorker {
	worker := &ViolationWorker{
		config:      &p.Config.Worker,
		redisClient: p.RedisClient,
		producer:    streams.NewProducer(p.RedisClient),
		repo:        p.Repo,
		logger:      p.Logger.With(zap.String("component", "violation_worker")),
		shutdown:    make(chan struct{}),
		notifConfig: &p.Config.Notification,
		// Note: Cooldown is handled via Redis, no local cache needed
	}

	// Set notification publisher if available
	if p.NotifPublisher != nil {
		worker.notifPublisher = p.NotifPublisher
		worker.logger.Info("Notification publisher configured",
			zap.String("stream", p.Config.Notification.StreamName),
			zap.Int("min_severity", p.Config.Notification.MinSeverity))
	}

	// Set assessment client if available
	if p.AssessmentClient != nil {
		worker.assessmentClient = p.AssessmentClient
		worker.logger.Info("Assessment client configured",
			zap.String("base_url", p.Config.AssessmentService.BaseURL))
	}

	return worker
}
