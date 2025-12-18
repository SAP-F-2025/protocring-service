package worker

import (
	"protocring-service/internal/config"
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
	RedisClient *redis.Client
	Repo        repository.ViolationRepositoryInterface
	Logger      *zap.Logger
}

// NewViolationWorker creates a ViolationWorker with FX dependency injection
func NewViolationWorker(p ViolationWorkerParams) *ViolationWorker {
	worker := &ViolationWorker{
		config:   &p.Config.Worker,
		producer: streams.NewProducer(p.RedisClient),
		repo:     p.Repo,
		logger:   p.Logger.With(zap.String("component", "violation_worker")),
		shutdown: make(chan struct{}),
	}

	// Initialize consumer
	worker.initConsumer(p.RedisClient, &p.Config.Worker)

	return worker
}
