package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Server            ServerConfig
	Database          DatabaseConfig
	Redis             RedisConfig
	NotificationRedis RedisConfig `mapstructure:"notification_redis"` // Redis #2 for notification MQ
	Log               LogConfig
	Casdoor           CasdoorConfig
	Worker            WorkerConfig
	Storage           StorageConfig
	Notification      NotificationConfig      `mapstructure:"notification"`
	AssessmentService AssessmentServiceConfig `mapstructure:"assessment_service"`
}

type ServerConfig struct {
	Host         string
	Port         int
	Mode         string // debug, release, test
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

type DatabaseConfig struct {
	Host            string
	Port            int
	User            string
	Password        string
	DBName          string
	SSLMode         string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

type RedisConfig struct {
	Host     string
	Port     int
	Password string
	DB       int
}

type LogConfig struct {
	Level  string // debug, info, warn, error
	Format string // json, console
}

type CasdoorConfig struct {
	Endpoint     string
	ClientID     string
	ClientSecret string
	Cert         string
	Application  string
	Organization string
}

type WorkerConfig struct {
	Enabled       bool          `mapstructure:"enabled"`
	NumWorkers    int           `mapstructure:"num_workers"`
	StreamName    string        `mapstructure:"stream_name"`
	ConsumerGroup string        `mapstructure:"consumer_group"`
	BatchSize     int64         `mapstructure:"batch_size"`
	BlockTime     time.Duration `mapstructure:"block_time"`
	RetryAttempts int           `mapstructure:"retry_attempts"`
	DLQStream     string        `mapstructure:"dlq_stream"`

	// Batch insert configuration
	InsertBatchSize    int           `mapstructure:"insert_batch_size"`    // Records per DB insert batch
	InsertFlushTimeout time.Duration `mapstructure:"insert_flush_timeout"` // Max time to wait before flush

	// Redis Stream configuration
	StreamMaxLen int64 `mapstructure:"stream_max_len"` // Max stream length (MAXLEN)

	// Retry worker configuration
	RetryWorkerEnabled bool          `mapstructure:"retry_worker_enabled"`
	RetryCheckInterval time.Duration `mapstructure:"retry_check_interval"`
	RetryMinIdleTime   time.Duration `mapstructure:"retry_min_idle_time"`
	RetryBatchSize     int64         `mapstructure:"retry_batch_size"`
}

type StorageConfig struct {
	Endpoint        string        `mapstructure:"endpoint"`
	Region          string        `mapstructure:"region"`
	AccessKeyID     string        `mapstructure:"access_key_id"`
	SecretAccessKey string        `mapstructure:"secret_access_key"`
	BucketName      string        `mapstructure:"bucket_name"`
	CDNEndpoint     string        `mapstructure:"cdn_endpoint"`
	PresignExpiry   time.Duration `mapstructure:"presign_expiry"`
}

// NotificationConfig holds settings for violation notification publishing
type NotificationConfig struct {
	Enabled         bool   `mapstructure:"enabled"`
	StreamName      string `mapstructure:"stream_name"`      // "proctoring-events"
	MinSeverity     int    `mapstructure:"min_severity"`     // 2=HIGH, 3=CRITICAL
	CooldownSeconds int    `mapstructure:"cooldown_seconds"` // 60s between notifications
}

// AssessmentServiceConfig holds settings for assessment-service API client
type AssessmentServiceConfig struct {
	BaseURL    string        `mapstructure:"base_url"`    // "http://assessment-service:8080"
	ServiceKey string        `mapstructure:"service_key"` // X-Service-Key header
	Timeout    time.Duration `mapstructure:"timeout"`     // 5s
	CacheTTL   time.Duration `mapstructure:"cache_ttl"`   // 10m
}

// Load reads configuration from file or environment variables.
func Load() (*Config, error) {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("./config")

	// Set defaults
	setDefaults()

	// Enable environment variable override
	// This allows K8s env vars like SERVER_HOST to map to server.host
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	// Read config file (optional)
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("error reading config file: %w", err)
		}
		// Config file not found; use defaults and env vars
	}

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("unable to decode config: %w", err)
	}
	return &config, nil
}

func setDefaults() {
	// Server defaults
	viper.SetDefault("server.host", "0.0.0.0")
	viper.SetDefault("server.port", 8080)
	viper.SetDefault("server.mode", "debug")
	viper.SetDefault("server.readtimeout", 10*time.Second)
	viper.SetDefault("server.writetimeout", 10*time.Second)

	// Database defaults
	viper.SetDefault("database.host", "localhost")
	viper.SetDefault("database.port", 5432)
	viper.SetDefault("database.user", "postgres")
	viper.SetDefault("database.password", "postgres")
	viper.SetDefault("database.dbname", "protocring")
	viper.SetDefault("database.sslmode", "disable")
	viper.SetDefault("database.maxopenconns", 25)
	viper.SetDefault("database.maxidleconns", 5)
	viper.SetDefault("database.connmaxlifetime", 5*time.Minute)

	// Redis defaults
	viper.SetDefault("redis.host", "localhost")
	viper.SetDefault("redis.port", 6379)
	viper.SetDefault("redis.password", "")
	viper.SetDefault("redis.db", 0)

	// Log defaults
	viper.SetDefault("log.level", "info")
	viper.SetDefault("log.format", "json")

	// Casdoor defaults
	viper.SetDefault("casdoor.endpoint", "localhost:8000")
	viper.SetDefault("casdoor.clientid", "")
	viper.SetDefault("casdoor.clientsecret", "")
	viper.SetDefault("casdoor.cert", "")

	// Worker defaults
	viper.SetDefault("worker.enabled", true)
	viper.SetDefault("worker.num_workers", 10)
	viper.SetDefault("worker.stream_name", "violations:ingest")
	viper.SetDefault("worker.consumer_group", "violation-workers")
	viper.SetDefault("worker.batch_size", 100)
	viper.SetDefault("worker.block_time", 5*time.Second)
	viper.SetDefault("worker.retry_attempts", 3)
	viper.SetDefault("worker.dlq_stream", "violations:dlq")

	// Batch insert defaults
	viper.SetDefault("worker.insert_batch_size", 100)
	viper.SetDefault("worker.insert_flush_timeout", 100*time.Millisecond)
	viper.SetDefault("worker.stream_max_len", 500000)

	// Retry worker defaults
	viper.SetDefault("worker.retry_worker_enabled", true)
	viper.SetDefault("worker.retry_check_interval", 30*time.Second)
	viper.SetDefault("worker.retry_min_idle_time", 1*time.Minute)
	viper.SetDefault("worker.retry_batch_size", 100)

	// Storage defaults (DigitalOcean Spaces)
	viper.SetDefault("storage.endpoint", "")
	viper.SetDefault("storage.region", "sgp1")
	viper.SetDefault("storage.access_key_id", "")
	viper.SetDefault("storage.secret_access_key", "")
	viper.SetDefault("storage.bucket_name", "")
	viper.SetDefault("storage.cdn_endpoint", "")
	viper.SetDefault("storage.presign_expiry", 5*time.Minute)

	// Notification Redis defaults (Redis #2 for notification MQ)
	viper.SetDefault("notification_redis.host", "localhost")
	viper.SetDefault("notification_redis.port", 6380)
	viper.SetDefault("notification_redis.password", "")
	viper.SetDefault("notification_redis.db", 0)

	// Notification defaults
	viper.SetDefault("notification.enabled", true)
	viper.SetDefault("notification.stream_name", "proctoring-events")
	viper.SetDefault("notification.min_severity", 2) // HIGH=2, CRITICAL=3
	viper.SetDefault("notification.cooldown_seconds", 60)

	// Assessment Service client defaults
	viper.SetDefault("assessment_service.base_url", "http://localhost:8081")
	viper.SetDefault("assessment_service.service_key", "")
	viper.SetDefault("assessment_service.timeout", 5*time.Second)
	viper.SetDefault("assessment_service.cache_ttl", 10*time.Minute)
}
