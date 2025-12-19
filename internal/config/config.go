package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Server   ServerConfig
	Database DatabaseConfig
	Redis    RedisConfig
	Log      LogConfig
	Casdoor  CasdoorConfig
	Worker   WorkerConfig
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
	viper.SetDefault("worker.num_workers", 3)
	viper.SetDefault("worker.stream_name", "violations:ingest")
	viper.SetDefault("worker.consumer_group", "violation-workers")
	viper.SetDefault("worker.batch_size", 10)
	viper.SetDefault("worker.block_time", 5*time.Second)
	viper.SetDefault("worker.retry_attempts", 3)
	viper.SetDefault("worker.dlq_stream", "violations:dlq")
}
