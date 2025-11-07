package config

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Server   ServerConfig
	Database DatabaseConfig
	Redis    RedisConfig
	Log      LogConfig
	Casdoor  CasdoorConfig
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

// Load reads configuration from file or environment variables.
func Load() (*Config, error) {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("./config")

	// Enable environment variable override
	viper.AutomaticEnv()

	// Set defaults
	setDefaults()

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
}
