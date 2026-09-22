// Package config loads and validates application configuration.
//
// Configuration is read from a YAML file and then overlaid with environment
// variables, in that order. The file holds the shape and the sensible defaults
// so a new contributor can read one document and understand every knob; the
// environment holds whatever a deployment must change, above all secrets, which
// never belong in a file that is committed.
package config

import (
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/anashasan/goqueue/pkg/common/errors"
)

// EnvPrefix namespaces every environment override.
const EnvPrefix = "GOQUEUE_"

// AppConfig is the root configuration document.
type AppConfig struct {
	Env       string          `yaml:"env"`
	Server    ServerConfig    `yaml:"server"`
	Logger    LoggerConfig    `yaml:"logger"`
	Redis     RedisConfig     `yaml:"redis"`
	Worker    WorkerConfig    `yaml:"worker"`
	Scheduler SchedulerConfig `yaml:"scheduler"`
	Job       JobConfig       `yaml:"job"`
}

// ServerConfig configures the two HTTP listeners.
//
// Public and private are separate ports so that a deployment can expose the job
// API to clients while keeping metrics, health and the operator dashboard on an
// internal network. Serving them on one port would mean anyone who can enqueue a
// job can also pause a queue.
type ServerConfig struct {
	Name            string        `yaml:"name"`
	Version         string        `yaml:"version"`
	PublicPort      int           `yaml:"public_port"`
	PrivatePort     int           `yaml:"private_port"`
	ReadTimeout     time.Duration `yaml:"read_timeout"`
	WriteTimeout    time.Duration `yaml:"write_timeout"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
}

// LoggerConfig configures the structured logger.
type LoggerConfig struct {
	Level string `yaml:"level"`
}

// RedisConfig configures the Redis connection.
type RedisConfig struct {
	Addrs        []string      `yaml:"addrs"`
	Username     string        `yaml:"username"`
	Password     string        `yaml:"password"`
	DB           int           `yaml:"db"`
	Namespace    string        `yaml:"namespace"`
	PoolSize     int           `yaml:"pool_size"`
	MinIdleConns int           `yaml:"min_idle_conns"`
	DialTimeout  time.Duration `yaml:"dial_timeout"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
}

// WorkerConfig configures the worker pool.
type WorkerConfig struct {
	Enabled         bool             `yaml:"enabled"`
	Concurrency     int              `yaml:"concurrency"`
	Queues          map[string]uint8 `yaml:"queues"`
	PollInterval    time.Duration    `yaml:"poll_interval"`
	ShutdownTimeout time.Duration    `yaml:"shutdown_timeout"`
	LeaseDuration   time.Duration    `yaml:"lease_duration"`
}

// SchedulerConfig configures the periodic sweeps.
type SchedulerConfig struct {
	Enabled         bool          `yaml:"enabled"`
	PromoteInterval time.Duration `yaml:"promote_interval"`
	ReclaimInterval time.Duration `yaml:"reclaim_interval"`
	JanitorInterval time.Duration `yaml:"janitor_interval"`
	MetricsInterval time.Duration `yaml:"metrics_interval"`
	BatchLimit      int64         `yaml:"batch_limit"`
}

// JobConfig configures job-level defaults.
type JobConfig struct {
	CompletedRetention time.Duration `yaml:"completed_retention"`
	IdempotencyTTL     time.Duration `yaml:"idempotency_ttl"`
}

// Default returns a fully populated configuration.
//
// Every field has a working default so the binary runs with no file and no
// environment at all against a local Redis. A config system that requires
// twenty environment variables before it will start is a config system people
// work around.
func Default() *AppConfig {
	return &AppConfig{
		Env: "local",
		Server: ServerConfig{
			Name:            "goqueue",
			Version:         "1.0.0",
			PublicPort:      8080,
			PrivatePort:     9090,
			ReadTimeout:     15 * time.Second,
			WriteTimeout:    30 * time.Second,
			ShutdownTimeout: 20 * time.Second,
		},
		Logger: LoggerConfig{Level: "info"},
		Redis: RedisConfig{
			Addrs:        []string{"127.0.0.1:6379"},
			DB:           0,
			Namespace:    "goqueue",
			PoolSize:     20,
			MinIdleConns: 4,
			DialTimeout:  5 * time.Second,
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 3 * time.Second,
		},
		Worker: WorkerConfig{
			Enabled:         true,
			Concurrency:     10,
			Queues:          map[string]uint8{"critical": 6, "default": 3, "low": 1},
			PollInterval:    500 * time.Millisecond,
			ShutdownTimeout: 30 * time.Second,
			LeaseDuration:   60 * time.Second,
		},
		Scheduler: SchedulerConfig{
			Enabled:         true,
			PromoteInterval: time.Second,
			ReclaimInterval: 30 * time.Second,
			JanitorInterval: time.Minute,
			MetricsInterval: 5 * time.Second,
			BatchLimit:      500,
		},
		Job: JobConfig{
			CompletedRetention: time.Hour,
			IdempotencyTTL:     24 * time.Hour,
		},
	}
}

// Load reads configuration from an optional YAML file and the environment.
//
// A missing path is not an error: it means "run on defaults plus environment",
// which is exactly what a container deployment wants.
func Load(path string) (*AppConfig, error) {
	cfg := Default()

	if path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, errors.Internal("config_read_failed", "failed to read config file "+path, err)
		}
		if err := yaml.Unmarshal(raw, cfg); err != nil {
			return nil, errors.Internal("config_parse_failed", "failed to parse config file "+path, err)
		}
	}

	cfg.applyEnv()

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// applyEnv overlays environment variables onto the loaded configuration.
//
// Only the values a deployment genuinely varies are exposed. Mapping every field
// automatically would mean a typo in an env var silently changes behaviour that
// nobody intended to make configurable.
func (c *AppConfig) applyEnv() {
	envString(&c.Env, "ENV")
	envString(&c.Logger.Level, "LOG_LEVEL")

	envInt(&c.Server.PublicPort, "PUBLIC_PORT")
	envInt(&c.Server.PrivatePort, "PRIVATE_PORT")

	if raw := os.Getenv(EnvPrefix + "REDIS_ADDRS"); raw != "" {
		parts := strings.Split(raw, ",")
		addrs := make([]string, 0, len(parts))
		for _, p := range parts {
			if trimmed := strings.TrimSpace(p); trimmed != "" {
				addrs = append(addrs, trimmed)
			}
		}
		if len(addrs) > 0 {
			c.Redis.Addrs = addrs
		}
	}
	envString(&c.Redis.Username, "REDIS_USERNAME")
	envString(&c.Redis.Password, "REDIS_PASSWORD")
	envString(&c.Redis.Namespace, "REDIS_NAMESPACE")
	envInt(&c.Redis.DB, "REDIS_DB")

	envBool(&c.Worker.Enabled, "WORKER_ENABLED")
	envInt(&c.Worker.Concurrency, "WORKER_CONCURRENCY")
	envBool(&c.Scheduler.Enabled, "SCHEDULER_ENABLED")
}

// Validate rejects a configuration that cannot produce a working process.
//
// This runs at startup so a bad value fails loudly at boot rather than at 3am
// when the first job of that shape arrives.
func (c *AppConfig) Validate() error {
	if c.Server.PublicPort <= 0 || c.Server.PublicPort > 65535 {
		return errors.Invalid("invalid_config", "server.public_port must be between 1 and 65535")
	}
	if c.Server.PrivatePort <= 0 || c.Server.PrivatePort > 65535 {
		return errors.Invalid("invalid_config", "server.private_port must be between 1 and 65535")
	}
	if c.Server.PublicPort == c.Server.PrivatePort {
		return errors.Invalid("invalid_config", "server.public_port and server.private_port must differ")
	}
	if len(c.Redis.Addrs) == 0 {
		return errors.Invalid("invalid_config", "redis.addrs must not be empty")
	}
	if c.Worker.Enabled && c.Worker.Concurrency <= 0 {
		return errors.Invalid("invalid_config", "worker.concurrency must be greater than 0")
	}
	if c.Worker.Enabled && len(c.Worker.Queues) == 0 {
		return errors.Invalid("invalid_config", "worker.queues must not be empty when the worker is enabled")
	}
	return nil
}

// IsProduction reports whether the process is running in a production-like
// environment, which switches Gin out of debug mode.
func (c *AppConfig) IsProduction() bool {
	switch strings.ToLower(c.Env) {
	case "prod", "production", "stg", "staging":
		return true
	default:
		return false
	}
}

func envString(target *string, key string) {
	if v := os.Getenv(EnvPrefix + key); v != "" {
		*target = v
	}
}

func envInt(target *int, key string) {
	raw := os.Getenv(EnvPrefix + key)
	if raw == "" {
		return
	}
	if v, err := strconv.Atoi(raw); err == nil {
		*target = v
	}
}

func envBool(target *bool, key string) {
	raw := os.Getenv(EnvPrefix + key)
	if raw == "" {
		return
	}
	if v, err := strconv.ParseBool(raw); err == nil {
		*target = v
	}
}
