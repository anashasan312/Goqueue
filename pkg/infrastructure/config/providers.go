package config

import (
	"github.com/anashasan/goqueue/pkg/application/enqueuer"
	"github.com/anashasan/goqueue/pkg/application/processor"
	redisInfra "github.com/anashasan/goqueue/pkg/infrastructure/persistence/redis"
	"github.com/anashasan/goqueue/pkg/infrastructure/scheduler"
	"github.com/anashasan/goqueue/pkg/infrastructure/worker"
)

// This file holds the narrow accessors Wire uses to hand each component exactly
// the slice of configuration it needs.
//
// Passing *AppConfig everywhere would be simpler to write and much worse to
// live with: every constructor would depend on the whole config document, so a
// change to an unrelated section would recompile and re-test half the project,
// and a unit test for the worker pool would have to build a Redis section to
// construct one. This is the interface segregation principle applied to
// configuration.

// GetRedisClientConfig extracts the Redis connection settings.
func GetRedisClientConfig(cfg *AppConfig) redisInfra.ClientConfig {
	return redisInfra.ClientConfig{
		Addrs:        cfg.Redis.Addrs,
		Username:     cfg.Redis.Username,
		Password:     cfg.Redis.Password,
		DB:           cfg.Redis.DB,
		PoolSize:     cfg.Redis.PoolSize,
		MinIdleConns: cfg.Redis.MinIdleConns,
		DialTimeout:  cfg.Redis.DialTimeout,
		ReadTimeout:  cfg.Redis.ReadTimeout,
		WriteTimeout: cfg.Redis.WriteTimeout,
	}
}

// Named string types for the scalar settings.
//
// Wire resolves providers by type, so three accessors that all returned a plain
// string would be indistinguishable to it and the graph would fail to build.
// Naming each one also makes the call sites self-documenting: a function taking
// a RedisNamespace cannot be handed a log level by mistake.
type (
	// RedisNamespace prefixes every Redis key.
	RedisNamespace string
	// LogLevel is the minimum level the logger emits.
	LogLevel string
	// ServerVersion is the build version reported by the health endpoint.
	ServerVersion string
)

// GetRedisNamespace extracts the key namespace.
func GetRedisNamespace(cfg *AppConfig) RedisNamespace {
	return RedisNamespace(cfg.Redis.Namespace)
}

// GetLogLevel extracts the logger level.
func GetLogLevel(cfg *AppConfig) LogLevel { return LogLevel(cfg.Logger.Level) }

// GetServerVersion extracts the version reported by the health endpoint.
func GetServerVersion(cfg *AppConfig) ServerVersion { return ServerVersion(cfg.Server.Version) }

// GetEnqueuerConfig extracts the enqueue settings.
func GetEnqueuerConfig(cfg *AppConfig) enqueuer.Config {
	return enqueuer.Config{IdempotencyTTL: cfg.Job.IdempotencyTTL}
}

// GetProcessorConfig extracts the processing settings.
func GetProcessorConfig(cfg *AppConfig) processor.Config {
	return processor.Config{CompletedRetention: cfg.Job.CompletedRetention}
}

// GetWorkerPoolConfig extracts the worker pool settings.
func GetWorkerPoolConfig(cfg *AppConfig) worker.PoolConfig {
	return worker.PoolConfig{
		Concurrency:     cfg.Worker.Concurrency,
		Queues:          cfg.Worker.Queues,
		PollInterval:    cfg.Worker.PollInterval,
		ShutdownTimeout: cfg.Worker.ShutdownTimeout,
		LeaseDuration:   cfg.Worker.LeaseDuration,
	}
}

// GetSchedulerConfig extracts the sweep settings.
func GetSchedulerConfig(cfg *AppConfig) scheduler.Config {
	return scheduler.Config{
		PromoteInterval: cfg.Scheduler.PromoteInterval,
		ReclaimInterval: cfg.Scheduler.ReclaimInterval,
		JanitorInterval: cfg.Scheduler.JanitorInterval,
		MetricsInterval: cfg.Scheduler.MetricsInterval,
		BatchLimit:      cfg.Scheduler.BatchLimit,
	}
}
