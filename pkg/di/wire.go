//go:build wireinject
// +build wireinject

// Package di is the composition root.
//
// It is the only package that knows which concrete type satisfies which
// interface. Every other package depends on interfaces alone, which is what
// makes the dependency inversion principle real here rather than decorative:
// swap Redis for Postgres and this file is the only one that changes.
//
// The structure follows a consistent shape:
//
//	provideX     — a Wire injector returning one concrete type
//	xSet         — a ProviderSet binding that concrete type to its interface
//
// Grouping a provider with its wire.Bind means a consumer asks for the interface
// and never has to know, or repeat, which implementation satisfies it.
//
// After editing this file run `make wire` to regenerate wire_gen.go.
package di

import (
	"context"

	"github.com/google/wire"
	prom "github.com/prometheus/client_golang/prometheus"
	goredis "github.com/redis/go-redis/v9"

	h "github.com/anashasan/goqueue/pkg/api/handlers"
	enqApp "github.com/anashasan/goqueue/pkg/application/enqueuer"
	insApp "github.com/anashasan/goqueue/pkg/application/inspector"
	procApp "github.com/anashasan/goqueue/pkg/application/processor"
	svc "github.com/anashasan/goqueue/pkg/application/services"
	"github.com/anashasan/goqueue/pkg/common/clock"
	"github.com/anashasan/goqueue/pkg/common/logger"
	"github.com/anashasan/goqueue/pkg/common/uid"
	"github.com/anashasan/goqueue/pkg/domain/metrics"
	iPersist "github.com/anashasan/goqueue/pkg/domain/persistence"
	"github.com/anashasan/goqueue/pkg/infrastructure/config"
	promInfra "github.com/anashasan/goqueue/pkg/infrastructure/metrics/prometheus"
	redisInfra "github.com/anashasan/goqueue/pkg/infrastructure/persistence/redis"
	"github.com/anashasan/goqueue/pkg/infrastructure/scheduler"
	"github.com/anashasan/goqueue/pkg/infrastructure/worker"
)

// region — shared kernel

// ProvideClock provides the wall clock.
func ProvideClock() clock.Clock {
	wire.Build(clockSet)
	return nil
}

var clockSet = wire.NewSet(
	clock.NewSystemClock,
	wire.Bind(new(clock.Clock), new(*clock.SystemClock)),
)

// ProvideUIDGenerator provides the identifier generator.
func ProvideUIDGenerator() uid.Generator {
	wire.Build(uidSet)
	return nil
}

var uidSet = wire.NewSet(
	uid.NewRandomGenerator,
	wire.Bind(new(uid.Generator), new(*uid.RandomGenerator)),
)

// ProvideLogger provides the structured logger.
func ProvideLogger(cfg *config.AppConfig) logger.Logger {
	wire.Build(loggerSet)
	return nil
}

var loggerSet = wire.NewSet(
	config.GetLogLevel,
	provideSlogLogger,
	wire.Bind(new(logger.Logger), new(*logger.SlogLogger)),
)

// endregion

// region — metrics

// ProvideMetricsRegistry provides the Prometheus registry.
func ProvideMetricsRegistry() *prom.Registry {
	wire.Build(promInfra.NewRegistry)
	return nil
}

// ProvideMetricsRecorder provides the metrics port implementation.
func ProvideMetricsRecorder(registry *prom.Registry) metrics.Recorder {
	wire.Build(metricsSet)
	return nil
}

// metricsSet binds the recorder and adapts *prom.Registry to the narrower
// prom.Registerer that NewRecorder accepts. The narrowing is intentional: the
// recorder only registers collectors, so it should not be handed a type that
// can also gather them.
var metricsSet = wire.NewSet(
	provideRegisterer,
	promInfra.NewRecorder,
	wire.Bind(new(metrics.Recorder), new(*promInfra.Recorder)),
)

// endregion

// region — redis infrastructure

// ProvideRedisClient provides the Redis connection.
func ProvideRedisClient(ctx context.Context, cfg *config.AppConfig) (goredis.UniversalClient, error) {
	wire.Build(config.GetRedisClientConfig, redisInfra.NewClient)
	return nil, nil
}

// ProvideKeyBuilder provides the Redis key layout.
func ProvideKeyBuilder(cfg *config.AppConfig) *redisInfra.KeyBuilder {
	wire.Build(keyBuilderSet)
	return nil
}

var keyBuilderSet = wire.NewSet(config.GetRedisNamespace, provideKeyBuilder)

var jobRepoSet = wire.NewSet(
	redisInfra.NewJobRepo,
	wire.Bind(new(iPersist.IJobRepo), new(*redisInfra.JobRepo)),
)

var jobBrokerSet = wire.NewSet(
	redisInfra.NewJobBroker,
	wire.Bind(new(iPersist.IJobBroker), new(*redisInfra.JobBroker)),
)

var queueRepoSet = wire.NewSet(
	redisInfra.NewQueueRepo,
	wire.Bind(new(iPersist.IQueueRepo), new(*redisInfra.QueueRepo)),
)

var idempotencySet = wire.NewSet(
	redisInfra.NewIdempotencyStore,
	wire.Bind(new(iPersist.IIdempotencyStore), new(*redisInfra.IdempotencyStore)),
)

// persistenceSet is every storage port bound to its Redis implementation.
var persistenceSet = wire.NewSet(
	keyBuilderSet,
	jobRepoSet,
	jobBrokerSet,
	queueRepoSet,
	idempotencySet,
)

// endregion

// region — application services

var handlerRegistrySet = wire.NewSet(
	procApp.NewHandlerRegistry,
	wire.Bind(new(svc.IHandlerRegistry), new(*procApp.HandlerRegistry)),
)

var enqueuerSet = wire.NewSet(
	config.GetEnqueuerConfig,
	enqApp.NewEnqueuerService,
	wire.Bind(new(svc.IEnqueuerService), new(*enqApp.EnqueuerService)),
)

var inspectorSet = wire.NewSet(
	insApp.NewInspectorService,
	wire.Bind(new(svc.IInspectorService), new(*insApp.InspectorService)),
)

var processorSet = wire.NewSet(
	config.GetProcessorConfig,
	procApp.NewJobProcessorService,
	wire.Bind(new(svc.IJobProcessorService), new(*procApp.JobProcessorService)),
)

// endregion

// region — injectors

// InjectHandlers builds the full HTTP handler graph.
func InjectHandlers(
	cfg *config.AppConfig,
	client goredis.UniversalClient,
	recorder metrics.Recorder,
	registry svc.IHandlerRegistry,
) *h.Handlers {
	wire.Build(
		h.NewHandlers,
		h.NewJobHandler,
		h.NewQueueHandler,
		provideSupportHandler,
		h.NewDashboardHandler,
		config.GetServerVersion,

		enqueuerSet,
		inspectorSet,
		persistenceSet,
		clockSet,
		uidSet,
		loggerSet,
	)
	return nil
}

// InjectWorkerPool builds the worker pool graph.
func InjectWorkerPool(
	cfg *config.AppConfig,
	client goredis.UniversalClient,
	recorder metrics.Recorder,
	registry svc.IHandlerRegistry,
) *worker.Pool {
	wire.Build(
		worker.NewPool,
		config.GetWorkerPoolConfig,

		processorSet,
		persistenceSet,
		clockSet,
		uidSet,
		loggerSet,
	)
	return nil
}

// InjectScheduler builds the periodic sweep graph.
func InjectScheduler(
	cfg *config.AppConfig,
	client goredis.UniversalClient,
	recorder metrics.Recorder,
) *scheduler.Scheduler {
	wire.Build(
		scheduler.NewScheduler,
		config.GetSchedulerConfig,

		persistenceSet,
		clockSet,
		loggerSet,
	)
	return nil
}

// InjectHandlerRegistry builds the task handler registry.
//
// It is injected once and shared by the worker pool and the readiness endpoint,
// so both agree on what this process can actually run.
func InjectHandlerRegistry() svc.IHandlerRegistry {
	wire.Build(handlerRegistrySet)
	return nil
}

// endregion
