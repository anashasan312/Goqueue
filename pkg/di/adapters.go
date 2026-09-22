package di

import (
	prom "github.com/prometheus/client_golang/prometheus"

	h "github.com/anashasan/goqueue/pkg/api/handlers"
	svc "github.com/anashasan/goqueue/pkg/application/services"
	"github.com/anashasan/goqueue/pkg/common/logger"
	"github.com/anashasan/goqueue/pkg/infrastructure/config"
	redisInfra "github.com/anashasan/goqueue/pkg/infrastructure/persistence/redis"
)

// This file holds the thin adapters that let Wire connect two components whose
// signatures are correct on their own but do not line up by type.
//
// They live in di rather than being worked around by loosening a constructor's
// parameter types, because the alternative — making every constructor take a
// plain string so the container can wire it — would push a container concern
// into code that has nothing to do with containers.

// provideRegisterer narrows the concrete registry to the interface the metrics
// recorder actually needs.
//
// The recorder only ever registers collectors, so handing it the full *Registry
// would give it the ability to gather and reset metrics that it has no business
// having.
func provideRegisterer(registry *prom.Registry) prom.Registerer { return registry }

// provideSlogLogger adapts the named LogLevel to the logger's plain string
// parameter, keeping the logger package free of any config import.
func provideSlogLogger(level config.LogLevel) *logger.SlogLogger {
	return logger.NewSlogLogger(string(level))
}

// provideKeyBuilder adapts the named RedisNamespace to the key builder.
func provideKeyBuilder(namespace config.RedisNamespace) *redisInfra.KeyBuilder {
	return redisInfra.NewKeyBuilder(string(namespace))
}

// provideSupportHandler adapts the named ServerVersion to the support handler.
func provideSupportHandler(
	registry svc.IHandlerRegistry,
	version config.ServerVersion,
) *h.SupportHandler {
	return h.NewSupportHandler(registry, string(version))
}
