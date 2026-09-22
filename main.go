// Command goqueue runs the GoQueue service: the HTTP API, the worker pool, the
// periodic sweeps, or any combination of them.
//
// One binary serves every role, selected by configuration. A deployment can run
// API-only pods behind a load balancer and worker-only pods on a different node
// pool, from the same image, with no build-time variants to keep in sync.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/anashasan/goqueue/cmd/server"
	svc "github.com/anashasan/goqueue/pkg/application/services"
	"github.com/anashasan/goqueue/pkg/common/logger"
	"github.com/anashasan/goqueue/pkg/di"
	"github.com/anashasan/goqueue/pkg/infrastructure/config"
	redisInfra "github.com/anashasan/goqueue/pkg/infrastructure/persistence/redis"
	"github.com/anashasan/goqueue/pkg/tasks"
)

// stoppable is the shutdown contract every background component satisfies.
//
// Collecting the running components in a slice of these lets main stop them in
// reverse start order without knowing what any of them are — the pool and the
// scheduler share no interface beyond "can be asked to stop".
type stoppable struct {
	name string
	stop func(context.Context) error
}

func main() {
	configPath := flag.String("config", os.Getenv("GOQUEUE_CONFIG_PATH"), "path to the YAML config file")
	flag.Parse()

	if err := run(*configPath); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

// run owns the whole process lifecycle and returns an error instead of calling
// os.Exit, so every deferred cleanup below actually runs.
func run(configPath string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}

	log := di.ProvideLogger(cfg)

	// Signals are trapped before anything is started, so a Ctrl-C during a slow
	// Redis connect is still handled gracefully rather than killing the process
	// mid-initialisation.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Info(ctx, "starting goqueue",
		logger.F("env", cfg.Env),
		logger.F("version", cfg.Server.Version),
		logger.F("worker_enabled", cfg.Worker.Enabled),
		logger.F("scheduler_enabled", cfg.Scheduler.Enabled),
	)

	redisClient, err := di.ProvideRedisClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := redisClient.Close(); closeErr != nil {
			log.Error(ctx, "failed to close redis client", closeErr)
		}
	}()

	// Preloading is an optimisation, not a requirement: go-redis falls back to
	// EVAL and reloads a missing script on demand, so a failure here is logged
	// and the process continues.
	if err := redisInfra.PreloadScripts(ctx, redisClient); err != nil {
		log.Warn(ctx, "failed to preload lua scripts; they will load on first use",
			logger.F("error", err.Error()))
	}

	metricsRegistry := di.ProvideMetricsRegistry()
	recorder := di.ProvideMetricsRecorder(metricsRegistry)

	handlerRegistry := di.InjectHandlerRegistry()
	if err := registerTaskHandlers(handlerRegistry, log); err != nil {
		return err
	}

	handlers := di.InjectHandlers(cfg, redisClient, recorder, handlerRegistry)
	httpServer := server.NewHTTPServer(
		cfg, handlers, metricsRegistry, di.ProvideUIDGenerator(), log,
	)

	var running []stoppable

	if cfg.Scheduler.Enabled {
		sched := di.InjectScheduler(cfg, redisClient, recorder)
		if err := sched.Start(ctx); err != nil {
			return err
		}
		running = append(running, stoppable{"scheduler", sched.Stop})
	}

	if cfg.Worker.Enabled {
		pool := di.InjectWorkerPool(cfg, redisClient, recorder, handlerRegistry)
		if err := pool.Start(ctx); err != nil {
			return err
		}
		running = append(running, stoppable{"worker pool", pool.Stop})
	}

	httpFailures := httpServer.Start(ctx)

	log.Info(ctx, "goqueue is ready",
		logger.F("api", fmt.Sprintf("http://localhost:%d/api/v1", cfg.Server.PublicPort)),
		logger.F("dashboard", fmt.Sprintf("http://localhost:%d/dashboard", cfg.Server.PrivatePort)),
		logger.F("metrics", fmt.Sprintf("http://localhost:%d/metrics", cfg.Server.PrivatePort)),
	)

	select {
	case <-ctx.Done():
		log.Info(ctx, "shutdown signal received")
	case err := <-httpFailures:
		log.Error(ctx, "http listener failed; shutting down", err)
	}

	return shutdown(log, httpServer, running)
}

// shutdown stops every component in reverse start order.
//
// The order is what makes the drain correct: the HTTP listeners stop first so no
// new work is accepted, then the worker pool drains what it already holds, then
// the scheduler stops. Reversing that would let the API keep enqueuing into a
// queue nobody is draining, and let the pool claim new jobs it is about to
// abandon.
func shutdown(
	log logger.Logger,
	httpServer *server.HTTPServer,
	running []stoppable,
) error {
	// A fresh context: the signal context is already cancelled, and passing it
	// here would make every shutdown step return immediately without draining.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var firstErr error

	if err := httpServer.Stop(shutdownCtx); err != nil {
		log.Error(shutdownCtx, "failed to stop http server", err)
		firstErr = err
	}

	for i := len(running) - 1; i >= 0; i-- {
		component := running[i]
		if err := component.stop(shutdownCtx); err != nil {
			log.Error(shutdownCtx, "failed to stop component", err,
				logger.F("component", component.name))
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	log.Info(shutdownCtx, "goqueue stopped")
	return firstErr
}

// registerTaskHandlers binds this deployment's task handlers.
//
// This is the seam an application customises: replace the call below with its
// own registrations and nothing else in the project changes.
func registerTaskHandlers(registry svc.IHandlerRegistry, log logger.Logger) error {
	return tasks.RegisterAll(registry, log)
}
