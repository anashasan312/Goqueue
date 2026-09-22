// Package scheduler runs the periodic sweeps that keep a queue honest:
// promoting due jobs, reclaiming lapsed leases, trimming retention, and
// sampling queue depth for the metrics endpoint.
//
// Each sweep is a separate loop with its own interval because they have
// genuinely different urgencies. Promotion decides how punctual a scheduled job
// is and wants to run often; retention cleanup is housekeeping and can run once
// a minute without anyone noticing.
package scheduler

import (
	"context"
	"sync"
	"time"

	"github.com/anashasan/goqueue/pkg/common/clock"
	"github.com/anashasan/goqueue/pkg/common/errors"
	"github.com/anashasan/goqueue/pkg/common/logger"
	jobVO "github.com/anashasan/goqueue/pkg/domain/job_aggregate/value_objects"
	"github.com/anashasan/goqueue/pkg/domain/metrics"
	"github.com/anashasan/goqueue/pkg/domain/persistence"
)

// Config tunes the sweep intervals.
type Config struct {
	// PromoteInterval is how often due scheduled and retrying jobs are moved
	// into pending. It is the floor on scheduling precision.
	PromoteInterval time.Duration

	// ReclaimInterval is how often lapsed leases are returned to the queue.
	ReclaimInterval time.Duration

	// JanitorInterval is how often expired completed jobs are trimmed.
	JanitorInterval time.Duration

	// MetricsInterval is how often queue depth gauges are refreshed.
	MetricsInterval time.Duration

	// BatchLimit caps how many jobs one sweep moves, so a large backlog is
	// drained over several ticks instead of blocking Redis in one long script.
	BatchLimit int64
}

// withDefaults fills unset fields.
func (c Config) withDefaults() Config {
	if c.PromoteInterval <= 0 {
		c.PromoteInterval = time.Second
	}
	if c.ReclaimInterval <= 0 {
		c.ReclaimInterval = 30 * time.Second
	}
	if c.JanitorInterval <= 0 {
		c.JanitorInterval = time.Minute
	}
	if c.MetricsInterval <= 0 {
		c.MetricsInterval = 5 * time.Second
	}
	if c.BatchLimit <= 0 {
		c.BatchLimit = 500
	}
	return c
}

// Scheduler owns the periodic maintenance loops.
type Scheduler struct {
	broker    persistence.IJobBroker
	jobRepo   persistence.IJobRepo
	queueRepo persistence.IQueueRepo
	metrics   metrics.Recorder
	clock     clock.Clock
	log       logger.Logger
	cfg       Config

	cancel  context.CancelFunc
	wg      sync.WaitGroup
	started bool
	mu      sync.Mutex
}

// NewScheduler builds a Scheduler.
func NewScheduler(
	broker persistence.IJobBroker,
	jobRepo persistence.IJobRepo,
	queueRepo persistence.IQueueRepo,
	recorder metrics.Recorder,
	clk clock.Clock,
	log logger.Logger,
	cfg Config,
) *Scheduler {
	return &Scheduler{
		broker:    broker,
		jobRepo:   jobRepo,
		queueRepo: queueRepo,
		metrics:   recorder,
		clock:     clk,
		log:       log,
		cfg:       cfg.withDefaults(),
	}
}

// Start launches every sweep loop.
func (s *Scheduler) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return errors.Conflict("scheduler_already_started", "scheduler is already running")
	}

	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	s.cancel = cancel
	s.started = true

	s.spawn(runCtx, "promote", s.cfg.PromoteInterval, s.promoteDue)
	s.spawn(runCtx, "reclaim", s.cfg.ReclaimInterval, s.reclaimExpired)
	s.spawn(runCtx, "janitor", s.cfg.JanitorInterval, s.purgeCompleted)
	s.spawn(runCtx, "metrics", s.cfg.MetricsInterval, s.sampleQueueDepth)

	s.log.Info(ctx, "scheduler started",
		logger.F("promote_interval", s.cfg.PromoteInterval.String()),
		logger.F("reclaim_interval", s.cfg.ReclaimInterval.String()),
		logger.F("janitor_interval", s.cfg.JanitorInterval.String()),
	)
	return nil
}

// Stop cancels every loop and waits for them to exit.
//
// Sweeps are idempotent and short, so there is no drain budget here: whatever a
// sweep did not finish, the next process to start will pick up.
func (s *Scheduler) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		return nil
	}
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	s.started = false

	s.log.Info(ctx, "scheduler stopped")
	return nil
}

// sweep is one unit of periodic work.
type sweep func(ctx context.Context, queue jobVO.QueueName) error

// spawn runs a sweep against every registered queue on a fixed interval.
func (s *Scheduler) spawn(ctx context.Context, name string, interval time.Duration, fn sweep) {
	s.wg.Add(1)

	go func() {
		defer s.wg.Done()

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		loopCtx := logger.WithFields(ctx, logger.F("sweep", name))

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.runSweep(loopCtx, name, fn)
			}
		}
	}()
}

// runSweep applies one sweep to every queue, logging rather than propagating
// failures: a sweep that fails on one tick must not stop the loop, because the
// next tick is the recovery mechanism.
func (s *Scheduler) runSweep(ctx context.Context, name string, fn sweep) {
	queues, err := s.queueRepo.ListAll(ctx)
	if err != nil {
		s.log.Error(ctx, "sweep could not list queues", err)
		return
	}

	for _, queue := range queues {
		if err := fn(ctx, queue.Name()); err != nil {
			s.log.Error(ctx, "sweep failed for queue", err,
				logger.F("queue", queue.Name().String()),
				logger.F("sweep", name),
			)
		}
	}
}

// promoteDue moves scheduled and retrying jobs whose instant has arrived into
// the pending set.
func (s *Scheduler) promoteDue(ctx context.Context, queue jobVO.QueueName) error {
	moved, err := s.broker.PromoteDue(ctx, queue, s.clock.Now(), s.cfg.BatchLimit)
	if err != nil {
		return err
	}
	if moved > 0 {
		s.log.Debug(ctx, "promoted due jobs",
			logger.F("queue", queue.String()),
			logger.F("count", moved),
		)
	}
	return nil
}

// reclaimExpired returns jobs whose worker lease lapsed to the pending set.
func (s *Scheduler) reclaimExpired(ctx context.Context, queue jobVO.QueueName) error {
	reclaimed, err := s.broker.ReclaimExpired(ctx, queue, s.clock.Now(), s.cfg.BatchLimit)
	if err != nil {
		return err
	}
	if reclaimed > 0 {
		// Worth a warning rather than a debug line: a steady trickle of reclaims
		// means workers are dying or handlers are outliving their timeouts, and
		// somebody should look.
		s.log.Warn(ctx, "reclaimed jobs with expired leases",
			logger.F("queue", queue.String()),
			logger.F("count", reclaimed),
		)
	}
	return nil
}

// purgeCompleted trims completed jobs past their retention window.
func (s *Scheduler) purgeCompleted(ctx context.Context, queue jobVO.QueueName) error {
	removed, err := s.jobRepo.PurgeCompleted(ctx, queue, s.cfg.BatchLimit)
	if err != nil {
		return err
	}
	if removed > 0 {
		s.log.Debug(ctx, "purged expired completed jobs",
			logger.F("queue", queue.String()),
			logger.F("count", removed),
		)
	}
	return nil
}

// sampleQueueDepth publishes the current depth of every lifecycle state.
//
// Depth is a gauge sampled on a timer rather than maintained incrementally,
// because an incremental counter drifts the moment anything touches Redis from
// outside the application — a manual fix during an incident, say — and a wrong
// gauge is worse than a slightly stale one.
func (s *Scheduler) sampleQueueDepth(ctx context.Context, queue jobVO.QueueName) error {
	stats, err := s.queueRepo.Stats(ctx, queue)
	if err != nil {
		return err
	}

	s.metrics.SetQueueDepth(queue, jobVO.StatePending, stats.Pending)
	s.metrics.SetQueueDepth(queue, jobVO.StateActive, stats.Active)
	s.metrics.SetQueueDepth(queue, jobVO.StateScheduled, stats.Scheduled)
	s.metrics.SetQueueDepth(queue, jobVO.StateRetrying, stats.Retrying)
	s.metrics.SetQueueDepth(queue, jobVO.StateCompleted, stats.Completed)
	s.metrics.SetQueueDepth(queue, jobVO.StateDead, stats.Dead)

	return nil
}
