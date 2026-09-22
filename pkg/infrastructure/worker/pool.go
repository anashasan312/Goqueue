// Package worker runs jobs concurrently.
//
// The pool's only responsibilities are concurrency and lifecycle: how many
// goroutines exist, where they get work, and how they stop. What a job *means*
// belongs to the processor service, and what a job *is* belongs to the
// aggregate. Keeping the split honest is what makes graceful shutdown testable
// without a Redis and retry policy testable without a goroutine.
package worker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anashasan/goqueue/pkg/application/services"
	"github.com/anashasan/goqueue/pkg/common/errors"
	"github.com/anashasan/goqueue/pkg/common/logger"
	"github.com/anashasan/goqueue/pkg/common/uid"
	jobVO "github.com/anashasan/goqueue/pkg/domain/job_aggregate/value_objects"
	"github.com/anashasan/goqueue/pkg/domain/metrics"
	"github.com/anashasan/goqueue/pkg/domain/persistence"
	queueAgg "github.com/anashasan/goqueue/pkg/domain/queue_aggregate"
	queueVO "github.com/anashasan/goqueue/pkg/domain/queue_aggregate/value_objects"
)

// PoolConfig tunes the worker pool.
type PoolConfig struct {
	// Concurrency is how many jobs run at once. Zero resolves to 10.
	Concurrency int

	// Queues maps a queue name to its weight. Empty resolves to the default
	// queue at weight 1.
	Queues map[string]uint8

	// PollInterval is how long a worker sleeps after finding every queue empty.
	// It trades idle Redis traffic against pickup latency for a job that arrives
	// just after a poll.
	PollInterval time.Duration

	// ShutdownTimeout bounds how long Stop waits for in-flight jobs. Jobs still
	// running when it elapses are abandoned; their leases lapse and the reclaim
	// sweep returns them to the queue, so no work is lost — it is just retried
	// somewhere else.
	ShutdownTimeout time.Duration

	// LeaseDuration is the initial lease granted on dequeue, before the job's
	// own timeout extends it.
	LeaseDuration time.Duration
}

// withDefaults fills unset fields so a zero-valued config still runs.
func (c PoolConfig) withDefaults() PoolConfig {
	if c.Concurrency <= 0 {
		c.Concurrency = 10
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 500 * time.Millisecond
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = 30 * time.Second
	}
	if c.LeaseDuration <= 0 {
		c.LeaseDuration = 60 * time.Second
	}
	if len(c.Queues) == 0 {
		c.Queues = map[string]uint8{jobVO.DefaultQueueName: 1}
	}
	return c
}

// Pool runs a fixed set of worker goroutines against a weighted queue order.
type Pool struct {
	broker    persistence.IJobBroker
	queueRepo persistence.IQueueRepo
	processor services.IJobProcessorService
	metrics   metrics.Recorder
	ids       uid.Generator
	log       logger.Logger
	cfg       PoolConfig

	// dequeueOrder is the weighted polling order, rebuilt whenever queue
	// configuration changes.
	dequeueOrder []jobVO.QueueName

	// cancel stops the worker loops. It is set by Start and read by Stop.
	cancel context.CancelFunc
	// wg tracks the worker goroutines so Stop can wait for them.
	wg sync.WaitGroup

	// activeJobs counts jobs currently executing, published as a gauge.
	activeJobs atomic.Int64
	// started guards against a double Start, which would silently double
	// concurrency and leak the first set of goroutines.
	started atomic.Bool
}

// NewPool builds a Pool.
func NewPool(
	broker persistence.IJobBroker,
	queueRepo persistence.IQueueRepo,
	processor services.IJobProcessorService,
	recorder metrics.Recorder,
	ids uid.Generator,
	log logger.Logger,
	cfg PoolConfig,
) *Pool {
	return &Pool{
		broker:    broker,
		queueRepo: queueRepo,
		processor: processor,
		metrics:   recorder,
		ids:       ids,
		log:       log,
		cfg:       cfg.withDefaults(),
	}
}

// Start registers the configured queues and launches the worker goroutines.
//
// It returns as soon as the workers are running; the caller keeps serving HTTP
// or blocks on a signal. Stop is what waits.
func (p *Pool) Start(ctx context.Context) error {
	if !p.started.CompareAndSwap(false, true) {
		return errors.Conflict("pool_already_started", "worker pool is already running")
	}

	if err := p.registerQueues(ctx); err != nil {
		p.started.Store(false)
		return err
	}

	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	p.cancel = cancel

	for i := 0; i < p.cfg.Concurrency; i++ {
		workerID := fmt.Sprintf("worker-%s-%d", p.ids.New()[:8], i)
		p.wg.Add(1)
		go p.run(runCtx, workerID)
	}

	p.log.Info(ctx, "worker pool started",
		logger.F("concurrency", p.cfg.Concurrency),
		logger.F("queues", queueNames(p.dequeueOrder)),
		logger.F("poll_interval", p.cfg.PollInterval.String()),
	)
	return nil
}

// Stop signals every worker to finish its current job and stop dequeuing, then
// waits up to ShutdownTimeout for them.
//
// This is the graceful part: cancelling runCtx breaks the dequeue loop but the
// job already in flight runs to completion, because it holds its own context
// derived from the job timeout rather than from the pool. A worker therefore
// never abandons a job it has already started unless the shutdown budget runs
// out entirely.
func (p *Pool) Stop(ctx context.Context) error {
	if !p.started.Load() {
		return nil
	}
	if p.cancel != nil {
		p.cancel()
	}

	p.log.Info(ctx, "worker pool draining",
		logger.F("in_flight", p.activeJobs.Load()),
		logger.F("timeout", p.cfg.ShutdownTimeout.String()),
	)

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		p.started.Store(false)
		p.log.Info(ctx, "worker pool stopped cleanly")
		return nil

	case <-time.After(p.cfg.ShutdownTimeout):
		p.started.Store(false)
		// Not returning an error: the jobs still running hold leases that will
		// lapse, and the reclaim sweep will requeue them. A slow shutdown is a
		// latency event, not data loss, and reporting it as a failure would make
		// a rolling deploy look broken.
		p.log.Warn(ctx, "worker pool shutdown timed out; in-flight jobs will be reclaimed by lease expiry",
			logger.F("in_flight", p.activeJobs.Load()))
		return nil
	}
}

// ActiveJobs reports how many jobs are executing right now.
func (p *Pool) ActiveJobs() int64 { return p.activeJobs.Load() }

// Queues returns the queues this pool drains.
func (p *Pool) Queues() []jobVO.QueueName {
	return dedupe(p.dequeueOrder)
}

// run is one worker goroutine: dequeue, process, repeat until cancelled.
func (p *Pool) run(ctx context.Context, workerID string) {
	defer p.wg.Done()

	workerCtx := logger.WithFields(ctx, logger.F("worker_id", workerID))
	p.log.Debug(workerCtx, "worker started")

	for {
		// Check for shutdown before dequeuing, so a cancelled pool never claims
		// a job it is about to abandon.
		select {
		case <-ctx.Done():
			p.log.Debug(workerCtx, "worker stopped")
			return
		default:
		}

		job, err := p.broker.Dequeue(ctx, p.dequeueOrder, workerID, p.cfg.LeaseDuration)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			p.log.Error(workerCtx, "dequeue failed", err)
			if !p.sleep(ctx, p.cfg.PollInterval) {
				return
			}
			continue
		}

		if job == nil {
			if !p.sleep(ctx, p.cfg.PollInterval) {
				return
			}
			continue
		}

		p.activeJobs.Add(1)
		p.metrics.SetActiveWorkers(p.activeJobs.Load())

		// The job runs under a context detached from the pool's cancellation so
		// that shutdown lets it finish. Its own timeout, applied inside the
		// processor, is what bounds it.
		jobCtx := context.WithoutCancel(workerCtx)
		if err := p.processor.Process(jobCtx, job); err != nil {
			p.log.Error(workerCtx, "job processing failed", err,
				logger.F("job_id", job.ID().String()))
		}

		p.activeJobs.Add(-1)
		p.metrics.SetActiveWorkers(p.activeJobs.Load())
	}
}

// sleep waits for d or until the context is cancelled. It reports false when
// the worker should stop.
func (p *Pool) sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// registerQueues persists the configured queues and computes the polling order.
//
// The order is built by the domain's BuildDequeueOrder so that fairness is
// defined once, in a package with no infrastructure dependencies, and exercised
// by the same tests whatever the broker.
func (p *Pool) registerQueues(ctx context.Context) error {
	queues := make([]*queueAgg.Queue, 0, len(p.cfg.Queues))

	for rawName, rawWeight := range p.cfg.Queues {
		name, err := jobVO.NewQueueName(rawName)
		if err != nil {
			return err
		}
		weight, err := queueVO.NewWeight(rawWeight)
		if err != nil {
			return err
		}

		queue := queueAgg.NewQueue(name, weight)
		if err := p.queueRepo.Register(ctx, queue); err != nil {
			return err
		}
		queues = append(queues, queue)
	}

	p.dequeueOrder = queueAgg.BuildDequeueOrder(queues)
	if len(p.dequeueOrder) == 0 {
		return errors.Invalid("no_queues_configured", "worker pool has no queues to drain")
	}
	return nil
}

// dedupe collapses a weighted order into distinct names.
func dedupe(order []jobVO.QueueName) []jobVO.QueueName {
	seen := make(map[jobVO.QueueName]struct{}, len(order))
	out := make([]jobVO.QueueName, 0, len(order))
	for _, q := range order {
		if _, ok := seen[q]; ok {
			continue
		}
		seen[q] = struct{}{}
		out = append(out, q)
	}
	return out
}

// queueNames renders the distinct queues for a log field.
func queueNames(order []jobVO.QueueName) []string {
	distinct := dedupe(order)
	out := make([]string, 0, len(distinct))
	for _, q := range distinct {
		out = append(out, q.String())
	}
	return out
}
