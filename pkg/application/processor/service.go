package processor

import (
	"context"
	"fmt"
	"time"

	"github.com/anashasan/goqueue/pkg/application/services"
	"github.com/anashasan/goqueue/pkg/common/clock"
	"github.com/anashasan/goqueue/pkg/common/errors"
	"github.com/anashasan/goqueue/pkg/common/logger"
	jobAgg "github.com/anashasan/goqueue/pkg/domain/job_aggregate"
	jobErr "github.com/anashasan/goqueue/pkg/domain/job_aggregate/error"
	"github.com/anashasan/goqueue/pkg/domain/metrics"
	"github.com/anashasan/goqueue/pkg/domain/persistence"
)

var _ services.IJobProcessorService = (*JobProcessorService)(nil)

// Config tunes processing behaviour.
type Config struct {
	// CompletedRetention is how long a succeeded job's document is kept so it
	// remains visible on the dashboard. Zero disables retention entirely.
	CompletedRetention time.Duration
}

// JobProcessorService executes one leased job and records its outcome.
//
// This is where the reliability guarantees actually land: the timeout, the panic
// barrier, the retry-or-die decision and the metrics all happen in Process. The
// worker pool around it only supplies jobs and goroutines.
type JobProcessorService struct {
	registry services.IHandlerRegistry
	broker   persistence.IJobBroker
	jobRepo  persistence.IJobRepo
	metrics  metrics.Recorder
	clock    clock.Clock
	log      logger.Logger
	cfg      Config
}

// NewJobProcessorService builds a JobProcessorService.
func NewJobProcessorService(
	registry services.IHandlerRegistry,
	broker persistence.IJobBroker,
	jobRepo persistence.IJobRepo,
	recorder metrics.Recorder,
	clk clock.Clock,
	log logger.Logger,
	cfg Config,
) *JobProcessorService {
	if cfg.CompletedRetention <= 0 {
		cfg.CompletedRetention = time.Hour
	}
	return &JobProcessorService{
		registry: registry,
		broker:   broker,
		jobRepo:  jobRepo,
		metrics:  recorder,
		clock:    clk,
		log:      log,
		cfg:      cfg,
	}
}

// Process executes one leased job and persists its outcome.
//
// The returned error describes a failure of the processing machinery — an
// unreachable Redis, say. A handler that returns an error is an expected
// outcome, recorded and retried, and yields a nil error from Process so the
// worker loop keeps going.
func (s *JobProcessorService) Process(ctx context.Context, job *jobAgg.Job) error {
	jobCtx := logger.WithFields(ctx,
		logger.F("job_id", job.ID().String()),
		logger.F("queue", job.Queue().String()),
		logger.F("task_type", job.TaskType().String()),
		logger.F("attempt", job.AttemptsMade()),
	)

	// Persist the lease the broker handed us before running anything. If the
	// process dies mid-handler, the stored document already shows an active job
	// with a lease, and the reclaim sweep will find it.
	if err := s.jobRepo.Save(jobCtx, job); err != nil {
		return err
	}

	handler, err := s.registry.Resolve(job.TaskType())
	if err != nil {
		// An unroutable job is a configuration problem, not a transient one.
		// Retrying it would burn the whole budget waiting for a deploy that may
		// never come, so it goes straight to the dead-letter queue where an
		// operator can see it.
		return s.kill(jobCtx, job, err)
	}

	startedAt := s.clock.Now()
	handlerErr := s.runHandler(jobCtx, job, handler)
	duration := s.clock.Now().Sub(startedAt)

	s.metrics.RecordJobDuration(job.Queue(), job.TaskType(), duration)

	if handlerErr == nil {
		return s.complete(jobCtx, job, duration)
	}
	return s.fail(jobCtx, job, handlerErr, duration)
}

// runHandler executes the handler under the job's timeout, with a panic barrier.
//
// Both guards exist for the same reason: a worker goroutine is shared
// infrastructure. A handler that blocks forever would hold a pool slot until the
// process restarts, and a handler that panics would take the whole pool down
// with it. Converting each into an ordinary job failure keeps one bad task type
// from becoming an outage.
func (s *JobProcessorService) runHandler(
	ctx context.Context,
	job *jobAgg.Job,
	handler services.Handler,
) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = errors.New(
				errors.KindInternal,
				jobErr.EHandlerPanic,
				fmt.Sprintf("handler panicked: %v", recovered),
			)
			s.log.Error(ctx, "handler panicked", err)
		}
	}()

	handlerCtx, cancel := context.WithTimeout(ctx, job.Timeout())
	defer cancel()

	err = handler.Handle(handlerCtx, services.JobContext{
		ID:         job.ID(),
		Queue:      job.Queue(),
		TaskType:   job.TaskType(),
		Payload:    job.Payload(),
		Attempt:    job.AttemptsMade(),
		MaxRetries: job.RetryPolicy().MaxRetries(),
	})

	// Distinguish "the handler ran out of time" from "the handler returned an
	// error", because they mean different things to whoever reads the dashboard.
	if handlerCtx.Err() == context.DeadlineExceeded {
		return errors.New(
			errors.KindInternal,
			jobErr.EJobTimeout,
			fmt.Sprintf("handler exceeded its %s timeout", job.Timeout()),
		)
	}
	return err
}

// complete records a successful execution.
func (s *JobProcessorService) complete(
	ctx context.Context,
	job *jobAgg.Job,
	duration time.Duration,
) error {
	if err := job.Complete(s.clock.Now()); err != nil {
		return err
	}
	if err := s.broker.Complete(ctx, job, s.cfg.CompletedRetention); err != nil {
		return err
	}

	s.metrics.RecordJobProcessed(job.Queue(), job.TaskType(), metrics.OutcomeSuccess)
	s.log.Info(ctx, "job completed", logger.F("duration_ms", duration.Milliseconds()))
	return nil
}

// fail records a failed execution and lets the aggregate decide what follows.
//
// Notice that this method contains no retry arithmetic. It asks the job what
// happened and reacts; the policy itself lives on the aggregate, where it can be
// tested without a broker and cannot drift between callers.
func (s *JobProcessorService) fail(
	ctx context.Context,
	job *jobAgg.Job,
	cause error,
	duration time.Duration,
) error {
	outcome, err := job.Fail(cause, s.clock.Now())
	if err != nil {
		return err
	}

	reason := failureReason(cause)
	s.metrics.RecordJobFailed(job.Queue(), job.TaskType(), reason)

	if outcome.ShouldRetry {
		if err := s.broker.Retry(ctx, job, outcome.RetryAt); err != nil {
			return err
		}
		s.metrics.RecordJobRetry(job.Queue(), job.TaskType())
		s.metrics.RecordJobProcessed(job.Queue(), job.TaskType(), metrics.OutcomeFailure)

		s.log.Warn(ctx, "job failed, scheduled for retry",
			logger.F("reason", reason),
			logger.F("error", cause.Error()),
			logger.F("retry_at", outcome.RetryAt.Format(time.RFC3339)),
			logger.F("remaining_retries", job.RemainingRetries()),
			logger.F("duration_ms", duration.Milliseconds()),
		)
		return nil
	}

	if err := s.broker.Kill(ctx, job); err != nil {
		return err
	}
	s.metrics.RecordJobProcessed(job.Queue(), job.TaskType(), metrics.OutcomeDead)

	s.log.Error(ctx, "job exhausted its retries and moved to the dead-letter queue", cause,
		logger.F("reason", reason),
		logger.F("attempts_made", job.AttemptsMade()),
	)
	return nil
}

// kill moves a job straight to the dead-letter queue.
func (s *JobProcessorService) kill(ctx context.Context, job *jobAgg.Job, cause error) error {
	if err := job.Kill(cause.Error(), s.clock.Now()); err != nil {
		return err
	}
	if err := s.broker.Kill(ctx, job); err != nil {
		return err
	}

	reason := failureReason(cause)
	s.metrics.RecordJobFailed(job.Queue(), job.TaskType(), reason)
	s.metrics.RecordJobProcessed(job.Queue(), job.TaskType(), metrics.OutcomeDead)

	s.log.Error(ctx, "job moved to the dead-letter queue without retrying", cause,
		logger.F("reason", reason))
	return nil
}

// failureReason extracts a bounded label for the failure metric.
//
// The raw error message must never become a label value: handler errors often
// embed ids or timestamps, and a label with unbounded cardinality will take the
// Prometheus server down long before it tells anyone anything useful.
func failureReason(err error) string {
	if appErr, ok := errors.As(err); ok {
		return appErr.Code
	}
	return "handler_error"
}
