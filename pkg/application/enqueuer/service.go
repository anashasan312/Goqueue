// Package enqueuer implements the write side of the job API.
package enqueuer

import (
	"context"
	"time"

	"github.com/anashasan/goqueue/pkg/application/services"
	"github.com/anashasan/goqueue/pkg/common/clock"
	"github.com/anashasan/goqueue/pkg/common/errors"
	"github.com/anashasan/goqueue/pkg/common/logger"
	"github.com/anashasan/goqueue/pkg/common/uid"
	jobContr "github.com/anashasan/goqueue/pkg/contracts/job"
	jobAgg "github.com/anashasan/goqueue/pkg/domain/job_aggregate"
	jobErr "github.com/anashasan/goqueue/pkg/domain/job_aggregate/error"
	jobVO "github.com/anashasan/goqueue/pkg/domain/job_aggregate/value_objects"
	"github.com/anashasan/goqueue/pkg/domain/persistence"
	"github.com/anashasan/goqueue/pkg/hydrator"
)

var _ services.IEnqueuerService = (*EnqueuerService)(nil)

// Config tunes enqueue behaviour.
type Config struct {
	// IdempotencyTTL is how long a deduplication claim is held. It should
	// comfortably exceed the longest retry window a client might use, or a
	// retried request could slip past deduplication.
	IdempotencyTTL time.Duration
}

// EnqueuerService turns a client request into a queued job.
//
// Its dependencies are all domain interfaces. Nothing here knows that the store
// is Redis, which is why the whole service is testable with in-memory fakes.
type EnqueuerService struct {
	broker      persistence.IJobBroker
	idempotency persistence.IIdempotencyStore
	jobRepo     persistence.IJobRepo
	ids         uid.Generator
	clock       clock.Clock
	log         logger.Logger
	cfg         Config
}

// NewEnqueuerService builds an EnqueuerService.
func NewEnqueuerService(
	broker persistence.IJobBroker,
	idempotency persistence.IIdempotencyStore,
	jobRepo persistence.IJobRepo,
	ids uid.Generator,
	clk clock.Clock,
	log logger.Logger,
	cfg Config,
) *EnqueuerService {
	if cfg.IdempotencyTTL <= 0 {
		cfg.IdempotencyTTL = 24 * time.Hour
	}
	return &EnqueuerService{
		broker:      broker,
		idempotency: idempotency,
		jobRepo:     jobRepo,
		ids:         ids,
		clock:       clk,
		log:         log,
		cfg:         cfg,
	}
}

// Enqueue validates a request, deduplicates it, and places the job on its queue.
//
// The order of operations is deliberate: claim the idempotency key *before*
// writing the job. If the write then fails, the claim is released, so a failed
// enqueue does not poison the key for the next 24 hours. Claiming after the
// write would leave a window in which two concurrent requests both succeed.
func (s *EnqueuerService) Enqueue(
	ctx context.Context,
	req jobContr.EnqueueJobReq,
) (*jobContr.EnqueueJobRes, error) {
	job, err := s.buildJob(req)
	if err != nil {
		return nil, err
	}

	if !job.IdempotencyKey().IsZero() {
		claimed, existingID, err := s.idempotency.Claim(
			ctx, job.IdempotencyKey(), job.ID(), s.cfg.IdempotencyTTL,
		)
		if err != nil {
			return nil, err
		}
		if !claimed {
			return s.resolveDuplicate(ctx, job.IdempotencyKey(), existingID)
		}
	}

	if err := s.persist(ctx, job); err != nil {
		// Roll the claim back so the caller's next attempt is not rejected as a
		// duplicate of a job that was never created.
		if !job.IdempotencyKey().IsZero() {
			if releaseErr := s.idempotency.Release(ctx, job.IdempotencyKey()); releaseErr != nil {
				s.log.Error(ctx, "failed to release idempotency claim after enqueue failure", releaseErr,
					logger.F("job_id", job.ID().String()))
			}
		}
		return nil, err
	}

	s.log.Info(ctx, "job enqueued",
		logger.F("job_id", job.ID().String()),
		logger.F("queue", job.Queue().String()),
		logger.F("task_type", job.TaskType().String()),
		logger.F("state", job.State().String()),
	)

	res := hydrator.ToEnqueueJobRes(job, false)
	return &res, nil
}

// persist routes the job to the right broker call for its initial state.
func (s *EnqueuerService) persist(ctx context.Context, job *jobAgg.Job) error {
	if job.State() == jobVO.StateScheduled {
		return s.broker.Schedule(ctx, job)
	}
	return s.broker.Enqueue(ctx, job)
}

// resolveDuplicate answers a request whose idempotency key was already held.
//
// Returning the original job, rather than an error, is what makes the endpoint
// safe to retry: a client that times out and resends gets the same job id back
// and can carry on as if the first call had succeeded.
func (s *EnqueuerService) resolveDuplicate(
	ctx context.Context,
	key jobVO.IdempotencyKey,
	existingID jobVO.JobID,
) (*jobContr.EnqueueJobRes, error) {
	if existingID.IsZero() {
		return nil, errors.Conflict(
			jobErr.EDuplicateJob,
			"idempotency key "+key.String()+" is already in use",
		)
	}

	existing, err := s.jobRepo.FindByID(ctx, existingID)
	if err != nil {
		// The claim outlived the job it protected, which happens when a
		// completed job's retention expires before the key does. Report the
		// conflict rather than inventing a response for a job that is gone.
		if errors.KindOf(err) == errors.KindNotFound {
			return nil, errors.Conflict(
				jobErr.EDuplicateJob,
				"idempotency key "+key.String()+" is already in use",
			)
		}
		return nil, err
	}

	s.log.Info(ctx, "enqueue deduplicated",
		logger.F("job_id", existing.ID().String()),
		logger.F("idempotency_key", key.String()),
	)

	res := hydrator.ToEnqueueJobRes(existing, true)
	return &res, nil
}

// buildJob translates the request DTO into a validated aggregate.
//
// Every field is pushed through its value object rather than validated ad hoc,
// so the rules a job must satisfy are identical whether it arrived over HTTP or
// was constructed by a test.
func (s *EnqueuerService) buildJob(req jobContr.EnqueueJobReq) (*jobAgg.Job, error) {
	taskType, err := jobVO.NewTaskType(req.TaskType)
	if err != nil {
		return nil, err
	}
	queue, err := jobVO.NewQueueName(req.Queue)
	if err != nil {
		return nil, err
	}
	priority, err := jobVO.ParsePriority(req.Priority)
	if err != nil {
		return nil, err
	}
	idempotencyKey, err := jobVO.NewIdempotencyKey(req.IdempotencyKey)
	if err != nil {
		return nil, err
	}
	id, err := jobVO.NewJobID(s.ids.New())
	if err != nil {
		return nil, err
	}

	now := s.clock.Now()
	processAt, err := resolveProcessAt(req, now)
	if err != nil {
		return nil, err
	}

	policy, err := jobAgg.NewRetryPolicy(
		derefOr(req.MaxRetries, jobAgg.DefaultMaxRetries),
		secondsOr(req.RetryBaseDelaySeconds, jobAgg.DefaultBaseDelay),
		secondsOr(req.RetryMaxDelaySeconds, jobAgg.DefaultMaxDelay),
		req.BackoffStrategy,
	)
	if err != nil {
		return nil, err
	}

	return jobAgg.NewJob(jobAgg.NewJobParams{
		ID:             id,
		Queue:          queue,
		TaskType:       taskType,
		Payload:        req.Payload,
		Priority:       priority,
		RetryPolicy:    policy,
		IdempotencyKey: idempotencyKey,
		Timeout:        secondsOr(req.TimeoutSeconds, jobAgg.DefaultTimeout),
		ProcessAt:      processAt,
		Now:            now,
	})
}

// resolveProcessAt turns the two mutually exclusive scheduling fields into one
// instant, rejecting a request that sets both rather than silently picking one.
func resolveProcessAt(req jobContr.EnqueueJobReq, now time.Time) (time.Time, error) {
	if req.DelaySeconds != nil && req.RunAt != nil {
		return time.Time{}, errors.Invalid(
			jobErr.EInvalidScheduleTime,
			"delay_seconds and run_at are mutually exclusive",
		)
	}

	switch {
	case req.DelaySeconds != nil:
		return now.Add(time.Duration(*req.DelaySeconds) * time.Second), nil

	case req.RunAt != nil:
		parsed, err := time.Parse(time.RFC3339, *req.RunAt)
		if err != nil {
			return time.Time{}, errors.Invalid(
				jobErr.EInvalidScheduleTime,
				"run_at must be a valid RFC3339 timestamp",
			)
		}
		return parsed.UTC(), nil

	default:
		return now, nil
	}
}

// derefOr reads an optional value with a fallback.
func derefOr(v *uint32, fallback uint32) uint32 {
	if v == nil {
		return fallback
	}
	return *v
}

// secondsOr reads an optional second count as a Duration with a fallback.
func secondsOr(v *int64, fallback time.Duration) time.Duration {
	if v == nil || *v <= 0 {
		return fallback
	}
	return time.Duration(*v) * time.Second
}
