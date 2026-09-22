package processor_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anashasan/goqueue/pkg/application/processor"
	"github.com/anashasan/goqueue/pkg/application/services"
	"github.com/anashasan/goqueue/pkg/common/clock"
	"github.com/anashasan/goqueue/pkg/common/logger"
	jobAgg "github.com/anashasan/goqueue/pkg/domain/job_aggregate"
	jobErr "github.com/anashasan/goqueue/pkg/domain/job_aggregate/error"
	jobVO "github.com/anashasan/goqueue/pkg/domain/job_aggregate/value_objects"
	"github.com/anashasan/goqueue/pkg/domain/metrics"
	"github.com/anashasan/goqueue/pkg/domain/persistence/fake"
)

var testTime = time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)

// spyRecorder captures the measurements the processor emits, so the tests can
// assert that the dashboard would show the truth — not just that the job ended
// in the right state.
type spyRecorder struct {
	metrics.NopRecorder
	processed map[metrics.Outcome]int
	retries   int
	failures  map[string]int
	durations []time.Duration
}

func newSpyRecorder() *spyRecorder {
	return &spyRecorder{
		processed: make(map[metrics.Outcome]int),
		failures:  make(map[string]int),
	}
}

func (s *spyRecorder) RecordJobProcessed(_ jobVO.QueueName, _ jobVO.TaskType, outcome metrics.Outcome) {
	s.processed[outcome]++
}

func (s *spyRecorder) RecordJobRetry(_ jobVO.QueueName, _ jobVO.TaskType) { s.retries++ }

func (s *spyRecorder) RecordJobFailed(_ jobVO.QueueName, _ jobVO.TaskType, reason string) {
	s.failures[reason]++
}

func (s *spyRecorder) RecordJobDuration(_ jobVO.QueueName, _ jobVO.TaskType, d time.Duration) {
	s.durations = append(s.durations, d)
}

// harness bundles everything a processor test needs.
type harness struct {
	svc      *processor.JobProcessorService
	store    *fake.Store
	registry *processor.HandlerRegistry
	spy      *spyRecorder
}

func newHarness(t *testing.T) *harness {
	t.Helper()

	store := fake.NewStore()
	registry := processor.NewHandlerRegistry()
	spy := newSpyRecorder()

	svc := processor.NewJobProcessorService(
		registry, store, store, spy,
		clock.NewSystemClock(),
		logger.NewNop(),
		processor.Config{CompletedRetention: time.Hour},
	)
	return &harness{svc: svc, store: store, registry: registry, spy: spy}
}

// leasedJob builds a job already leased to a worker, which is the state the
// processor always receives one in.
func leasedJob(t *testing.T, taskType string, maxRetries uint32) *jobAgg.Job {
	t.Helper()

	policy, err := jobAgg.NewRetryPolicy(maxRetries, time.Millisecond, time.Second, jobAgg.BackoffConstant)
	require.NoError(t, err)

	job, err := jobAgg.NewJob(jobAgg.NewJobParams{
		ID:          "job-1",
		Queue:       jobVO.DefaultQueueName,
		TaskType:    jobVO.TaskType(taskType),
		Priority:    jobVO.PriorityNormal,
		RetryPolicy: policy,
		Timeout:     time.Second,
		Now:         testTime,
	})
	require.NoError(t, err)
	require.NoError(t, job.Lease("worker-1", time.Now().UTC(), time.Second))
	return job
}

func register(t *testing.T, registry *processor.HandlerRegistry, taskType string, fn services.HandlerFunc) {
	t.Helper()
	require.NoError(t, registry.Register(jobVO.TaskType(taskType), fn))
}

func TestProcess_CompletesASuccessfulJob(t *testing.T) {
	h := newHarness(t)
	register(t, h.registry, "email.send", func(context.Context, services.JobContext) error {
		return nil
	})
	job := leasedJob(t, "email.send", 3)

	require.NoError(t, h.svc.Process(context.Background(), job))

	assert.Equal(t, jobVO.StateCompleted, job.State())
	assert.Equal(t, 1, h.spy.processed[metrics.OutcomeSuccess])
	assert.Zero(t, h.spy.retries)
	assert.Len(t, h.spy.durations, 1)
}

func TestProcess_RetriesAFailedJobWhileBudgetRemains(t *testing.T) {
	h := newHarness(t)
	register(t, h.registry, "demo.flaky", func(context.Context, services.JobContext) error {
		return errors.New("downstream unavailable")
	})
	job := leasedJob(t, "demo.flaky", 3)

	// A handler error is an expected outcome, so Process reports no error: only
	// a broken broker or store is a processing failure.
	require.NoError(t, h.svc.Process(context.Background(), job))

	assert.Equal(t, jobVO.StateRetrying, job.State())
	assert.Equal(t, "downstream unavailable", job.LastError())
	assert.Equal(t, 1, h.spy.retries)
	assert.Equal(t, 1, h.spy.processed[metrics.OutcomeFailure])
	assert.Equal(t, 1, h.spy.failures["handler_error"])
}

func TestProcess_DeadLettersAJobWithNoRetriesLeft(t *testing.T) {
	h := newHarness(t)
	register(t, h.registry, "demo.flaky", func(context.Context, services.JobContext) error {
		return errors.New("still broken")
	})
	job := leasedJob(t, "demo.flaky", 0)

	require.NoError(t, h.svc.Process(context.Background(), job))

	assert.Equal(t, jobVO.StateDead, job.State())
	assert.Equal(t, 1, h.spy.processed[metrics.OutcomeDead])
	assert.Zero(t, h.spy.retries)
}

func TestProcess_DeadLettersAnUnroutableJobWithoutRetrying(t *testing.T) {
	h := newHarness(t)
	job := leasedJob(t, "unregistered.task", 5)

	require.NoError(t, h.svc.Process(context.Background(), job))

	// Retrying would burn the whole budget waiting for a deploy that may never
	// happen, so an unroutable job goes straight to the dead-letter queue — with
	// its retry budget deliberately untouched, so an operator who deploys the
	// missing handler can requeue it and get the full number of attempts.
	assert.Equal(t, jobVO.StateDead, job.State())
	assert.Zero(t, h.spy.retries)
	assert.Equal(t, 1, h.spy.processed[metrics.OutcomeDead])
	assert.Equal(t, 1, h.spy.failures[jobErr.EHandlerNotFound])
}

func TestProcess_TreatsAHandlerTimeoutAsAFailure(t *testing.T) {
	h := newHarness(t)
	register(t, h.registry, "demo.slow", func(ctx context.Context, _ services.JobContext) error {
		// A well-behaved handler honours cancellation; the timeout is what
		// triggers it.
		<-ctx.Done()
		return ctx.Err()
	})

	policy, err := jobAgg.NewRetryPolicy(2, time.Millisecond, time.Second, jobAgg.BackoffConstant)
	require.NoError(t, err)

	job, err := jobAgg.NewJob(jobAgg.NewJobParams{
		ID:          "job-slow",
		TaskType:    "demo.slow",
		RetryPolicy: policy,
		Timeout:     50 * time.Millisecond,
		Now:         testTime,
	})
	require.NoError(t, err)
	require.NoError(t, job.Lease("worker-1", time.Now().UTC(), time.Second))

	require.NoError(t, h.svc.Process(context.Background(), job))

	assert.Equal(t, jobVO.StateRetrying, job.State())
	// Labelled separately from a plain handler error, because a timeout and a
	// bug need different fixes.
	assert.Equal(t, 1, h.spy.failures[jobErr.EJobTimeout])
}

func TestProcess_ContainsAPanickingHandler(t *testing.T) {
	h := newHarness(t)
	register(t, h.registry, "demo.panic", func(context.Context, services.JobContext) error {
		panic("handler blew up")
	})
	job := leasedJob(t, "demo.panic", 2)

	// The point of the assertion is that this call returns at all: without the
	// panic barrier it would take the whole worker pool down with it.
	require.NotPanics(t, func() {
		require.NoError(t, h.svc.Process(context.Background(), job))
	})

	assert.Equal(t, jobVO.StateRetrying, job.State())
	assert.Contains(t, job.LastError(), "handler blew up")
	assert.Equal(t, 1, h.spy.failures[jobErr.EHandlerPanic])
}

func TestProcess_PassesTheAttemptNumberToTheHandler(t *testing.T) {
	h := newHarness(t)
	var seen services.JobContext

	register(t, h.registry, "email.send", func(_ context.Context, jc services.JobContext) error {
		seen = jc
		return nil
	})
	job := leasedJob(t, "email.send", 3)

	require.NoError(t, h.svc.Process(context.Background(), job))

	assert.Equal(t, uint32(1), seen.Attempt)
	assert.Equal(t, uint32(3), seen.MaxRetries)
	assert.Equal(t, jobVO.JobID("job-1"), seen.ID)
}
