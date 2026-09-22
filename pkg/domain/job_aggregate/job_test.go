package job_aggregate_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	appErrors "github.com/anashasan/goqueue/pkg/common/errors"
	jobAgg "github.com/anashasan/goqueue/pkg/domain/job_aggregate"
	jobErr "github.com/anashasan/goqueue/pkg/domain/job_aggregate/error"
	vo "github.com/anashasan/goqueue/pkg/domain/job_aggregate/value_objects"
)

// baseTime is a fixed instant so every assertion about scheduling is exact
// rather than "roughly now".
var baseTime = time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)

// newTestJob builds a valid job, letting each test override only what it cares
// about. Without this every test would repeat nine fields of setup and the one
// line that matters would be lost in them.
func newTestJob(t *testing.T, mutate ...func(*jobAgg.NewJobParams)) *jobAgg.Job {
	t.Helper()

	params := jobAgg.NewJobParams{
		ID:          "job-1",
		Queue:       vo.DefaultQueueName,
		TaskType:    "email.send",
		Payload:     []byte(`{"to":"a@b.c"}`),
		Priority:    vo.PriorityNormal,
		RetryPolicy: jobAgg.DefaultRetryPolicy(),
		Timeout:     30 * time.Second,
		Now:         baseTime,
	}
	for _, m := range mutate {
		m(&params)
	}

	job, err := jobAgg.NewJob(params)
	require.NoError(t, err)
	return job
}

// assertCode asserts that an error carries a specific domain error code, which
// is the contract a client actually branches on.
func assertCode(t *testing.T, err error, code string) {
	t.Helper()
	require.Error(t, err)

	var appErr *appErrors.AppError
	require.True(t, errors.As(err, &appErr), "expected an *AppError, got %T", err)
	assert.Equal(t, code, appErr.Code)
}

func TestNewJob_StartsPendingWhenDueImmediately(t *testing.T) {
	job := newTestJob(t)

	assert.Equal(t, vo.StatePending, job.State())
	assert.Equal(t, baseTime, job.ProcessAt())
	assert.Zero(t, job.AttemptsMade())
}

func TestNewJob_StartsScheduledWhenDueLater(t *testing.T) {
	runAt := baseTime.Add(time.Hour)
	job := newTestJob(t, func(p *jobAgg.NewJobParams) { p.ProcessAt = runAt })

	assert.Equal(t, vo.StateScheduled, job.State())
	assert.Equal(t, runAt, job.ProcessAt())
}

func TestNewJob_RejectsOversizedPayload(t *testing.T) {
	_, err := jobAgg.NewJob(jobAgg.NewJobParams{
		ID:       "job-1",
		TaskType: "email.send",
		Payload:  make([]byte, jobAgg.MaxPayloadBytes+1),
		Now:      baseTime,
	})

	assertCode(t, err, jobErr.EInvalidPayload)
}

func TestNewJob_DefensivelyCopiesPayload(t *testing.T) {
	payload := []byte(`{"secret":true}`)
	job := newTestJob(t, func(p *jobAgg.NewJobParams) { p.Payload = payload })

	// Mutating the caller's slice must not reach inside the aggregate, or an
	// aggregate's state could change without any method being called on it.
	payload[2] = 'X'

	assert.Equal(t, `{"secret":true}`, string(job.Payload()))
}

func TestLease_RecordsAttemptAndExpiry(t *testing.T) {
	job := newTestJob(t)

	require.NoError(t, job.Lease("worker-1", baseTime, 10*time.Second))

	assert.Equal(t, vo.StateActive, job.State())
	assert.Equal(t, uint32(1), job.AttemptsMade())
	assert.Equal(t, "worker-1", job.WorkerID())

	require.Len(t, job.Attempts(), 1)
	assert.Equal(t, uint32(1), job.Attempts()[0].Number)
	assert.Equal(t, "worker-1", job.Attempts()[0].WorkerID)

	require.NotNil(t, job.LeaseExpiry())
	assert.Equal(t, baseTime.Add(40*time.Second), *job.LeaseExpiry())
}

func TestLease_RejectsAnAlreadyActiveJob(t *testing.T) {
	job := newTestJob(t)
	require.NoError(t, job.Lease("worker-1", baseTime, time.Second))

	// This is the invariant that stops two workers holding the same job.
	err := job.Lease("worker-2", baseTime, time.Second)

	assertCode(t, err, jobErr.EIllegalStateTransition)
	assert.Equal(t, "worker-1", job.WorkerID())
}

func TestComplete_ClosesTheOpenAttempt(t *testing.T) {
	job := newTestJob(t)
	require.NoError(t, job.Lease("worker-1", baseTime, time.Second))

	finishedAt := baseTime.Add(2 * time.Second)
	require.NoError(t, job.Complete(finishedAt))

	assert.Equal(t, vo.StateCompleted, job.State())
	assert.True(t, job.State().IsTerminal())
	require.NotNil(t, job.CompletedAt())
	assert.Equal(t, finishedAt, *job.CompletedAt())
	assert.Nil(t, job.LeaseExpiry())
	assert.Equal(t, 2*time.Second, job.Attempts()[0].Duration())
}

func TestComplete_RejectsAJobThatWasNeverLeased(t *testing.T) {
	job := newTestJob(t)

	err := job.Complete(baseTime)

	assertCode(t, err, jobErr.EIllegalStateTransition)
}

func TestFail_SchedulesARetryWhileBudgetRemains(t *testing.T) {
	job := newTestJob(t, func(p *jobAgg.NewJobParams) {
		policy, err := jobAgg.NewRetryPolicy(3, time.Second, time.Minute, jobAgg.BackoffConstant)
		require.NoError(t, err)
		p.RetryPolicy = policy
	})
	require.NoError(t, job.Lease("worker-1", baseTime, time.Second))

	outcome, err := job.Fail(errors.New("smtp timeout"), baseTime.Add(time.Second))
	require.NoError(t, err)

	assert.True(t, outcome.ShouldRetry)
	assert.Equal(t, uint32(1), outcome.Attempt)
	assert.Equal(t, vo.StateRetrying, job.State())
	assert.Equal(t, "smtp timeout", job.LastError())
	// Constant backoff, so the retry lands exactly one base delay later.
	assert.Equal(t, baseTime.Add(2*time.Second), outcome.RetryAt)
	assert.Equal(t, uint32(2), job.RemainingRetries())
}

func TestFail_MovesToDeadWhenRetriesAreExhausted(t *testing.T) {
	job := newTestJob(t, func(p *jobAgg.NewJobParams) {
		policy, err := jobAgg.NewRetryPolicy(1, time.Second, time.Minute, jobAgg.BackoffConstant)
		require.NoError(t, err)
		p.RetryPolicy = policy
	})

	// Attempt 1 fails and retries; attempt 2 fails and exhausts the budget.
	require.NoError(t, job.Lease("worker-1", baseTime, time.Second))
	first, err := job.Fail(errors.New("boom"), baseTime)
	require.NoError(t, err)
	require.True(t, first.ShouldRetry)

	require.NoError(t, job.Promote(baseTime))
	require.NoError(t, job.Lease("worker-1", baseTime, time.Second))
	second, err := job.Fail(errors.New("boom again"), baseTime)
	require.NoError(t, err)

	assert.False(t, second.ShouldRetry)
	assert.Equal(t, vo.StateDead, job.State())
	assert.NotNil(t, job.DiedAt())
	assert.Zero(t, job.RemainingRetries())
	assert.Len(t, job.Attempts(), 2)
}

func TestFail_RejectsAJobThatIsNotActive(t *testing.T) {
	job := newTestJob(t)

	_, err := job.Fail(errors.New("boom"), baseTime)

	assertCode(t, err, jobErr.EIllegalStateTransition)
}

func TestRequeue_ResetsTheRetryBudget(t *testing.T) {
	job := newTestJob(t, func(p *jobAgg.NewJobParams) {
		policy, err := jobAgg.NewRetryPolicy(0, time.Second, time.Minute, jobAgg.BackoffConstant)
		require.NoError(t, err)
		p.RetryPolicy = policy
	})
	require.NoError(t, job.Lease("worker-1", baseTime, time.Second))
	_, err := job.Fail(errors.New("boom"), baseTime)
	require.NoError(t, err)
	require.Equal(t, vo.StateDead, job.State())

	requeuedAt := baseTime.Add(time.Hour)
	require.NoError(t, job.Requeue(requeuedAt))

	assert.Equal(t, vo.StatePending, job.State())
	assert.Zero(t, job.AttemptsMade())
	assert.Nil(t, job.DiedAt())
	assert.Empty(t, job.WorkerID())
	assert.Equal(t, requeuedAt, job.ProcessAt())
}

func TestRequeue_RejectsAJobThatIsNotDead(t *testing.T) {
	job := newTestJob(t)

	err := job.Requeue(baseTime)

	assertCode(t, err, jobErr.EJobNotRetryable)
}

func TestReclaimExpiredLease_ReturnsAnOrphanedJobToPending(t *testing.T) {
	job := newTestJob(t)
	require.NoError(t, job.Lease("worker-1", baseTime, 10*time.Second))

	// The lease is timeout (30s) + grace (10s) = 40s from baseTime.
	assert.False(t, job.IsLeaseExpired(baseTime.Add(39*time.Second)))
	assert.True(t, job.IsLeaseExpired(baseTime.Add(41*time.Second)))

	reclaimedAt := baseTime.Add(41 * time.Second)
	require.NoError(t, job.ReclaimExpiredLease(reclaimedAt))

	assert.Equal(t, vo.StatePending, job.State())
	assert.Empty(t, job.WorkerID())
	assert.Nil(t, job.LeaseExpiry())
	assert.Contains(t, job.LastError(), "lease expired")
	// The attempt is kept and stays counted: the worker really did try once, so
	// an orphaned job must not get an unlimited retry budget.
	assert.Equal(t, uint32(1), job.AttemptsMade())
	assert.NotZero(t, job.Attempts()[0].FinishedAt)
}

func TestKill_SkipsRemainingRetries(t *testing.T) {
	job := newTestJob(t)
	require.NoError(t, job.Lease("worker-1", baseTime, time.Second))

	require.NoError(t, job.Kill("killed by operator", baseTime))

	assert.Equal(t, vo.StateDead, job.State())
	assert.Equal(t, "killed by operator", job.LastError())
	assert.NotNil(t, job.DiedAt())
}

func TestIsDue(t *testing.T) {
	job := newTestJob(t, func(p *jobAgg.NewJobParams) {
		p.ProcessAt = baseTime.Add(time.Minute)
	})

	assert.False(t, job.IsDue(baseTime))
	assert.True(t, job.IsDue(baseTime.Add(time.Minute)))
	assert.True(t, job.IsDue(baseTime.Add(2*time.Minute)))
}
