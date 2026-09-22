package inspector_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anashasan/goqueue/pkg/application/inspector"
	"github.com/anashasan/goqueue/pkg/common/clock"
	appErrors "github.com/anashasan/goqueue/pkg/common/errors"
	"github.com/anashasan/goqueue/pkg/common/logger"
	jobContr "github.com/anashasan/goqueue/pkg/contracts/job"
	jobAgg "github.com/anashasan/goqueue/pkg/domain/job_aggregate"
	jobErr "github.com/anashasan/goqueue/pkg/domain/job_aggregate/error"
	jobVO "github.com/anashasan/goqueue/pkg/domain/job_aggregate/value_objects"
	"github.com/anashasan/goqueue/pkg/domain/persistence/fake"
	queueErr "github.com/anashasan/goqueue/pkg/domain/queue_aggregate/error"
)

var testTime = time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)

func newService(t *testing.T) (*inspector.InspectorService, *fake.Store, *clock.Fake) {
	t.Helper()

	store := fake.NewStore()
	clk := clock.NewFake(testTime)

	svc := inspector.NewInspectorService(store, store, store, clk, logger.NewNop())
	return svc, store, clk
}

func assertCode(t *testing.T, err error, code string) {
	t.Helper()
	require.Error(t, err)

	var appErr *appErrors.AppError
	require.True(t, errors.As(err, &appErr), "expected an *AppError, got %T", err)
	assert.Equal(t, code, appErr.Code)
}

// seed stores a job in the given state, driving it there through real domain
// transitions rather than constructing the state directly — so the fixture
// itself proves the lifecycle is reachable.
func seed(t *testing.T, store *fake.Store, id, queue, taskType string, state jobVO.JobState) *jobAgg.Job {
	t.Helper()
	ctx := context.Background()

	policy, err := jobAgg.NewRetryPolicy(0, time.Second, time.Minute, jobAgg.BackoffConstant)
	require.NoError(t, err)

	job, err := jobAgg.NewJob(jobAgg.NewJobParams{
		ID:          jobVO.JobID(id),
		Queue:       jobVO.QueueName(queue),
		TaskType:    jobVO.TaskType(taskType),
		Priority:    jobVO.PriorityNormal,
		RetryPolicy: policy,
		Timeout:     time.Second,
		Now:         testTime,
	})
	require.NoError(t, err)

	switch state {
	case jobVO.StatePending:
	case jobVO.StateActive:
		require.NoError(t, job.Lease("worker-1", testTime, time.Second))
	case jobVO.StateCompleted:
		require.NoError(t, job.Lease("worker-1", testTime, time.Second))
		require.NoError(t, job.Complete(testTime))
	case jobVO.StateDead:
		require.NoError(t, job.Lease("worker-1", testTime, time.Second))
		_, err := job.Fail(errors.New("boom"), testTime)
		require.NoError(t, err)
	default:
		t.Fatalf("unsupported seed state %s", state)
	}

	require.NoError(t, store.Save(ctx, job))
	return job
}

func TestGetJob_ReturnsTheFullRepresentation(t *testing.T) {
	svc, store, _ := newService(t)
	seed(t, store, "job-1", "default", "email.send", jobVO.StateCompleted)

	res, err := svc.GetJob(context.Background(), "job-1")
	require.NoError(t, err)

	assert.Equal(t, "job-1", res.ID)
	assert.Equal(t, "completed", res.State)
	assert.Equal(t, "email.send", res.TaskType)
	assert.NotEmpty(t, res.CompletedAt)
	// The attempt trail is what an operator actually opens a job to read.
	require.Len(t, res.Attempts, 1)
	assert.Equal(t, uint32(1), res.Attempts[0].Number)
}

func TestGetJob_ReportsAnUnknownID(t *testing.T) {
	svc, _, _ := newService(t)

	_, err := svc.GetJob(context.Background(), "nope")

	assertCode(t, err, jobErr.EJobNotFound)
}

func TestGetJob_RejectsAMalformedID(t *testing.T) {
	svc, _, _ := newService(t)

	// A malformed id is a bad request, not a missing resource: the two map to
	// different status codes and a client should be able to tell them apart.
	_, err := svc.GetJob(context.Background(), "bad id with spaces")

	assertCode(t, err, jobErr.EInvalidJobID)
}

func TestListJobs_FiltersByStateAndQueue(t *testing.T) {
	svc, store, _ := newService(t)
	seed(t, store, "job-dead-1", "default", "email.send", jobVO.StateDead)
	seed(t, store, "job-dead-2", "critical", "email.send", jobVO.StateDead)
	seed(t, store, "job-pending", "default", "email.send", jobVO.StatePending)

	byState, err := svc.ListJobs(context.Background(), jobContr.ListJobsQuery{State: "dead"})
	require.NoError(t, err)
	assert.Equal(t, int64(2), byState.Total)

	byQueue, err := svc.ListJobs(context.Background(), jobContr.ListJobsQuery{
		State: "dead", Queue: "critical",
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), byQueue.Total)
	assert.Equal(t, "job-dead-2", byQueue.Jobs[0].ID)
}

func TestListJobs_Paginates(t *testing.T) {
	svc, store, _ := newService(t)
	for i := 0; i < 5; i++ {
		seed(t, store, "job-"+string(rune('a'+i)), "default", "email.send", jobVO.StatePending)
	}

	page, err := svc.ListJobs(context.Background(), jobContr.ListJobsQuery{Limit: 2, Offset: 2})
	require.NoError(t, err)

	assert.Len(t, page.Jobs, 2)
	assert.Equal(t, int64(5), page.Total)
	assert.Equal(t, int64(2), page.Offset)
	assert.Equal(t, int64(2), page.Limit)
}

func TestListJobs_RejectsAnUnknownState(t *testing.T) {
	svc, _, _ := newService(t)

	_, err := svc.ListJobs(context.Background(), jobContr.ListJobsQuery{State: "zombie"})

	assertCode(t, err, jobErr.EInvalidJobState)
}

func TestRetryJob_RequeuesADeadJobWithAFreshBudget(t *testing.T) {
	svc, store, clk := newService(t)
	seed(t, store, "job-dead", "default", "email.send", jobVO.StateDead)
	clk.Advance(time.Hour)

	res, err := svc.RetryJob(context.Background(), "job-dead")
	require.NoError(t, err)
	assert.Equal(t, "pending", res.State)

	stored, err := store.FindByID(context.Background(), "job-dead")
	require.NoError(t, err)
	assert.Equal(t, jobVO.StatePending, stored.State())
	assert.Zero(t, stored.AttemptsMade())
}

func TestRetryJob_RefusesAJobThatIsNotDead(t *testing.T) {
	svc, store, _ := newService(t)
	seed(t, store, "job-pending", "default", "email.send", jobVO.StatePending)

	// The rule lives on the aggregate; this asserts the service does not
	// second-guess it or quietly succeed.
	_, err := svc.RetryJob(context.Background(), "job-pending")

	assertCode(t, err, jobErr.EJobNotRetryable)
}

func TestKillJob_MovesAnActiveJobToTheDeadLetterQueue(t *testing.T) {
	svc, store, _ := newService(t)
	seed(t, store, "job-active", "default", "demo.slow", jobVO.StateActive)

	res, err := svc.KillJob(context.Background(), "job-active")
	require.NoError(t, err)
	assert.Equal(t, "dead", res.State)

	stored, err := store.FindByID(context.Background(), "job-active")
	require.NoError(t, err)
	assert.Equal(t, jobVO.StateDead, stored.State())
	assert.Equal(t, "killed by operator", stored.LastError())
}

func TestDeleteJob_RemovesTheJob(t *testing.T) {
	svc, store, _ := newService(t)
	seed(t, store, "job-1", "default", "email.send", jobVO.StatePending)

	require.NoError(t, svc.DeleteJob(context.Background(), "job-1"))

	_, err := store.FindByID(context.Background(), "job-1")
	assertCode(t, err, jobErr.EJobNotFound)
}

func TestRetryAllDead_RequeuesEveryDeadJobInTheQueue(t *testing.T) {
	svc, store, _ := newService(t)
	seed(t, store, "job-dead-1", "default", "email.send", jobVO.StateDead)
	seed(t, store, "job-dead-2", "default", "email.send", jobVO.StateDead)
	seed(t, store, "job-dead-other", "critical", "email.send", jobVO.StateDead)
	seed(t, store, "job-ok", "default", "email.send", jobVO.StateCompleted)

	res, err := svc.RetryAllDead(context.Background(), "default")
	require.NoError(t, err)

	// Scoped to the named queue, and only dead jobs.
	assert.Equal(t, int64(2), res.Affected)

	stillDead, err := store.FindByID(context.Background(), "job-dead-other")
	require.NoError(t, err)
	assert.Equal(t, jobVO.StateDead, stillDead.State())
}

func TestListQueueStats_RollsUpTotals(t *testing.T) {
	svc, store, _ := newService(t)
	seed(t, store, "job-1", "default", "email.send", jobVO.StatePending)
	seed(t, store, "job-2", "default", "email.send", jobVO.StateCompleted)
	seed(t, store, "job-3", "critical", "email.send", jobVO.StateDead)

	res, err := svc.ListQueueStats(context.Background())
	require.NoError(t, err)

	assert.Len(t, res.Queues, 2)
	assert.Equal(t, int64(1), res.Totals.Pending)
	assert.Equal(t, int64(1), res.Totals.Completed)
	assert.Equal(t, int64(1), res.Totals.Dead)
	assert.Equal(t, int64(3), res.Totals.Total)
	// Backlog excludes terminal states: only the pending job is owed work.
	assert.Equal(t, int64(1), res.Totals.Backlog)
}

func TestSetQueuePaused_TogglesConsumption(t *testing.T) {
	svc, store, _ := newService(t)
	seed(t, store, "job-1", "default", "email.send", jobVO.StatePending)

	paused, err := svc.SetQueuePaused(context.Background(), "default", true)
	require.NoError(t, err)
	assert.True(t, paused.Paused)

	queue, err := store.FindByName(context.Background(), jobVO.DefaultQueueName)
	require.NoError(t, err)
	assert.True(t, queue.IsPaused())

	resumed, err := svc.SetQueuePaused(context.Background(), "default", false)
	require.NoError(t, err)
	assert.False(t, resumed.Paused)
}

func TestSetQueuePaused_ReportsAnUnknownQueue(t *testing.T) {
	svc, _, _ := newService(t)

	_, err := svc.SetQueuePaused(context.Background(), "ghost", true)

	assertCode(t, err, queueErr.EQueueNotFound)
}
