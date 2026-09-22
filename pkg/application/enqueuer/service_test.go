package enqueuer_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anashasan/goqueue/pkg/application/enqueuer"
	"github.com/anashasan/goqueue/pkg/common/clock"
	appErrors "github.com/anashasan/goqueue/pkg/common/errors"
	"github.com/anashasan/goqueue/pkg/common/logger"
	"github.com/anashasan/goqueue/pkg/common/uid"
	jobContr "github.com/anashasan/goqueue/pkg/contracts/job"
	jobErr "github.com/anashasan/goqueue/pkg/domain/job_aggregate/error"
	jobVO "github.com/anashasan/goqueue/pkg/domain/job_aggregate/value_objects"
	"github.com/anashasan/goqueue/pkg/domain/persistence"
	"github.com/anashasan/goqueue/pkg/domain/persistence/fake"
)

var testTime = time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)

// newService builds the service under test against in-memory fakes and a frozen
// clock. There is no Redis, no network and no sleeping in any test below, which
// is the point of having the service depend on interfaces.
func newService(t *testing.T) (*enqueuer.EnqueuerService, *fake.Store, *clock.Fake) {
	t.Helper()

	store := fake.NewStore()
	clk := clock.NewFake(testTime)

	svc := enqueuer.NewEnqueuerService(
		store, store, store,
		uid.NewSequenceGenerator("job"),
		clk,
		logger.NewNop(),
		enqueuer.Config{IdempotencyTTL: time.Hour},
	)
	return svc, store, clk
}

func assertCode(t *testing.T, err error, code string) {
	t.Helper()
	require.Error(t, err)

	var appErr *appErrors.AppError
	require.True(t, errors.As(err, &appErr), "expected an *AppError, got %T", err)
	assert.Equal(t, code, appErr.Code)
}

func TestEnqueue_CreatesAPendingJob(t *testing.T) {
	svc, store, _ := newService(t)

	res, err := svc.Enqueue(context.Background(), jobContr.EnqueueJobReq{
		TaskType: "email.send",
		Queue:    "critical",
		Priority: "high",
		Payload:  []byte(`{"to":"a@b.c"}`),
	})
	require.NoError(t, err)

	assert.Equal(t, "job-1", res.ID)
	assert.Equal(t, "critical", res.Queue)
	assert.Equal(t, "pending", res.State)
	assert.Equal(t, "high", res.Priority)
	assert.False(t, res.Deduplicated)

	stored, err := store.FindByID(context.Background(), jobVO.JobID("job-1"))
	require.NoError(t, err)
	assert.Equal(t, jobVO.StatePending, stored.State())
	assert.Equal(t, `{"to":"a@b.c"}`, string(stored.Payload()))
}

func TestEnqueue_DefaultsTheQueueAndPriority(t *testing.T) {
	svc, _, _ := newService(t)

	res, err := svc.Enqueue(context.Background(), jobContr.EnqueueJobReq{
		TaskType: "email.send",
	})
	require.NoError(t, err)

	assert.Equal(t, jobVO.DefaultQueueName, res.Queue)
	assert.Equal(t, "normal", res.Priority)
}

func TestEnqueue_SchedulesAJobWithADelay(t *testing.T) {
	svc, store, _ := newService(t)
	delay := int64(300)

	res, err := svc.Enqueue(context.Background(), jobContr.EnqueueJobReq{
		TaskType:     "report.generate",
		DelaySeconds: &delay,
	})
	require.NoError(t, err)

	assert.Equal(t, "scheduled", res.State)

	stored, err := store.FindByID(context.Background(), jobVO.JobID(res.ID))
	require.NoError(t, err)
	assert.Equal(t, testTime.Add(5*time.Minute), stored.ProcessAt())
}

func TestEnqueue_SchedulesAJobAtAnAbsoluteInstant(t *testing.T) {
	svc, store, _ := newService(t)
	runAt := testTime.Add(2 * time.Hour).Format(time.RFC3339)

	res, err := svc.Enqueue(context.Background(), jobContr.EnqueueJobReq{
		TaskType: "report.generate",
		RunAt:    &runAt,
	})
	require.NoError(t, err)

	assert.Equal(t, "scheduled", res.State)

	stored, err := store.FindByID(context.Background(), jobVO.JobID(res.ID))
	require.NoError(t, err)
	assert.Equal(t, testTime.Add(2*time.Hour), stored.ProcessAt())
}

func TestEnqueue_RejectsBothSchedulingFieldsAtOnce(t *testing.T) {
	svc, _, _ := newService(t)
	delay := int64(60)
	runAt := testTime.Add(time.Hour).Format(time.RFC3339)

	// Silently preferring one would make the API lie about what it did.
	_, err := svc.Enqueue(context.Background(), jobContr.EnqueueJobReq{
		TaskType:     "email.send",
		DelaySeconds: &delay,
		RunAt:        &runAt,
	})

	assertCode(t, err, jobErr.EInvalidScheduleTime)
}

func TestEnqueue_RejectsAnInvalidTaskType(t *testing.T) {
	svc, _, _ := newService(t)

	_, err := svc.Enqueue(context.Background(), jobContr.EnqueueJobReq{TaskType: ""})

	assertCode(t, err, jobErr.EInvalidTaskType)
}

func TestEnqueue_RejectsAnInvalidBackoffStrategy(t *testing.T) {
	svc, _, _ := newService(t)

	_, err := svc.Enqueue(context.Background(), jobContr.EnqueueJobReq{
		TaskType:        "email.send",
		BackoffStrategy: "fibonacci",
	})

	assertCode(t, err, jobErr.EInvalidRetryPolicy)
}

func TestEnqueue_DeduplicatesOnAnIdempotencyKey(t *testing.T) {
	svc, store, _ := newService(t)
	req := jobContr.EnqueueJobReq{
		TaskType:       "email.send",
		IdempotencyKey: "welcome-user-42",
	}

	first, err := svc.Enqueue(context.Background(), req)
	require.NoError(t, err)
	assert.False(t, first.Deduplicated)

	second, err := svc.Enqueue(context.Background(), req)
	require.NoError(t, err)

	// The second call returns the original job rather than an error, which is
	// what makes the endpoint safe for a client to retry after a timeout.
	assert.True(t, second.Deduplicated)
	assert.Equal(t, first.ID, second.ID)

	page, err := store.List(context.Background(), persistenceFilterAll())
	require.NoError(t, err)
	assert.Equal(t, int64(1), page.Total)
}

func TestEnqueue_ReleasesTheClaimWhenPersistenceFails(t *testing.T) {
	svc, store, _ := newService(t)
	req := jobContr.EnqueueJobReq{
		TaskType:       "email.send",
		IdempotencyKey: "welcome-user-42",
	}

	store.FailNext = errors.New("redis is down")
	_, err := svc.Enqueue(context.Background(), req)
	require.Error(t, err)

	// The key must be free again: a failed enqueue that leaves a claim behind
	// would lock the caller out for the whole TTL over a job that never existed.
	res, err := svc.Enqueue(context.Background(), req)
	require.NoError(t, err)
	assert.False(t, res.Deduplicated)
}

func TestEnqueue_AppliesACustomRetryPolicy(t *testing.T) {
	svc, store, _ := newService(t)
	maxRetries := uint32(7)
	base := int64(3)

	res, err := svc.Enqueue(context.Background(), jobContr.EnqueueJobReq{
		TaskType:              "demo.flaky",
		MaxRetries:            &maxRetries,
		RetryBaseDelaySeconds: &base,
		BackoffStrategy:       "linear",
	})
	require.NoError(t, err)

	stored, err := store.FindByID(context.Background(), jobVO.JobID(res.ID))
	require.NoError(t, err)

	policy := stored.RetryPolicy()
	assert.Equal(t, uint32(7), policy.MaxRetries())
	assert.Equal(t, 3*time.Second, policy.BaseDelay())
	assert.Equal(t, "linear", policy.StrategyName())
}

// persistenceFilterAll is an unfiltered listing, used to assert on totals.
func persistenceFilterAll() persistence.JobFilter {
	return persistence.JobFilter{Limit: 100}
}
