//go:build integration

// Package integration exercises the Redis implementation against a real server.
//
// These tests are behind a build tag because they need a Redis. The unit suite
// must stay runnable with `go test ./...` and nothing else installed, or people
// stop running it; the things that genuinely cannot be faked — Lua atomicity,
// score ordering, TTL behaviour — live here and run under `make test-integration`.
package integration

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anashasan/goqueue/pkg/common/clock"
	"github.com/anashasan/goqueue/pkg/common/uid"
	jobAgg "github.com/anashasan/goqueue/pkg/domain/job_aggregate"
	jobVO "github.com/anashasan/goqueue/pkg/domain/job_aggregate/value_objects"
	redisInfra "github.com/anashasan/goqueue/pkg/infrastructure/persistence/redis"
)

// suite bundles the Redis-backed components under test.
type suite struct {
	client      goredis.UniversalClient
	broker      *redisInfra.JobBroker
	jobRepo     *redisInfra.JobRepo
	queueRepo   *redisInfra.QueueRepo
	idempotency *redisInfra.IdempotencyStore
	queue       jobVO.QueueName
}

// newSuite connects to Redis and gives the test its own namespace.
//
// A per-test namespace rather than FLUSHDB means the suite can run against a
// developer's shared Redis without destroying whatever else is in it, and means
// the tests can run in parallel.
func newSuite(t *testing.T) *suite {
	t.Helper()

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}

	ctx := context.Background()
	client, err := redisInfra.NewClient(ctx, redisInfra.ClientConfig{
		Addrs:       []string{addr},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Skipf("redis is not reachable at %s: %v", addr, err)
	}

	namespace := fmt.Sprintf("goqueue-test-%s", uid.NewRandomGenerator().New()[:12])
	keys := redisInfra.NewKeyBuilder(namespace)
	clk := clock.NewSystemClock()

	t.Cleanup(func() {
		// Namespaces are unique per test, so a keyspace scan here is bounded and
		// safe; it never touches another test's or a developer's data.
		iter := client.Scan(ctx, 0, namespace+":*", 1000).Iterator()
		for iter.Next(ctx) {
			_ = client.Del(ctx, iter.Val()).Err()
		}
		_ = client.Close()
	})

	return &suite{
		client:      client,
		broker:      redisInfra.NewJobBroker(client, keys, clk),
		jobRepo:     redisInfra.NewJobRepo(client, keys, clk),
		queueRepo:   redisInfra.NewQueueRepo(client, keys),
		idempotency: redisInfra.NewIdempotencyStore(client, keys),
		queue:       jobVO.DefaultQueueName,
	}
}

// newJob builds a job for the suite's queue.
func (s *suite) newJob(t *testing.T, id string, priority jobVO.Priority) *jobAgg.Job {
	t.Helper()

	job, err := jobAgg.NewJob(jobAgg.NewJobParams{
		ID:          jobVO.JobID(id),
		Queue:       s.queue,
		TaskType:    "email.send",
		Payload:     []byte(`{"to":"a@b.c"}`),
		Priority:    priority,
		RetryPolicy: jobAgg.DefaultRetryPolicy(),
		Timeout:     30 * time.Second,
		Now:         time.Now().UTC(),
	})
	require.NoError(t, err)
	return job
}

func TestRedis_EnqueueAndDequeueRoundTrip(t *testing.T) {
	s := newSuite(t)
	ctx := context.Background()

	original := s.newJob(t, "job-roundtrip", jobVO.PriorityNormal)
	require.NoError(t, s.broker.Enqueue(ctx, original))

	dequeued, err := s.broker.Dequeue(ctx, []jobVO.QueueName{s.queue}, "worker-1", 30*time.Second)
	require.NoError(t, err)
	require.NotNil(t, dequeued)

	// Everything must survive the encode/decode round trip, including the
	// payload bytes and the retry policy.
	assert.Equal(t, original.ID(), dequeued.ID())
	assert.Equal(t, original.TaskType(), dequeued.TaskType())
	assert.Equal(t, string(original.Payload()), string(dequeued.Payload()))
	assert.Equal(t, original.RetryPolicy().MaxRetries(), dequeued.RetryPolicy().MaxRetries())
	assert.Equal(t, original.RetryPolicy().StrategyName(), dequeued.RetryPolicy().StrategyName())

	assert.Equal(t, jobVO.StateActive, dequeued.State())
	assert.Equal(t, "worker-1", dequeued.WorkerID())
	assert.Equal(t, uint32(1), dequeued.AttemptsMade())
}

func TestRedis_DequeueReturnsNilOnAnEmptyQueue(t *testing.T) {
	s := newSuite(t)

	job, err := s.broker.Dequeue(
		context.Background(), []jobVO.QueueName{s.queue}, "worker-1", time.Second,
	)

	// An empty queue is the steady state of an idle worker, not an error.
	require.NoError(t, err)
	assert.Nil(t, job)
}

func TestRedis_DequeueRespectsPriorityThenFIFO(t *testing.T) {
	s := newSuite(t)
	ctx := context.Background()

	// Enqueued low first, so anything other than priority ordering would
	// produce a different result.
	require.NoError(t, s.broker.Enqueue(ctx, s.newJob(t, "job-low", jobVO.PriorityLow)))
	time.Sleep(2 * time.Millisecond)
	require.NoError(t, s.broker.Enqueue(ctx, s.newJob(t, "job-critical", jobVO.PriorityCritical)))
	time.Sleep(2 * time.Millisecond)
	require.NoError(t, s.broker.Enqueue(ctx, s.newJob(t, "job-normal-a", jobVO.PriorityNormal)))
	time.Sleep(2 * time.Millisecond)
	require.NoError(t, s.broker.Enqueue(ctx, s.newJob(t, "job-normal-b", jobVO.PriorityNormal)))

	var order []string
	for i := 0; i < 4; i++ {
		job, err := s.broker.Dequeue(ctx, []jobVO.QueueName{s.queue}, "worker-1", time.Minute)
		require.NoError(t, err)
		require.NotNil(t, job)
		order = append(order, job.ID().String())
	}

	// Highest priority first; within one priority, oldest first. This is the
	// score arithmetic in keys.go being verified against a real ZPOPMIN.
	assert.Equal(t,
		[]string{"job-critical", "job-normal-a", "job-normal-b", "job-low"},
		order,
	)
}

func TestRedis_ConcurrentDequeueNeverDuplicatesAJob(t *testing.T) {
	s := newSuite(t)
	ctx := context.Background()

	const jobCount = 100
	for i := 0; i < jobCount; i++ {
		require.NoError(t, s.broker.Enqueue(ctx,
			s.newJob(t, fmt.Sprintf("job-%03d", i), jobVO.PriorityNormal)))
	}

	// Twenty goroutines racing for a hundred jobs against a real Redis. This is
	// the test the Lua script exists for: if ZPOPMIN and the move into the
	// active set were not one atomic step, a job would be claimed twice here.
	var (
		mu      sync.Mutex
		claimed = make(map[jobVO.JobID]int)
		wg      sync.WaitGroup
	)

	for w := 0; w < 20; w++ {
		wg.Add(1)
		go func(workerNum int) {
			defer wg.Done()
			workerID := fmt.Sprintf("worker-%d", workerNum)

			for {
				job, err := s.broker.Dequeue(ctx, []jobVO.QueueName{s.queue}, workerID, time.Minute)
				if err != nil || job == nil {
					return
				}
				mu.Lock()
				claimed[job.ID()]++
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()

	assert.Len(t, claimed, jobCount, "not every job was claimed")
	for id, count := range claimed {
		assert.Equal(t, 1, count, "job %s was claimed more than once", id)
	}
}

func TestRedis_ScheduledJobIsPromotedOnceDue(t *testing.T) {
	s := newSuite(t)
	ctx := context.Background()

	job, err := jobAgg.NewJob(jobAgg.NewJobParams{
		ID:          "job-scheduled",
		Queue:       s.queue,
		TaskType:    "email.send",
		Priority:    jobVO.PriorityNormal,
		RetryPolicy: jobAgg.DefaultRetryPolicy(),
		Timeout:     time.Second,
		ProcessAt:   time.Now().UTC().Add(300 * time.Millisecond),
		Now:         time.Now().UTC(),
	})
	require.NoError(t, err)
	require.Equal(t, jobVO.StateScheduled, job.State())
	require.NoError(t, s.broker.Schedule(ctx, job))

	// Not yet due: nothing to dequeue and nothing to promote.
	moved, err := s.broker.PromoteDue(ctx, s.queue, time.Now().UTC(), 100)
	require.NoError(t, err)
	assert.Zero(t, moved)

	time.Sleep(400 * time.Millisecond)

	moved, err = s.broker.PromoteDue(ctx, s.queue, time.Now().UTC(), 100)
	require.NoError(t, err)
	assert.Equal(t, int64(1), moved)

	dequeued, err := s.broker.Dequeue(ctx, []jobVO.QueueName{s.queue}, "worker-1", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, dequeued)
	assert.Equal(t, jobVO.JobID("job-scheduled"), dequeued.ID())
}

func TestRedis_ExpiredLeaseIsReclaimed(t *testing.T) {
	s := newSuite(t)
	ctx := context.Background()

	require.NoError(t, s.broker.Enqueue(ctx, s.newJob(t, "job-orphan", jobVO.PriorityNormal)))

	leased, err := s.broker.Dequeue(ctx, []jobVO.QueueName{s.queue}, "worker-doomed", time.Second)
	require.NoError(t, err)
	require.NotNil(t, leased)
	require.NoError(t, s.jobRepo.Save(ctx, leased))

	// The worker "dies" here: no Complete, no Retry, nothing. Sweeping at a
	// point past the lease expiry is what a crashed pod looks like to the rest
	// of the system.
	future := time.Now().UTC().Add(2 * time.Hour)

	reclaimed, err := s.broker.ReclaimExpired(ctx, s.queue, future, 100)
	require.NoError(t, err)
	assert.Equal(t, int64(1), reclaimed)

	stored, err := s.jobRepo.FindByID(ctx, "job-orphan")
	require.NoError(t, err)
	assert.Equal(t, jobVO.StatePending, stored.State())
	assert.Empty(t, stored.WorkerID())
	assert.Contains(t, stored.LastError(), "lease expired")

	// And it is genuinely runnable again, not just marked pending.
	again, err := s.broker.Dequeue(ctx, []jobVO.QueueName{s.queue}, "worker-healthy", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, again)
	assert.Equal(t, jobVO.JobID("job-orphan"), again.ID())
}

func TestRedis_IdempotencyKeyAdmitsExactlyOneClaim(t *testing.T) {
	s := newSuite(t)
	ctx := context.Background()
	key := jobVO.IdempotencyKey("order-42-confirmation")

	// Fifty goroutines claiming the same key at once. SET NX is the whole
	// mechanism; this proves it is sufficient under real contention.
	const attempts = 50
	var (
		mu       sync.Mutex
		winners  []jobVO.JobID
		existing []jobVO.JobID
		wg       sync.WaitGroup
	)

	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			jobID := jobVO.JobID(fmt.Sprintf("job-%02d", n))

			claimed, held, err := s.idempotency.Claim(ctx, key, jobID, time.Minute)
			require.NoError(t, err)

			mu.Lock()
			defer mu.Unlock()
			if claimed {
				winners = append(winners, jobID)
			} else {
				existing = append(existing, held)
			}
		}(i)
	}
	wg.Wait()

	require.Len(t, winners, 1, "exactly one claim must succeed")
	assert.Len(t, existing, attempts-1)

	// Every loser must learn the winner's id, so each can return the original
	// job instead of failing or creating a duplicate.
	for _, held := range existing {
		assert.Equal(t, winners[0], held)
	}
}

func TestRedis_ReleasedIdempotencyKeyCanBeClaimedAgain(t *testing.T) {
	s := newSuite(t)
	ctx := context.Background()
	key := jobVO.IdempotencyKey("retryable-key")

	claimed, _, err := s.idempotency.Claim(ctx, key, "job-1", time.Minute)
	require.NoError(t, err)
	require.True(t, claimed)

	require.NoError(t, s.idempotency.Release(ctx, key))

	claimed, _, err = s.idempotency.Claim(ctx, key, "job-2", time.Minute)
	require.NoError(t, err)
	assert.True(t, claimed)
}

func TestRedis_PausedQueueIsNotDrained(t *testing.T) {
	s := newSuite(t)
	ctx := context.Background()

	require.NoError(t, s.broker.Enqueue(ctx, s.newJob(t, "job-paused", jobVO.PriorityNormal)))
	require.NoError(t, s.queueRepo.SetPaused(ctx, s.queue, true))

	job, err := s.broker.Dequeue(ctx, []jobVO.QueueName{s.queue}, "worker-1", time.Minute)
	require.NoError(t, err)
	assert.Nil(t, job, "a paused queue must not yield work")

	require.NoError(t, s.queueRepo.SetPaused(ctx, s.queue, false))

	job, err = s.broker.Dequeue(ctx, []jobVO.QueueName{s.queue}, "worker-1", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, job, "resuming must make the job available again")
}

func TestRedis_RegisterPreservesAnExistingPausedFlag(t *testing.T) {
	s := newSuite(t)
	ctx := context.Background()

	require.NoError(t, s.broker.Enqueue(ctx, s.newJob(t, "job-1", jobVO.PriorityNormal)))
	require.NoError(t, s.queueRepo.SetPaused(ctx, s.queue, true))

	// Register runs on every worker start. It must not resume a queue an
	// operator paused during an incident.
	queue, err := s.queueRepo.FindByName(ctx, s.queue)
	require.NoError(t, err)
	require.True(t, queue.IsPaused())

	require.NoError(t, s.queueRepo.Register(ctx, queue))

	reloaded, err := s.queueRepo.FindByName(ctx, s.queue)
	require.NoError(t, err)
	assert.True(t, reloaded.IsPaused(), "registering must not resume a paused queue")
}

func TestRedis_StatsReflectEveryLifecycleState(t *testing.T) {
	s := newSuite(t)
	ctx := context.Background()

	require.NoError(t, s.broker.Enqueue(ctx, s.newJob(t, "job-pending", jobVO.PriorityNormal)))
	require.NoError(t, s.broker.Enqueue(ctx, s.newJob(t, "job-active", jobVO.PriorityNormal)))

	// Drain both, complete one and leave the other active.
	first, err := s.broker.Dequeue(ctx, []jobVO.QueueName{s.queue}, "worker-1", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, first)
	require.NoError(t, s.jobRepo.Save(ctx, first))

	second, err := s.broker.Dequeue(ctx, []jobVO.QueueName{s.queue}, "worker-1", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, second)
	require.NoError(t, second.Complete(time.Now().UTC()))
	require.NoError(t, s.broker.Complete(ctx, second, time.Hour))

	stats, err := s.queueRepo.Stats(ctx, s.queue)
	require.NoError(t, err)

	assert.Equal(t, int64(1), stats.Active)
	assert.Equal(t, int64(1), stats.Completed)
	assert.Zero(t, stats.Pending)
	assert.Equal(t, int64(2), stats.Total())
}

func TestRedis_FullFailureToDeadLetterToRequeueCycle(t *testing.T) {
	s := newSuite(t)
	ctx := context.Background()

	policy, err := jobAgg.NewRetryPolicy(1, 10*time.Millisecond, time.Second, jobAgg.BackoffConstant)
	require.NoError(t, err)

	job, err := jobAgg.NewJob(jobAgg.NewJobParams{
		ID:          "job-doomed",
		Queue:       s.queue,
		TaskType:    "demo.flaky",
		Priority:    jobVO.PriorityNormal,
		RetryPolicy: policy,
		Timeout:     time.Second,
		Now:         time.Now().UTC(),
	})
	require.NoError(t, err)
	require.NoError(t, s.broker.Enqueue(ctx, job))

	// Attempt 1 fails and is scheduled for retry.
	leased, err := s.broker.Dequeue(ctx, []jobVO.QueueName{s.queue}, "worker-1", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, leased)

	outcome, err := leased.Fail(assertableError("boom"), time.Now().UTC())
	require.NoError(t, err)
	require.True(t, outcome.ShouldRetry)
	require.NoError(t, s.broker.Retry(ctx, leased, outcome.RetryAt))

	time.Sleep(50 * time.Millisecond)
	moved, err := s.broker.PromoteDue(ctx, s.queue, time.Now().UTC(), 100)
	require.NoError(t, err)
	require.Equal(t, int64(1), moved)

	// Attempt 2 fails and exhausts the budget.
	leased, err = s.broker.Dequeue(ctx, []jobVO.QueueName{s.queue}, "worker-1", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, leased)

	outcome, err = leased.Fail(assertableError("boom again"), time.Now().UTC())
	require.NoError(t, err)
	require.False(t, outcome.ShouldRetry)
	require.NoError(t, s.broker.Kill(ctx, leased))

	stats, err := s.queueRepo.Stats(ctx, s.queue)
	require.NoError(t, err)
	assert.Equal(t, int64(1), stats.Dead)

	// The operator requeues it from the dashboard, and it runs again with a
	// fresh budget.
	dead, err := s.jobRepo.FindByID(ctx, "job-doomed")
	require.NoError(t, err)
	require.NoError(t, dead.Requeue(time.Now().UTC()))
	require.NoError(t, s.broker.Requeue(ctx, dead))

	revived, err := s.broker.Dequeue(ctx, []jobVO.QueueName{s.queue}, "worker-2", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, revived)
	assert.Equal(t, jobVO.JobID("job-doomed"), revived.ID())
	assert.Equal(t, uint32(1), revived.AttemptsMade(), "requeue must reset the attempt counter")

	stats, err = s.queueRepo.Stats(ctx, s.queue)
	require.NoError(t, err)
	assert.Zero(t, stats.Dead)
}

// assertableError is a tiny error type, used so the failure messages in these
// tests read as intentional rather than borrowed from a stdlib helper.
type assertableError string

func (e assertableError) Error() string { return string(e) }
