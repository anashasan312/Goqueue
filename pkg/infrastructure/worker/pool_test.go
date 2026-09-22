package worker_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anashasan/goqueue/pkg/application/processor"
	"github.com/anashasan/goqueue/pkg/application/services"
	"github.com/anashasan/goqueue/pkg/common/clock"
	"github.com/anashasan/goqueue/pkg/common/logger"
	"github.com/anashasan/goqueue/pkg/common/uid"
	jobAgg "github.com/anashasan/goqueue/pkg/domain/job_aggregate"
	jobVO "github.com/anashasan/goqueue/pkg/domain/job_aggregate/value_objects"
	"github.com/anashasan/goqueue/pkg/domain/metrics"
	"github.com/anashasan/goqueue/pkg/domain/persistence/fake"
	"github.com/anashasan/goqueue/pkg/infrastructure/worker"
)

var testTime = time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)

// newPool assembles a real pool over in-memory fakes. Nothing here is mocked
// except storage, so the concurrency behaviour under test is the production
// behaviour.
func newPool(
	t *testing.T,
	store *fake.Store,
	registry *processor.HandlerRegistry,
	cfg worker.PoolConfig,
) *worker.Pool {
	t.Helper()

	proc := processor.NewJobProcessorService(
		registry, store, store, metrics.NewNopRecorder(),
		clock.NewSystemClock(), logger.NewNop(),
		processor.Config{CompletedRetention: time.Hour},
	)

	return worker.NewPool(
		store, store, proc, metrics.NewNopRecorder(),
		uid.NewRandomGenerator(), logger.NewNop(), cfg,
	)
}

// seedJobs enqueues n pending jobs of the given task type.
func seedJobs(t *testing.T, store *fake.Store, taskType string, n int) {
	t.Helper()

	for i := 0; i < n; i++ {
		job, err := jobAgg.NewJob(jobAgg.NewJobParams{
			ID:          jobVO.JobID(uid.NewRandomGenerator().New()),
			Queue:       jobVO.DefaultQueueName,
			TaskType:    jobVO.TaskType(taskType),
			Priority:    jobVO.PriorityNormal,
			RetryPolicy: jobAgg.DefaultRetryPolicy(),
			Timeout:     5 * time.Second,
			Now:         testTime,
		})
		require.NoError(t, err)
		require.NoError(t, store.Enqueue(context.Background(), job))
	}
}

func TestPool_ProcessesEveryQueuedJobExactlyOnce(t *testing.T) {
	store := fake.NewStore()
	registry := processor.NewHandlerRegistry()

	var mu sync.Mutex
	seen := make(map[jobVO.JobID]int)

	require.NoError(t, registry.Register("email.send",
		services.HandlerFunc(func(_ context.Context, jc services.JobContext) error {
			mu.Lock()
			seen[jc.ID]++
			mu.Unlock()
			return nil
		})))

	const jobCount = 50
	seedJobs(t, store, "email.send", jobCount)

	pool := newPool(t, store, registry, worker.PoolConfig{
		Concurrency:     8,
		Queues:          map[string]uint8{jobVO.DefaultQueueName: 1},
		PollInterval:    5 * time.Millisecond,
		ShutdownTimeout: 5 * time.Second,
		LeaseDuration:   10 * time.Second,
	})

	ctx := context.Background()
	require.NoError(t, pool.Start(ctx))

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == jobCount
	}, 10*time.Second, 20*time.Millisecond, "not every job was processed")

	require.NoError(t, pool.Stop(ctx))

	mu.Lock()
	defer mu.Unlock()
	for id, count := range seen {
		// Eight workers competing for fifty jobs: if the claim were not atomic,
		// some job would be executed twice and this is where it would show.
		assert.Equal(t, 1, count, "job %s was executed more than once", id)
	}
}

func TestPool_StopWaitsForInFlightJobs(t *testing.T) {
	store := fake.NewStore()
	registry := processor.NewHandlerRegistry()

	var started, finished atomic.Int32
	release := make(chan struct{})

	require.NoError(t, registry.Register("demo.slow",
		services.HandlerFunc(func(context.Context, services.JobContext) error {
			started.Add(1)
			<-release
			finished.Add(1)
			return nil
		})))

	seedJobs(t, store, "demo.slow", 3)

	pool := newPool(t, store, registry, worker.PoolConfig{
		Concurrency:     3,
		Queues:          map[string]uint8{jobVO.DefaultQueueName: 1},
		PollInterval:    5 * time.Millisecond,
		ShutdownTimeout: 5 * time.Second,
		LeaseDuration:   10 * time.Second,
	})

	ctx := context.Background()
	require.NoError(t, pool.Start(ctx))

	require.Eventually(t, func() bool { return started.Load() == 3 },
		5*time.Second, 10*time.Millisecond, "handlers did not start")

	// Stop is called while three jobs are mid-flight. It must block until they
	// finish rather than cancelling them: that is the whole promise of
	// "graceful shutdown completes in-flight jobs".
	stopped := make(chan error, 1)
	go func() { stopped <- pool.Stop(ctx) }()

	select {
	case <-stopped:
		t.Fatal("Stop returned while jobs were still running")
	case <-time.After(150 * time.Millisecond):
	}

	close(release)

	require.NoError(t, <-stopped)
	assert.Equal(t, int32(3), finished.Load())
}

func TestPool_StopGivesUpAfterTheShutdownTimeout(t *testing.T) {
	store := fake.NewStore()
	registry := processor.NewHandlerRegistry()

	block := make(chan struct{})
	t.Cleanup(func() { close(block) })

	require.NoError(t, registry.Register("demo.stuck",
		services.HandlerFunc(func(context.Context, services.JobContext) error {
			<-block
			return nil
		})))

	seedJobs(t, store, "demo.stuck", 1)

	pool := newPool(t, store, registry, worker.PoolConfig{
		Concurrency:     1,
		Queues:          map[string]uint8{jobVO.DefaultQueueName: 1},
		PollInterval:    5 * time.Millisecond,
		ShutdownTimeout: 100 * time.Millisecond,
		LeaseDuration:   10 * time.Second,
	})

	ctx := context.Background()
	require.NoError(t, pool.Start(ctx))
	require.Eventually(t, func() bool { return pool.ActiveJobs() == 1 },
		5*time.Second, 10*time.Millisecond)

	start := time.Now()
	// A stuck handler must not block shutdown forever. Stop returns nil, not an
	// error: the job's lease will lapse and the reclaim sweep will requeue it,
	// so this is a latency event rather than data loss.
	require.NoError(t, pool.Stop(ctx))

	assert.Less(t, time.Since(start), 2*time.Second)
}

func TestPool_RejectsASecondStart(t *testing.T) {
	store := fake.NewStore()
	registry := processor.NewHandlerRegistry()
	require.NoError(t, registry.Register("email.send",
		services.HandlerFunc(func(context.Context, services.JobContext) error { return nil })))

	pool := newPool(t, store, registry, worker.PoolConfig{
		Concurrency:     1,
		Queues:          map[string]uint8{jobVO.DefaultQueueName: 1},
		PollInterval:    10 * time.Millisecond,
		ShutdownTimeout: time.Second,
	})

	ctx := context.Background()
	require.NoError(t, pool.Start(ctx))
	t.Cleanup(func() { _ = pool.Stop(ctx) })

	// A second Start would silently double concurrency and leak the first set of
	// goroutines, so it is refused rather than tolerated.
	require.Error(t, pool.Start(ctx))
}

func TestPool_SkipsAPausedQueue(t *testing.T) {
	store := fake.NewStore()
	registry := processor.NewHandlerRegistry()

	var handled atomic.Int32
	require.NoError(t, registry.Register("email.send",
		services.HandlerFunc(func(context.Context, services.JobContext) error {
			handled.Add(1)
			return nil
		})))

	seedJobs(t, store, "email.send", 5)
	require.NoError(t, store.SetPaused(context.Background(), jobVO.DefaultQueueName, true))

	pool := newPool(t, store, registry, worker.PoolConfig{
		Concurrency:     2,
		Queues:          map[string]uint8{jobVO.DefaultQueueName: 1},
		PollInterval:    5 * time.Millisecond,
		ShutdownTimeout: time.Second,
	})

	ctx := context.Background()
	require.NoError(t, pool.Start(ctx))
	time.Sleep(200 * time.Millisecond)
	require.NoError(t, pool.Stop(ctx))

	// Pausing suspends consumption, not production: the five jobs are still
	// there, untouched.
	assert.Zero(t, handled.Load())
}
