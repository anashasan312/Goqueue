package queue_aggregate_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	jobVO "github.com/anashasan/goqueue/pkg/domain/job_aggregate/value_objects"
	queueAgg "github.com/anashasan/goqueue/pkg/domain/queue_aggregate"
	vo "github.com/anashasan/goqueue/pkg/domain/queue_aggregate/value_objects"
)

func newQueue(t *testing.T, name string, weight uint8) *queueAgg.Queue {
	t.Helper()
	w, err := vo.NewWeight(weight)
	require.NoError(t, err)
	return queueAgg.NewQueue(jobVO.MustNewQueueName(name), w)
}

func TestBuildDequeueOrder_RepeatsEachQueueByItsWeight(t *testing.T) {
	order := queueAgg.BuildDequeueOrder([]*queueAgg.Queue{
		newQueue(t, "low", 1),
		newQueue(t, "critical", 3),
		newQueue(t, "default", 2),
	})

	// Heaviest first, each repeated by its weight: this is what stops a
	// low-traffic queue from starving while still letting critical lead.
	assert.Equal(t, []jobVO.QueueName{
		"critical", "critical", "critical",
		"default", "default",
		"low",
	}, order)
}

func TestBuildDequeueOrder_ExcludesPausedQueues(t *testing.T) {
	paused := newQueue(t, "critical", 3)
	paused.Pause()

	order := queueAgg.BuildDequeueOrder([]*queueAgg.Queue{
		paused,
		newQueue(t, "default", 1),
	})

	assert.Equal(t, []jobVO.QueueName{"default"}, order)
}

func TestBuildDequeueOrder_IsDeterministicForEqualWeights(t *testing.T) {
	// Two queues of equal weight must produce the same order on every call, or
	// two workers would disagree about which queue leads and the fairness
	// guarantee would only hold on average.
	first := queueAgg.BuildDequeueOrder([]*queueAgg.Queue{
		newQueue(t, "beta", 2), newQueue(t, "alpha", 2),
	})
	second := queueAgg.BuildDequeueOrder([]*queueAgg.Queue{
		newQueue(t, "alpha", 2), newQueue(t, "beta", 2),
	})

	assert.Equal(t, first, second)
	assert.Equal(t, jobVO.QueueName("alpha"), first[0])
}

func TestQueue_PauseAndResume(t *testing.T) {
	queue := newQueue(t, "default", 1)
	assert.False(t, queue.IsPaused())

	queue.Pause()
	assert.True(t, queue.IsPaused())

	queue.Resume()
	assert.False(t, queue.IsPaused())
}

func TestStats_BacklogExcludesTerminalStates(t *testing.T) {
	stats := queueAgg.Stats{
		Pending: 5, Active: 2, Scheduled: 3, Retrying: 1,
		Completed: 100, Dead: 4,
	}

	// Backlog is the number worth alerting on: completed and dead jobs are not
	// work anybody is still waiting for.
	assert.Equal(t, int64(11), stats.Backlog())
	assert.Equal(t, int64(115), stats.Total())
}

func TestNewWeight_RejectsOutOfRangeValues(t *testing.T) {
	_, err := vo.NewWeight(101)
	require.Error(t, err)

	// Zero means "unspecified" and resolves to the default rather than failing,
	// so a config that omits the weight still works.
	weight, err := vo.NewWeight(0)
	require.NoError(t, err)
	assert.Equal(t, vo.DefaultWeight, weight)
}
