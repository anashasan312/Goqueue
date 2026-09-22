// Package queue_aggregate contains the Queue aggregate root: the consistency
// boundary for a named queue's configuration and its observable statistics.
package queue_aggregate

import (
	"sort"

	jobVO "github.com/anashasan/goqueue/pkg/domain/job_aggregate/value_objects"
	vo "github.com/anashasan/goqueue/pkg/domain/queue_aggregate/value_objects"
)

// Queue is the aggregate root describing one named queue.
type Queue struct {
	name   jobVO.QueueName
	weight vo.Weight
	paused bool
}

// NewQueue validates and constructs a Queue.
func NewQueue(name jobVO.QueueName, weight vo.Weight) *Queue {
	if weight == 0 {
		weight = vo.DefaultWeight
	}
	return &Queue{name: name, weight: weight}
}

// Reconstitute rebuilds a Queue from storage, including its paused flag.
func Reconstitute(name jobVO.QueueName, weight vo.Weight, paused bool) *Queue {
	return &Queue{name: name, weight: weight, paused: paused}
}

// Name returns the queue name.
func (q *Queue) Name() jobVO.QueueName { return q.name }

// Weight returns the relative dequeue share.
func (q *Queue) Weight() vo.Weight { return q.weight }

// IsPaused reports whether dequeue from this queue is suspended.
func (q *Queue) IsPaused() bool { return q.paused }

// Pause suspends dequeue. Enqueue continues to work, which is deliberate: an
// operator pausing a queue wants to stop consumption during an incident without
// losing the work that arrives meanwhile.
func (q *Queue) Pause() { q.paused = true }

// Resume re-enables dequeue.
func (q *Queue) Resume() { q.paused = false }

// Stats is a read-model snapshot of a queue's depth per lifecycle state. It is
// a value object built by the repository for the dashboard, never mutated.
type Stats struct {
	Queue     jobVO.QueueName
	Paused    bool
	Weight    vo.Weight
	Pending   int64
	Active    int64
	Scheduled int64
	Retrying  int64
	Completed int64
	Dead      int64
}

// Total returns the number of jobs currently tracked for the queue.
func (s Stats) Total() int64 {
	return s.Pending + s.Active + s.Scheduled + s.Retrying + s.Completed + s.Dead
}

// Backlog returns the jobs that still have work owed to them, excluding
// terminal states. This is the number worth alerting on.
func (s Stats) Backlog() int64 {
	return s.Pending + s.Active + s.Scheduled + s.Retrying
}

// SortStatsByName orders a stats slice alphabetically so the dashboard renders
// deterministically between refreshes.
func SortStatsByName(stats []Stats) {
	sort.Slice(stats, func(i, j int) bool {
		return stats[i].Queue < stats[j].Queue
	})
}

// BuildDequeueOrder expands a set of queues into the polling order used by a
// worker: each queue appears as many times as its weight, and the result is
// sorted by descending weight so heavier queues are tried first on every sweep.
//
// This lives in the domain because queue fairness is a business rule, not a
// Redis detail — the same ordering would apply to any broker.
func BuildDequeueOrder(queues []*Queue) []jobVO.QueueName {
	active := make([]*Queue, 0, len(queues))
	for _, q := range queues {
		if q != nil && !q.IsPaused() {
			active = append(active, q)
		}
	}
	sort.SliceStable(active, func(i, j int) bool {
		if active[i].weight != active[j].weight {
			return active[i].weight > active[j].weight
		}
		return active[i].name < active[j].name
	})

	order := make([]jobVO.QueueName, 0, len(active))
	for _, q := range active {
		for i := 0; i < q.weight.Int(); i++ {
			order = append(order, q.name)
		}
	}
	return order
}
