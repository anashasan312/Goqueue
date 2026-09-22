package persistence

import (
	"context"

	jobVO "github.com/anashasan/goqueue/pkg/domain/job_aggregate/value_objects"
	queueAgg "github.com/anashasan/goqueue/pkg/domain/queue_aggregate"
)

// IQueueRepo persists queue registration and serves the read models the
// dashboard renders.
type IQueueRepo interface {
	// Register records that a queue exists so it shows up in listings even
	// before it has ever held a job.
	Register(ctx context.Context, queue *queueAgg.Queue) error

	// FindByName returns one queue, or a KindNotFound error.
	FindByName(ctx context.Context, name jobVO.QueueName) (*queueAgg.Queue, error)

	// ListAll returns every registered queue.
	ListAll(ctx context.Context) ([]*queueAgg.Queue, error)

	// SetPaused suspends or resumes dequeue for a queue.
	SetPaused(ctx context.Context, name jobVO.QueueName, paused bool) error

	// Stats returns the per-state depth snapshot for one queue.
	Stats(ctx context.Context, name jobVO.QueueName) (queueAgg.Stats, error)

	// StatsAll returns the depth snapshot for every registered queue.
	StatsAll(ctx context.Context) ([]queueAgg.Stats, error)
}
