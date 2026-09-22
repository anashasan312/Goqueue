package services

import (
	"context"

	jobContr "github.com/anashasan/goqueue/pkg/contracts/job"
	queueContr "github.com/anashasan/goqueue/pkg/contracts/queue"
)

// IInspectorService is the read and operate side of the API: everything the
// dashboard does once a job exists.
//
// It is a separate interface from IEnqueuerService because the two have
// genuinely different consumers. A producer service that only submits work
// should not be handed a dependency that can also delete jobs and pause queues.
type IInspectorService interface {
	// GetJob returns one job in full.
	GetJob(ctx context.Context, id string) (*jobContr.JobRes, error)

	// ListJobs returns a filtered, paginated page of jobs.
	ListJobs(ctx context.Context, query jobContr.ListJobsQuery) (*jobContr.ListJobsRes, error)

	// RetryJob returns a dead job to its queue with a fresh retry budget.
	RetryJob(ctx context.Context, id string) (*jobContr.JobActionRes, error)

	// KillJob moves a job to the dead-letter queue, skipping remaining retries.
	KillJob(ctx context.Context, id string) (*jobContr.JobActionRes, error)

	// DeleteJob removes a job entirely.
	DeleteJob(ctx context.Context, id string) error

	// RetryAllDead requeues every dead job in a queue and reports how many moved.
	RetryAllDead(ctx context.Context, queue string) (*jobContr.BulkActionRes, error)

	// ListQueueStats returns the depth snapshot of every queue.
	ListQueueStats(ctx context.Context) (*queueContr.ListQueueStatsRes, error)

	// SetQueuePaused suspends or resumes consumption of a queue.
	SetQueuePaused(ctx context.Context, queue string, paused bool) (*queueContr.QueueActionRes, error)
}
