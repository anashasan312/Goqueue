package persistence

import (
	"context"
	"time"

	jobAgg "github.com/anashasan/goqueue/pkg/domain/job_aggregate"
	jobVO "github.com/anashasan/goqueue/pkg/domain/job_aggregate/value_objects"
)

// IJobBroker is the queueing port: the atomic operations that move a job between
// lifecycle sets.
//
// Every method must be atomic with respect to concurrent workers. The Redis
// implementation achieves that with Lua scripts; an in-memory implementation
// achieves it with a mutex. Neither detail belongs in the application layer,
// which is exactly why this port exists.
type IJobBroker interface {
	// Enqueue makes a job immediately eligible for dequeue and writes its
	// document in the same atomic step.
	Enqueue(ctx context.Context, job *jobAgg.Job) error

	// Schedule stores a job for future execution at job.ProcessAt().
	Schedule(ctx context.Context, job *jobAgg.Job) error

	// Dequeue atomically claims the highest-priority eligible job from the first
	// queue in `order` that has one, moves it to the active set and returns it
	// already leased to workerID.
	//
	// It returns (nil, nil) when every queue is empty. A nil job with a nil
	// error is the normal idle case, not a failure, so the worker loop can poll
	// without treating emptiness as an error condition.
	Dequeue(
		ctx context.Context,
		order []jobVO.QueueName,
		workerID string,
		leaseFor time.Duration,
	) (*jobAgg.Job, error)

	// Complete moves an active job to the completed set with a retention TTL.
	Complete(ctx context.Context, job *jobAgg.Job, retainFor time.Duration) error

	// Retry moves an active job to the retry set, to be promoted at retryAt.
	Retry(ctx context.Context, job *jobAgg.Job, retryAt time.Time) error

	// Kill moves an active job to the dead-letter set.
	Kill(ctx context.Context, job *jobAgg.Job) error

	// Requeue moves a dead job back to the pending set.
	Requeue(ctx context.Context, job *jobAgg.Job) error

	// PromoteDue moves scheduled and retrying jobs whose instant has arrived
	// into the pending set, and reports how many moved.
	PromoteDue(ctx context.Context, queue jobVO.QueueName, now time.Time, limit int64) (int64, error)

	// ReclaimExpired returns active jobs whose lease lapsed to the pending set
	// and reports how many were reclaimed. This is the recovery path for a
	// worker that was killed mid-job.
	ReclaimExpired(ctx context.Context, queue jobVO.QueueName, now time.Time, limit int64) (int64, error)

	// ExtendLease pushes an active job's lease expiry further out. A long-running
	// handler calls this through its heartbeat so it is not reclaimed while it
	// is still making progress.
	ExtendLease(ctx context.Context, id jobVO.JobID, queue jobVO.QueueName, until time.Time) error
}
