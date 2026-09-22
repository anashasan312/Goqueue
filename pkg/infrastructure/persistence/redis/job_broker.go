package redis

import (
	"context"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/anashasan/goqueue/pkg/common/clock"
	"github.com/anashasan/goqueue/pkg/common/errors"
	jobAgg "github.com/anashasan/goqueue/pkg/domain/job_aggregate"
	jobVO "github.com/anashasan/goqueue/pkg/domain/job_aggregate/value_objects"
	"github.com/anashasan/goqueue/pkg/domain/persistence"
	queueVO "github.com/anashasan/goqueue/pkg/domain/queue_aggregate/value_objects"
	"github.com/anashasan/goqueue/pkg/infrastructure/persistence/redis/script"
)

// Compile-time proof that this type satisfies the port. Placing the assertion
// in the implementing package means a signature drift breaks the build here,
// next to the code that has to change, rather than in di.
var _ persistence.IJobBroker = (*JobBroker)(nil)

// leaseGrace is added to a job's timeout when computing its lease expiry.
//
// Without a margin, a handler that runs for exactly its timeout races the
// reclaim sweep and can be duplicated at the finish line. Thirty seconds is
// long enough to absorb GC pauses and a slow Redis round trip, short enough
// that a genuinely dead worker's jobs come back quickly.
const leaseGrace = 30 * time.Second

// JobBroker implements the queueing port on Redis.
//
// It owns exactly one thing: moving jobs between lifecycle sets atomically. It
// does not decide *whether* a job should retry — that is the Job aggregate's
// decision, already made by the time a method here is called.
type JobBroker struct {
	client     goredis.UniversalClient
	keys       *KeyBuilder
	clock      clock.Clock
	defWeight  queueVO.Weight
	sweepLimit int64
}

// NewJobBroker builds a JobBroker.
func NewJobBroker(
	client goredis.UniversalClient,
	keys *KeyBuilder,
	clk clock.Clock,
) *JobBroker {
	return &JobBroker{
		client:     client,
		keys:       keys,
		clock:      clk,
		defWeight:  queueVO.DefaultWeight,
		sweepLimit: 500,
	}
}

// Enqueue makes a job immediately eligible for dequeue.
func (b *JobBroker) Enqueue(ctx context.Context, job *jobAgg.Job) error {
	score := PendingScore(job.Priority(), toMillis(job.ProcessAt()))
	return b.insert(ctx, job, b.keys.Pending(job.Queue()), score)
}

// Schedule stores a job for execution at its process-at instant.
func (b *JobBroker) Schedule(ctx context.Context, job *jobAgg.Job) error {
	score := float64(toMillis(job.ProcessAt()))
	return b.insert(ctx, job, b.keys.Scheduled(job.Queue()), score)
}

// insert is the shared body of Enqueue and Schedule.
func (b *JobBroker) insert(ctx context.Context, job *jobAgg.Job, targetKey string, score float64) error {
	fields, err := toJobFields(job)
	if err != nil {
		return err
	}

	argv := make([]any, 0, len(fields)+4)
	argv = append(argv,
		job.ID().String(),
		score,
		job.Queue().String(),
		b.defWeight.Uint8(),
	)
	argv = append(argv, fields...)

	keys := []string{
		b.keys.Job(job.ID()),
		targetKey,
		b.keys.QueueRegistry(),
		b.keys.QueueMeta(job.Queue()),
	}

	inserted, err := script.Enqueue.Run(ctx, b.client, keys, argv...).Int64()
	if err != nil {
		return errors.Internal("job_enqueue_failed", "failed to enqueue job", err)
	}
	if inserted == 0 {
		return errors.Conflict("duplicate_job_id", "a job with this id already exists")
	}
	return nil
}

// Dequeue claims the next eligible job for a worker.
//
// The returned job is already leased: the Lua script moved it into the active
// set and stamped the lease before returning, and this method then replays the
// domain's Lease transition on the reconstituted aggregate so the attempt trail
// and retry accounting stay owned by the Job.
func (b *JobBroker) Dequeue(
	ctx context.Context,
	order []jobVO.QueueName,
	workerID string,
	leaseFor time.Duration,
) (*jobAgg.Job, error) {
	if len(order) == 0 {
		return nil, nil
	}

	// Collapse the weighted polling order into its distinct queues. The weights
	// have already done their job upstream by deciding which queue leads the
	// order on this sweep; sending the same key three times would only make the
	// script re-check an empty set.
	distinct := dedupeQueues(order)

	keys := make([]string, 0, len(distinct)*3)
	for _, queue := range distinct {
		keys = append(keys,
			b.keys.Pending(queue),
			b.keys.Active(queue),
			b.keys.QueueMeta(queue),
		)
	}

	now := b.clock.Now()
	leaseExpiry := now.Add(leaseFor + leaseGrace)

	raw, err := script.Dequeue.Run(ctx, b.client, keys,
		len(distinct),
		workerID,
		toMillis(leaseExpiry),
		toMillis(now),
		b.keys.JobPrefix(),
	).Slice()

	if err != nil {
		// redis.Nil is the script returning false: every queue was empty. That
		// is the steady state of an idle worker, not a failure.
		if err == goredis.Nil {
			return nil, nil
		}
		return nil, errors.Internal("job_dequeue_failed", "failed to dequeue job", err)
	}
	if len(raw) == 0 {
		return nil, nil
	}

	hash, err := flatArrayToMap(raw)
	if err != nil {
		return nil, err
	}

	job, err := toAggregate(hash)
	if err != nil {
		return nil, err
	}

	// Apply the domain transition on top of the snapshot the script returned.
	if err := job.Lease(workerID, now, leaseGrace); err != nil {
		return nil, err
	}

	return job, nil
}

// Complete moves an active job to the completed set with a retention TTL.
func (b *JobBroker) Complete(ctx context.Context, job *jobAgg.Job, retainFor time.Duration) error {
	retainUntil := b.clock.Now().Add(retainFor)
	return b.transition(
		ctx, job,
		b.keys.Active(job.Queue()),
		b.keys.Completed(job.Queue()),
		float64(toMillis(retainUntil)),
		int64(retainFor.Seconds()),
	)
}

// Retry moves an active job to the retry set for promotion at retryAt.
func (b *JobBroker) Retry(ctx context.Context, job *jobAgg.Job, retryAt time.Time) error {
	return b.transition(
		ctx, job,
		b.keys.Active(job.Queue()),
		b.keys.Retrying(job.Queue()),
		float64(toMillis(retryAt)),
		0,
	)
}

// Kill moves a job to the dead-letter set.
//
// The source set is chosen from the job's previous position rather than assumed
// to be active, because an operator can kill a job that is still scheduled or
// waiting on a retry.
func (b *JobBroker) Kill(ctx context.Context, job *jobAgg.Job) error {
	diedAt := job.DiedAt()
	score := float64(toMillis(b.clock.Now()))
	if diedAt != nil {
		score = float64(toMillis(*diedAt))
	}
	return b.transitionFromAny(ctx, job, b.keys.Dead(job.Queue()), score, 0)
}

// Requeue moves a dead job back to the pending set.
func (b *JobBroker) Requeue(ctx context.Context, job *jobAgg.Job) error {
	score := PendingScore(job.Priority(), toMillis(job.ProcessAt()))
	return b.transition(
		ctx, job,
		b.keys.Dead(job.Queue()),
		b.keys.Pending(job.Queue()),
		score,
		0,
	)
}

// transition runs the shared move-and-rewrite script.
func (b *JobBroker) transition(
	ctx context.Context,
	job *jobAgg.Job,
	fromKey, toKey string,
	score float64,
	ttlSeconds int64,
) error {
	fields, err := toJobFields(job)
	if err != nil {
		return err
	}

	argv := make([]any, 0, len(fields)+3)
	argv = append(argv, job.ID().String(), score, ttlSeconds)
	argv = append(argv, fields...)

	keys := []string{b.keys.Job(job.ID()), fromKey, toKey}

	if _, err := script.Transition.Run(ctx, b.client, keys, argv...).Int64(); err != nil {
		return errors.Internal("job_transition_failed", "failed to transition job", err)
	}
	return nil
}

// transitionFromAny performs a move whose source set is unknown, by removing the
// job from every set of its queue before inserting it into the destination.
//
// It costs one extra round trip and is used only on the operator-driven kill
// path, which is rare. The hot paths always know where the job came from.
func (b *JobBroker) transitionFromAny(
	ctx context.Context,
	job *jobAgg.Job,
	toKey string,
	score float64,
	ttlSeconds int64,
) error {
	queue := job.Queue()
	id := job.ID().String()

	pipe := b.client.TxPipeline()
	for _, key := range []string{
		b.keys.Pending(queue),
		b.keys.Active(queue),
		b.keys.Scheduled(queue),
		b.keys.Retrying(queue),
		b.keys.Completed(queue),
		b.keys.Dead(queue),
	} {
		pipe.ZRem(ctx, key, id)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != goredis.Nil {
		return errors.Internal("job_transition_failed", "failed to clear job set membership", err)
	}

	// The source set is now empty of this job, so any key works as "from".
	return b.transition(ctx, job, b.keys.Pending(queue), toKey, score, ttlSeconds)
}

// PromoteDue moves due scheduled and retrying jobs into pending.
func (b *JobBroker) PromoteDue(
	ctx context.Context,
	queue jobVO.QueueName,
	now time.Time,
	limit int64,
) (int64, error) {
	if limit <= 0 {
		limit = b.sweepLimit
	}

	var total int64
	for _, sourceKey := range []string{b.keys.Scheduled(queue), b.keys.Retrying(queue)} {
		moved, err := script.Promote.Run(
			ctx, b.client,
			[]string{sourceKey, b.keys.Pending(queue)},
			toMillis(now),
			limit,
			b.keys.JobPrefix(),
			int64(priorityBand),
			jobVO.MaxPriority.Uint8(),
		).Int64()
		if err != nil {
			return total, errors.Internal("job_promote_failed", "failed to promote due jobs", err)
		}
		total += moved
	}
	return total, nil
}

// ReclaimExpired returns jobs with lapsed leases to the pending set.
func (b *JobBroker) ReclaimExpired(
	ctx context.Context,
	queue jobVO.QueueName,
	now time.Time,
	limit int64,
) (int64, error) {
	if limit <= 0 {
		limit = b.sweepLimit
	}

	reclaimed, err := script.Reclaim.Run(
		ctx, b.client,
		[]string{b.keys.Active(queue), b.keys.Pending(queue)},
		toMillis(now),
		limit,
		b.keys.JobPrefix(),
		int64(priorityBand),
		jobVO.MaxPriority.Uint8(),
	).Int64()
	if err != nil {
		return 0, errors.Internal("job_reclaim_failed", "failed to reclaim expired jobs", err)
	}
	return reclaimed, nil
}

// ExtendLease renews an active job's lease on behalf of a worker heartbeat.
func (b *JobBroker) ExtendLease(
	ctx context.Context,
	id jobVO.JobID,
	queue jobVO.QueueName,
	until time.Time,
) error {
	extended, err := script.ExtendLease.Run(
		ctx, b.client,
		[]string{b.keys.Active(queue), b.keys.Job(id)},
		id.String(),
		toMillis(until),
	).Int64()
	if err != nil {
		return errors.Internal("lease_extend_failed", "failed to extend job lease", err)
	}
	if extended == 0 {
		return errors.Conflict(
			"lease_lost",
			"job is no longer active; its lease was reclaimed by another worker",
		)
	}
	return nil
}

// dedupeQueues collapses a weighted polling order into distinct queues while
// preserving first-appearance order.
func dedupeQueues(order []jobVO.QueueName) []jobVO.QueueName {
	seen := make(map[jobVO.QueueName]struct{}, len(order))
	out := make([]jobVO.QueueName, 0, len(order))
	for _, q := range order {
		if _, ok := seen[q]; ok {
			continue
		}
		seen[q] = struct{}{}
		out = append(out, q)
	}
	return out
}
