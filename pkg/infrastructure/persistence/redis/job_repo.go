package redis

import (
	"context"

	goredis "github.com/redis/go-redis/v9"

	"github.com/anashasan/goqueue/pkg/common/clock"
	"github.com/anashasan/goqueue/pkg/common/errors"
	jobAgg "github.com/anashasan/goqueue/pkg/domain/job_aggregate"
	jobErr "github.com/anashasan/goqueue/pkg/domain/job_aggregate/error"
	jobVO "github.com/anashasan/goqueue/pkg/domain/job_aggregate/value_objects"
	"github.com/anashasan/goqueue/pkg/domain/persistence"
	"github.com/anashasan/goqueue/pkg/infrastructure/persistence/redis/script"
)

var _ persistence.IJobRepo = (*JobRepo)(nil)

// defaultPageSize caps an unbounded listing so a dashboard request cannot ask
// Redis for a million documents.
const defaultPageSize int64 = 50

// maxPageSize is the hard ceiling regardless of what a caller asks for.
const maxPageSize int64 = 500

// JobRepo implements the job persistence port on Redis.
//
// It is read-oriented: writes on the hot path go through JobBroker, which needs
// to move set membership in the same atomic step. This type exists for the
// queries the dashboard and the API ask — look one up, list a page, delete one —
// which have no queueing semantics at all.
type JobRepo struct {
	client goredis.UniversalClient
	keys   *KeyBuilder
	clock  clock.Clock
}

// NewJobRepo builds a JobRepo.
func NewJobRepo(client goredis.UniversalClient, keys *KeyBuilder, clk clock.Clock) *JobRepo {
	return &JobRepo{client: client, keys: keys, clock: clk}
}

// Save writes the full job document without touching set membership.
//
// This is the right call after a domain transition that does not move the job
// between sets — recording a lease, for instance, where the broker has already
// placed the job in the active set and only the document needs to catch up.
func (r *JobRepo) Save(ctx context.Context, job *jobAgg.Job) error {
	fields, err := toJobFields(job)
	if err != nil {
		return err
	}
	if err := r.client.HSet(ctx, r.keys.Job(job.ID()), fields...).Err(); err != nil {
		return errors.Internal("job_save_failed", "failed to save job", err)
	}
	return nil
}

// FindByID returns one job.
func (r *JobRepo) FindByID(ctx context.Context, id jobVO.JobID) (*jobAgg.Job, error) {
	hash, err := r.client.HGetAll(ctx, r.keys.Job(id)).Result()
	if err != nil {
		return nil, errors.Internal("job_read_failed", "failed to read job", err)
	}
	if len(hash) == 0 {
		return nil, errors.NotFound(jobErr.EJobNotFound, "no job found with id "+id.String())
	}
	return toAggregate(hash)
}

// List returns a filtered, paginated page of jobs.
//
// The filter is applied across the lifecycle sorted sets rather than by scanning
// the key space: SCAN over a large Redis instance is an incident waiting to
// happen, while a sorted set range is O(log N + page).
func (r *JobRepo) List(ctx context.Context, filter persistence.JobFilter) (persistence.JobPage, error) {
	limit := filter.Limit
	if limit <= 0 {
		limit = defaultPageSize
	}
	if limit > maxPageSize {
		limit = maxPageSize
	}
	offset := filter.Offset
	if offset < 0 {
		offset = 0
	}

	queues, err := r.resolveQueues(ctx, filter.Queue)
	if err != nil {
		return persistence.JobPage{}, err
	}
	states := resolveStates(filter.State)

	// Collect candidate ids from every (queue, state) pair in the filter.
	ids := make([]jobVO.JobID, 0, limit+offset)
	var total int64

	for _, queue := range queues {
		for _, state := range states {
			setKey := r.keys.StateSet(queue, state)
			if setKey == "" {
				continue
			}
			count, err := r.client.ZCard(ctx, setKey).Result()
			if err != nil {
				return persistence.JobPage{}, errors.Internal(
					"job_list_failed", "failed to count jobs", err,
				)
			}
			total += count

			// Over-fetch by the offset so paging still works when several sets
			// contribute to one page.
			members, err := r.client.ZRange(ctx, setKey, 0, offset+limit-1).Result()
			if err != nil {
				return persistence.JobPage{}, errors.Internal(
					"job_list_failed", "failed to list jobs", err,
				)
			}
			for _, member := range members {
				id, err := jobVO.NewJobID(member)
				if err != nil {
					continue
				}
				ids = append(ids, id)
			}
		}
	}

	jobs, err := r.loadMany(ctx, ids)
	if err != nil {
		return persistence.JobPage{}, err
	}

	// Task type is not indexed, so it is applied in memory over the page. A
	// dedicated index would be the answer if this ever became a hot query; for a
	// dashboard filter over a bounded page it is not worth the write amplification.
	if filter.TaskType != "" {
		filtered := jobs[:0]
		for _, job := range jobs {
			if job.TaskType() == filter.TaskType {
				filtered = append(filtered, job)
			}
		}
		jobs = filtered
		total = int64(len(jobs))
	}

	// Newest first: this is a dashboard, and the job someone is looking for is
	// almost always one that just happened.
	sortJobsByCreatedAtDesc(jobs)

	if offset >= int64(len(jobs)) {
		return persistence.JobPage{Jobs: []*jobAgg.Job{}, Total: total}, nil
	}
	end := offset + limit
	if end > int64(len(jobs)) {
		end = int64(len(jobs))
	}

	return persistence.JobPage{Jobs: jobs[offset:end], Total: total}, nil
}

// Delete removes a job document and every set entry that referenced it.
func (r *JobRepo) Delete(ctx context.Context, id jobVO.JobID) error {
	job, err := r.FindByID(ctx, id)
	if err != nil {
		return err
	}

	queue := job.Queue()
	pipe := r.client.TxPipeline()
	pipe.Del(ctx, r.keys.Job(id))
	for _, state := range jobVO.AllStates() {
		pipe.ZRem(ctx, r.keys.StateSet(queue, state), id.String())
	}
	if !job.IdempotencyKey().IsZero() {
		pipe.Del(ctx, r.keys.Idempotency(job.IdempotencyKey()))
	}

	if _, err := pipe.Exec(ctx); err != nil && err != goredis.Nil {
		return errors.Internal("job_delete_failed", "failed to delete job", err)
	}
	return nil
}

// PurgeCompleted trims completed jobs whose retention window has elapsed.
func (r *JobRepo) PurgeCompleted(
	ctx context.Context,
	queue jobVO.QueueName,
	limit int64,
) (int64, error) {
	if limit <= 0 {
		limit = maxPageSize
	}
	removed, err := script.PurgeCompleted.Run(
		ctx, r.client,
		[]string{r.keys.Completed(queue)},
		toMillis(r.clock.Now()),
		limit,
		r.keys.JobPrefix(),
	).Int64()
	if err != nil {
		return 0, errors.Internal("job_purge_failed", "failed to purge completed jobs", err)
	}
	return removed, nil
}

// loadMany fetches several job documents in one pipeline round trip.
func (r *JobRepo) loadMany(ctx context.Context, ids []jobVO.JobID) ([]*jobAgg.Job, error) {
	if len(ids) == 0 {
		return []*jobAgg.Job{}, nil
	}

	pipe := r.client.Pipeline()
	cmds := make([]*goredis.MapStringStringCmd, 0, len(ids))
	for _, id := range ids {
		cmds = append(cmds, pipe.HGetAll(ctx, r.keys.Job(id)))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != goredis.Nil {
		return nil, errors.Internal("job_list_failed", "failed to load jobs", err)
	}

	jobs := make([]*jobAgg.Job, 0, len(cmds))
	for _, cmd := range cmds {
		hash, err := cmd.Result()
		if err != nil || len(hash) == 0 {
			// A set entry whose document has expired is stale, not fatal: skip it
			// rather than failing a whole dashboard page over one purged job.
			continue
		}
		job, err := toAggregate(hash)
		if err != nil {
			continue
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

// resolveQueues expands an optional queue filter into the set to search.
func (r *JobRepo) resolveQueues(ctx context.Context, filter jobVO.QueueName) ([]jobVO.QueueName, error) {
	if filter != "" {
		return []jobVO.QueueName{filter}, nil
	}
	names, err := r.client.SMembers(ctx, r.keys.QueueRegistry()).Result()
	if err != nil {
		return nil, errors.Internal("queue_list_failed", "failed to list queues", err)
	}
	queues := make([]jobVO.QueueName, 0, len(names))
	for _, name := range names {
		queue, err := jobVO.NewQueueName(name)
		if err != nil {
			continue
		}
		queues = append(queues, queue)
	}
	return queues, nil
}

// resolveStates expands an optional state filter into the set to search.
func resolveStates(filter jobVO.JobState) []jobVO.JobState {
	if filter != "" {
		return []jobVO.JobState{filter}
	}
	return jobVO.AllStates()
}
