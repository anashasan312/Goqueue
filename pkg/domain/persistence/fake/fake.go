// Package fake provides in-memory implementations of the persistence ports.
//
// These live in the production tree rather than in a _test file so that every
// package can use them, and so they are compiled and type-checked on every
// build rather than only when one particular package's tests run.
//
// Their existence is the practical payoff of depending on interfaces: the
// enqueuer, the inspector, the processor and the worker pool are all fully
// testable with no Redis, no Docker and no network, because none of them ever
// knew what a Redis was.
package fake

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/anashasan/goqueue/pkg/common/errors"
	jobAgg "github.com/anashasan/goqueue/pkg/domain/job_aggregate"
	jobErr "github.com/anashasan/goqueue/pkg/domain/job_aggregate/error"
	jobVO "github.com/anashasan/goqueue/pkg/domain/job_aggregate/value_objects"
	"github.com/anashasan/goqueue/pkg/domain/persistence"
	queueAgg "github.com/anashasan/goqueue/pkg/domain/queue_aggregate"
	queueErr "github.com/anashasan/goqueue/pkg/domain/queue_aggregate/error"
	queueVO "github.com/anashasan/goqueue/pkg/domain/queue_aggregate/value_objects"
)

// Compile-time proof that the fakes really are substitutable for the real
// implementations — the Liskov substitution principle checked by the compiler
// rather than assumed.
var (
	_ persistence.IJobRepo          = (*Store)(nil)
	_ persistence.IJobBroker        = (*Store)(nil)
	_ persistence.IQueueRepo        = (*Store)(nil)
	_ persistence.IIdempotencyStore = (*Store)(nil)
)

// Store is an in-memory implementation of all four persistence ports.
//
// One type implements all of them because, like the Redis implementation, they
// share state: a job's set membership and its document have to stay consistent.
// Consumers still depend on the narrow interfaces, so nothing gains access it
// should not have.
type Store struct {
	mu sync.RWMutex

	jobs   map[jobVO.JobID]*jobAgg.Job
	queues map[jobVO.QueueName]*queueAgg.Queue
	claims map[jobVO.IdempotencyKey]jobVO.JobID

	// FailNext, when set, makes the next mutating call return it. Tests use it
	// to exercise rollback paths that are otherwise unreachable.
	FailNext error
}

// NewStore builds an empty Store.
func NewStore() *Store {
	return &Store{
		jobs:   make(map[jobVO.JobID]*jobAgg.Job),
		queues: make(map[jobVO.QueueName]*queueAgg.Queue),
		claims: make(map[jobVO.IdempotencyKey]jobVO.JobID),
	}
}

// takeFailure consumes a one-shot injected failure.
func (s *Store) takeFailure() error {
	if s.FailNext != nil {
		err := s.FailNext
		s.FailNext = nil
		return err
	}
	return nil
}

// ---------------------------------------------------------------------------
// IJobRepo
// ---------------------------------------------------------------------------

// Save stores a job document.
func (s *Store) Save(_ context.Context, job *jobAgg.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.takeFailure(); err != nil {
		return err
	}
	// Store a clone so the caller keeps sole ownership of its pointer. Without
	// this the map would alias the caller's aggregate and two goroutines could
	// mutate the same Job — the same aliasing bug a real store cannot have,
	// because it serialises every write.
	s.jobs[job.ID()] = job.Clone()
	s.ensureQueue(job.Queue())
	return nil
}

// FindByID returns one job.
func (s *Store) FindByID(_ context.Context, id jobVO.JobID) (*jobAgg.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	job, ok := s.jobs[id]
	if !ok {
		return nil, errors.NotFound(jobErr.EJobNotFound, "no job found with id "+id.String())
	}
	return job.Clone(), nil
}

// List returns a filtered, paginated page of jobs.
func (s *Store) List(_ context.Context, filter persistence.JobFilter) (persistence.JobPage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	matched := make([]*jobAgg.Job, 0, len(s.jobs))
	for _, job := range s.jobs {
		if filter.Queue != "" && job.Queue() != filter.Queue {
			continue
		}
		if filter.State != "" && job.State() != filter.State {
			continue
		}
		if filter.TaskType != "" && job.TaskType() != filter.TaskType {
			continue
		}
		matched = append(matched, job.Clone())
	}

	sort.SliceStable(matched, func(i, j int) bool {
		if matched[i].CreatedAt().Equal(matched[j].CreatedAt()) {
			return matched[i].ID() < matched[j].ID()
		}
		return matched[i].CreatedAt().After(matched[j].CreatedAt())
	})

	total := int64(len(matched))
	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}

	if filter.Offset >= total {
		return persistence.JobPage{Jobs: []*jobAgg.Job{}, Total: total}, nil
	}
	end := filter.Offset + limit
	if end > total {
		end = total
	}
	return persistence.JobPage{Jobs: matched[filter.Offset:end], Total: total}, nil
}

// Delete removes a job.
func (s *Store) Delete(_ context.Context, id jobVO.JobID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[id]
	if !ok {
		return errors.NotFound(jobErr.EJobNotFound, "no job found with id "+id.String())
	}
	delete(s.jobs, id)
	if !job.IdempotencyKey().IsZero() {
		delete(s.claims, job.IdempotencyKey())
	}
	return nil
}

// PurgeCompleted removes completed jobs and reports the count.
func (s *Store) PurgeCompleted(_ context.Context, queue jobVO.QueueName, limit int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var removed int64
	for id, job := range s.jobs {
		if limit > 0 && removed >= limit {
			break
		}
		if job.Queue() == queue && job.State() == jobVO.StateCompleted {
			delete(s.jobs, id)
			removed++
		}
	}
	return removed, nil
}

// ---------------------------------------------------------------------------
// IJobBroker
// ---------------------------------------------------------------------------

// Enqueue stores a job as immediately eligible.
func (s *Store) Enqueue(ctx context.Context, job *jobAgg.Job) error { return s.Save(ctx, job) }

// Schedule stores a job for later execution.
func (s *Store) Schedule(ctx context.Context, job *jobAgg.Job) error { return s.Save(ctx, job) }

// Dequeue claims the highest-priority pending job from the first queue in order
// that has one.
//
// The mutex plays the role the Lua script plays in the Redis implementation:
// it makes the claim indivisible, so two concurrent workers cannot take the
// same job. That is what lets a concurrency test written against this fake mean
// something about the real broker.
func (s *Store) Dequeue(
	_ context.Context,
	order []jobVO.QueueName,
	workerID string,
	leaseFor time.Duration,
) (*jobAgg.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.takeFailure(); err != nil {
		return nil, err
	}

	for _, queue := range order {
		if q, ok := s.queues[queue]; ok && q.IsPaused() {
			continue
		}

		var best *jobAgg.Job
		for _, job := range s.jobs {
			if job.Queue() != queue || job.State() != jobVO.StatePending {
				continue
			}
			if best == nil || beats(job, best) {
				best = job
			}
		}

		if best != nil {
			// Lease the stored aggregate first, so the job is already active in
			// the store and no second worker can select it, then hand the caller
			// its own copy to transition.
			if err := best.Lease(workerID, time.Now().UTC(), leaseFor); err != nil {
				return nil, err
			}
			return best.Clone(), nil
		}
	}
	return nil, nil
}

// beats reports whether candidate should be dequeued before current: higher
// priority first, then older first.
func beats(candidate, current *jobAgg.Job) bool {
	if candidate.Priority() != current.Priority() {
		return candidate.Priority() > current.Priority()
	}
	if !candidate.ProcessAt().Equal(current.ProcessAt()) {
		return candidate.ProcessAt().Before(current.ProcessAt())
	}
	return candidate.ID() < current.ID()
}

// Complete stores a completed job.
func (s *Store) Complete(ctx context.Context, job *jobAgg.Job, _ time.Duration) error {
	return s.Save(ctx, job)
}

// Retry stores a retrying job.
func (s *Store) Retry(ctx context.Context, job *jobAgg.Job, _ time.Time) error {
	return s.Save(ctx, job)
}

// Kill stores a dead job.
func (s *Store) Kill(ctx context.Context, job *jobAgg.Job) error { return s.Save(ctx, job) }

// Requeue stores a requeued job.
func (s *Store) Requeue(ctx context.Context, job *jobAgg.Job) error { return s.Save(ctx, job) }

// PromoteDue moves due scheduled and retrying jobs to pending.
func (s *Store) PromoteDue(
	_ context.Context,
	queue jobVO.QueueName,
	now time.Time,
	limit int64,
) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var moved int64
	for _, job := range s.jobs {
		if limit > 0 && moved >= limit {
			break
		}
		if job.Queue() != queue {
			continue
		}
		if job.State() != jobVO.StateScheduled && job.State() != jobVO.StateRetrying {
			continue
		}
		if !job.IsDue(now) {
			continue
		}
		if err := job.Promote(now); err != nil {
			continue
		}
		moved++
	}
	return moved, nil
}

// ReclaimExpired returns jobs with lapsed leases to pending.
func (s *Store) ReclaimExpired(
	_ context.Context,
	queue jobVO.QueueName,
	now time.Time,
	limit int64,
) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var reclaimed int64
	for _, job := range s.jobs {
		if limit > 0 && reclaimed >= limit {
			break
		}
		if job.Queue() != queue || !job.IsLeaseExpired(now) {
			continue
		}
		if err := job.ReclaimExpiredLease(now); err != nil {
			continue
		}
		reclaimed++
	}
	return reclaimed, nil
}

// ExtendLease is a no-op for the fake, which has no lease sweeper of its own.
func (s *Store) ExtendLease(
	_ context.Context,
	id jobVO.JobID,
	_ jobVO.QueueName,
	_ time.Time,
) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, ok := s.jobs[id]; !ok {
		return errors.Conflict("lease_lost", "job is no longer active")
	}
	return nil
}

// ---------------------------------------------------------------------------
// IQueueRepo
// ---------------------------------------------------------------------------

// Register records a queue, preserving an existing paused flag.
//
// Registration happens on every worker start, so overwriting the flag here
// would silently resume a queue an operator paused during an incident the
// moment anything restarted.
func (s *Store) Register(_ context.Context, queue *queueAgg.Queue) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	paused := false
	if existing, ok := s.queues[queue.Name()]; ok {
		paused = existing.IsPaused()
	}
	s.queues[queue.Name()] = queueAgg.Reconstitute(queue.Name(), queue.Weight(), paused)
	return nil
}

// FindByName returns one queue.
func (s *Store) FindByName(_ context.Context, name jobVO.QueueName) (*queueAgg.Queue, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	queue, ok := s.queues[name]
	if !ok {
		return nil, errors.NotFound(queueErr.EQueueNotFound, "no queue named "+name.String())
	}
	return queue, nil
}

// ListAll returns every registered queue.
func (s *Store) ListAll(_ context.Context) ([]*queueAgg.Queue, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	queues := make([]*queueAgg.Queue, 0, len(s.queues))
	for _, queue := range s.queues {
		queues = append(queues, queue)
	}
	sort.Slice(queues, func(i, j int) bool { return queues[i].Name() < queues[j].Name() })
	return queues, nil
}

// SetPaused suspends or resumes a queue.
func (s *Store) SetPaused(_ context.Context, name jobVO.QueueName, paused bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	queue, ok := s.queues[name]
	if !ok {
		return errors.NotFound(queueErr.EQueueNotFound, "no queue named "+name.String())
	}
	if paused {
		queue.Pause()
	} else {
		queue.Resume()
	}
	return nil
}

// Stats returns the depth snapshot for one queue.
func (s *Store) Stats(_ context.Context, name jobVO.QueueName) (queueAgg.Stats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.statsFor(name), nil
}

// StatsAll returns the depth snapshot for every queue.
func (s *Store) StatsAll(_ context.Context) ([]queueAgg.Stats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := make([]queueAgg.Stats, 0, len(s.queues))
	for name := range s.queues {
		stats = append(stats, s.statsFor(name))
	}
	queueAgg.SortStatsByName(stats)
	return stats, nil
}

// statsFor counts jobs by state. The caller already holds the lock.
func (s *Store) statsFor(name jobVO.QueueName) queueAgg.Stats {
	stats := queueAgg.Stats{Queue: name, Weight: queueVO.DefaultWeight}
	if queue, ok := s.queues[name]; ok {
		stats.Paused = queue.IsPaused()
		stats.Weight = queue.Weight()
	}

	for _, job := range s.jobs {
		if job.Queue() != name {
			continue
		}
		switch job.State() {
		case jobVO.StatePending:
			stats.Pending++
		case jobVO.StateActive:
			stats.Active++
		case jobVO.StateScheduled:
			stats.Scheduled++
		case jobVO.StateRetrying:
			stats.Retrying++
		case jobVO.StateCompleted:
			stats.Completed++
		case jobVO.StateDead:
			stats.Dead++
		}
	}
	return stats
}

// ensureQueue registers a queue implicitly on first use. The caller holds the
// lock.
func (s *Store) ensureQueue(name jobVO.QueueName) {
	if _, ok := s.queues[name]; !ok {
		s.queues[name] = queueAgg.NewQueue(name, queueVO.DefaultWeight)
	}
}

// ---------------------------------------------------------------------------
// IIdempotencyStore
// ---------------------------------------------------------------------------

// Claim reserves a key for a job.
func (s *Store) Claim(
	_ context.Context,
	key jobVO.IdempotencyKey,
	jobID jobVO.JobID,
	_ time.Duration,
) (bool, jobVO.JobID, error) {
	if key.IsZero() {
		return true, "", nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.claims[key]; ok {
		return false, existing, nil
	}
	s.claims[key] = jobID
	return true, "", nil
}

// Release drops a claim.
func (s *Store) Release(_ context.Context, key jobVO.IdempotencyKey) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.claims, key)
	return nil
}
