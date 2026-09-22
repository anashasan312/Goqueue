// Package job_aggregate contains the Job aggregate root: the consistency
// boundary for everything that happens to a single unit of work.
//
// Every lifecycle change goes through a method on *Job. Services orchestrate and
// persist; they never assign to a field directly. That is what keeps the
// invariants — legal transitions, attempt accounting, retry exhaustion — in one
// auditable place instead of scattered across the application layer.
package job_aggregate

import (
	"time"

	"github.com/anashasan/goqueue/pkg/common/errors"
	jobErr "github.com/anashasan/goqueue/pkg/domain/job_aggregate/error"
	vo "github.com/anashasan/goqueue/pkg/domain/job_aggregate/value_objects"
)

// Payload limits. A queue is not a blob store; oversized payloads belong behind
// a reference. 256 KiB comfortably fits a rich job description.
const (
	MaxPayloadBytes = 256 * 1024
	DefaultTimeout  = 30 * time.Second
	MaxTimeout      = 24 * time.Hour
)

// Attempt is an immutable record of one execution of a job. The trail is kept on
// the aggregate because "why did this job die" is a domain question, not a
// logging concern.
type Attempt struct {
	// Number is the 1-based attempt index.
	Number uint32
	// StartedAt is when a worker leased the job for this attempt.
	StartedAt time.Time
	// FinishedAt is when the attempt ended, successfully or otherwise.
	FinishedAt time.Time
	// Error is the failure message, empty when the attempt succeeded.
	Error string
	// WorkerID identifies the worker that ran the attempt.
	WorkerID string
}

// Duration returns how long the attempt ran.
func (a Attempt) Duration() time.Duration {
	if a.FinishedAt.IsZero() || a.StartedAt.IsZero() {
		return 0
	}
	return a.FinishedAt.Sub(a.StartedAt)
}

// Job is the aggregate root.
//
// Fields are unexported so that the only way to change a Job is through a method
// that enforces the relevant invariant. Reconstruction from storage uses the
// dedicated Reconstitute constructor, which is the one documented seam that
// bypasses transition checks.
type Job struct {
	id             vo.JobID
	queue          vo.QueueName
	taskType       vo.TaskType
	payload        []byte
	priority       vo.Priority
	state          vo.JobState
	retryPolicy    RetryPolicy
	idempotencyKey vo.IdempotencyKey
	timeout        time.Duration

	attemptsMade uint32
	attempts     []Attempt
	lastError    string

	createdAt   time.Time
	processAt   time.Time
	completedAt *time.Time
	diedAt      *time.Time
	leaseExpiry *time.Time
	workerID    string
}

// NewJobParams is the input to NewJob. A struct is used instead of a long
// positional argument list because the constructor has nine meaningful inputs,
// four of which are optional — positional arguments would be unreadable and easy
// to transpose at the call site.
type NewJobParams struct {
	ID             vo.JobID
	Queue          vo.QueueName
	TaskType       vo.TaskType
	Payload        []byte
	Priority       vo.Priority
	RetryPolicy    RetryPolicy
	IdempotencyKey vo.IdempotencyKey
	Timeout        time.Duration
	// ProcessAt is when the job becomes eligible to run. A zero value or an
	// instant at/before Now means "run immediately".
	ProcessAt time.Time
	// Now is the creation instant, supplied by the caller's Clock so that the
	// aggregate stays free of ambient time.
	Now time.Time
}

// NewJob validates the parameters and returns a Job in its correct initial
// state: pending when it should run now, scheduled when it should run later.
func NewJob(p NewJobParams) (*Job, error) {
	if p.ID.IsZero() {
		return nil, errors.Invalid(jobErr.EInvalidJobID, "job id is required")
	}
	if p.TaskType == "" {
		return nil, errors.Invalid(jobErr.EInvalidTaskType, "task type is required")
	}
	if p.Queue == "" {
		p.Queue = vo.DefaultQueueName
	}
	if len(p.Payload) > MaxPayloadBytes {
		return nil, errors.Invalid(
			jobErr.EInvalidPayload,
			"payload must not exceed 256KiB",
		)
	}
	if p.Priority == 0 {
		p.Priority = vo.PriorityNormal
	}
	if p.Timeout == 0 {
		p.Timeout = DefaultTimeout
	}
	if p.Timeout < 0 || p.Timeout > MaxTimeout {
		return nil, errors.Invalid(
			jobErr.EInvalidTimeout,
			"timeout must be between 1ns and 24h",
		)
	}

	now := p.Now.UTC()
	if now.IsZero() {
		return nil, errors.Invalid(jobErr.EInvalidScheduleTime, "creation time is required")
	}

	processAt := p.ProcessAt.UTC()
	state := vo.StatePending
	if processAt.IsZero() || !processAt.After(now) {
		processAt = now
	} else {
		state = vo.StateScheduled
	}

	policy := p.RetryPolicy
	if policy.StrategyName() == "" {
		policy = DefaultRetryPolicy()
	}

	return &Job{
		id:             p.ID,
		queue:          p.Queue,
		taskType:       p.TaskType,
		payload:        append([]byte(nil), p.Payload...),
		priority:       p.Priority,
		state:          state,
		retryPolicy:    policy,
		idempotencyKey: p.IdempotencyKey,
		timeout:        p.Timeout,
		createdAt:      now,
		processAt:      processAt,
		attempts:       make([]Attempt, 0, 1),
	}, nil
}

// ReconstituteParams carries the full persisted state of a Job.
type ReconstituteParams struct {
	ID             vo.JobID
	Queue          vo.QueueName
	TaskType       vo.TaskType
	Payload        []byte
	Priority       vo.Priority
	State          vo.JobState
	RetryPolicy    RetryPolicy
	IdempotencyKey vo.IdempotencyKey
	Timeout        time.Duration
	AttemptsMade   uint32
	Attempts       []Attempt
	LastError      string
	CreatedAt      time.Time
	ProcessAt      time.Time
	CompletedAt    *time.Time
	DiedAt         *time.Time
	LeaseExpiry    *time.Time
	WorkerID       string
}

// Reconstitute rebuilds a Job from storage without re-running creation rules.
//
// This is the single sanctioned way to bypass the transition checks, and it
// exists because a repository is replaying facts that were already valid when
// they were written, not proposing a new change.
func Reconstitute(p ReconstituteParams) *Job {
	return &Job{
		id:             p.ID,
		queue:          p.Queue,
		taskType:       p.TaskType,
		payload:        p.Payload,
		priority:       p.Priority,
		state:          p.State,
		retryPolicy:    p.RetryPolicy,
		idempotencyKey: p.IdempotencyKey,
		timeout:        p.Timeout,
		attemptsMade:   p.AttemptsMade,
		attempts:       p.Attempts,
		lastError:      p.LastError,
		createdAt:      p.CreatedAt,
		processAt:      p.ProcessAt,
		completedAt:    p.CompletedAt,
		diedAt:         p.DiedAt,
		leaseExpiry:    p.LeaseExpiry,
		workerID:       p.WorkerID,
	}
}

// Clone returns an independent copy of the job.
//
// A repository hands out a Job that the caller is free to transition, so two
// callers must never receive the same pointer: one worker completing a job
// would otherwise mutate the copy another goroutine is reading. The Redis
// implementation gets this for free by decoding a fresh aggregate per read;
// any in-memory implementation has to clone explicitly.
func (j *Job) Clone() *Job {
	clone := *j
	clone.payload = append([]byte(nil), j.payload...)
	clone.attempts = append([]Attempt(nil), j.attempts...)

	// The time pointers are copied by value above, which would leave both jobs
	// pointing at the same instant. They are immutable in practice, but sharing
	// them makes the clone only shallowly independent.
	clone.completedAt = copyTimePtr(j.completedAt)
	clone.diedAt = copyTimePtr(j.diedAt)
	clone.leaseExpiry = copyTimePtr(j.leaseExpiry)

	return &clone
}

// copyTimePtr deep-copies an optional instant.
func copyTimePtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	copied := *t
	return &copied
}

// ---------------------------------------------------------------------------
// Accessors
// ---------------------------------------------------------------------------

// ID returns the job identity.
func (j *Job) ID() vo.JobID { return j.id }

// Queue returns the queue the job belongs to.
func (j *Job) Queue() vo.QueueName { return j.queue }

// TaskType returns the handler routing key.
func (j *Job) TaskType() vo.TaskType { return j.taskType }

// Payload returns a defensive copy of the job payload.
func (j *Job) Payload() []byte { return append([]byte(nil), j.payload...) }

// Priority returns the dequeue rank.
func (j *Job) Priority() vo.Priority { return j.priority }

// State returns the current lifecycle position.
func (j *Job) State() vo.JobState { return j.state }

// RetryPolicy returns the failure policy.
func (j *Job) RetryPolicy() RetryPolicy { return j.retryPolicy }

// IdempotencyKey returns the deduplication token, possibly zero.
func (j *Job) IdempotencyKey() vo.IdempotencyKey { return j.idempotencyKey }

// Timeout returns the per-attempt execution budget.
func (j *Job) Timeout() time.Duration { return j.timeout }

// AttemptsMade returns how many times the job has been executed.
func (j *Job) AttemptsMade() uint32 { return j.attemptsMade }

// Attempts returns a defensive copy of the execution trail.
func (j *Job) Attempts() []Attempt { return append([]Attempt(nil), j.attempts...) }

// LastError returns the most recent failure message.
func (j *Job) LastError() string { return j.lastError }

// CreatedAt returns the creation instant.
func (j *Job) CreatedAt() time.Time { return j.createdAt }

// ProcessAt returns the instant the job becomes eligible to run.
func (j *Job) ProcessAt() time.Time { return j.processAt }

// CompletedAt returns the success instant, nil when the job has not succeeded.
func (j *Job) CompletedAt() *time.Time { return j.completedAt }

// DiedAt returns the dead-letter instant, nil when the job is not dead.
func (j *Job) DiedAt() *time.Time { return j.diedAt }

// LeaseExpiry returns the instant the current worker's lease lapses.
func (j *Job) LeaseExpiry() *time.Time { return j.leaseExpiry }

// WorkerID returns the worker currently or most recently holding the job.
func (j *Job) WorkerID() string { return j.workerID }

// RemainingRetries reports how many retries the job has left.
func (j *Job) RemainingRetries() uint32 {
	if j.attemptsMade > j.retryPolicy.MaxRetries() {
		return 0
	}
	return j.retryPolicy.MaxRetries() - j.attemptsMade
}

// ---------------------------------------------------------------------------
// Lifecycle transitions
// ---------------------------------------------------------------------------

// Promote moves a scheduled or retrying job into the pending set, making it
// eligible for dequeue. The scheduler calls this once processAt has elapsed.
func (j *Job) Promote(now time.Time) error {
	if err := j.guard(vo.StatePending); err != nil {
		return err
	}
	j.state = vo.StatePending
	j.processAt = now.UTC()
	j.leaseExpiry = nil
	return nil
}

// Lease marks the job active for the given worker and records the lease expiry.
//
// The lease is what makes at-least-once delivery safe: if the worker dies, the
// lease lapses and the recovery loop returns the job to pending. It is derived
// from the job timeout plus a grace margin so a healthy-but-slow handler is not
// stolen from underneath itself.
func (j *Job) Lease(workerID string, now time.Time, grace time.Duration) error {
	if err := j.guard(vo.StateActive); err != nil {
		return err
	}
	if workerID == "" {
		return errors.Invalid(jobErr.EIllegalStateTransition, "worker id is required to lease a job")
	}

	startedAt := now.UTC()
	expiry := startedAt.Add(j.timeout + grace)

	j.state = vo.StateActive
	j.workerID = workerID
	j.leaseExpiry = &expiry
	j.attemptsMade++
	j.attempts = append(j.attempts, Attempt{
		Number:    j.attemptsMade,
		StartedAt: startedAt,
		WorkerID:  workerID,
	})
	return nil
}

// Complete marks a successful execution.
func (j *Job) Complete(now time.Time) error {
	if err := j.guard(vo.StateCompleted); err != nil {
		return err
	}
	finishedAt := now.UTC()
	j.state = vo.StateCompleted
	j.completedAt = &finishedAt
	j.leaseExpiry = nil
	j.lastError = ""
	j.closeCurrentAttempt(finishedAt, "")
	return nil
}

// FailureOutcome tells the caller what the domain decided after a failure.
type FailureOutcome struct {
	// ShouldRetry is true when the job was moved to the retrying state.
	ShouldRetry bool
	// RetryAt is the instant of the next attempt, meaningful only when
	// ShouldRetry is true.
	RetryAt time.Time
	// Attempt is the attempt number that just failed.
	Attempt uint32
}

// Fail records a failed execution and decides, by the job's own retry policy,
// whether the job retries or moves to the dead-letter queue.
//
// The decision lives here rather than in the processor because "is this job out
// of retries" is an invariant of the job, and putting it in a service would let
// two callers disagree about it.
func (j *Job) Fail(cause error, now time.Time) (FailureOutcome, error) {
	if j.state != vo.StateActive {
		return FailureOutcome{}, errors.Invalid(
			jobErr.EIllegalStateTransition,
			"only an active job can fail, current state: "+j.state.String(),
		)
	}

	finishedAt := now.UTC()
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	j.lastError = message
	j.closeCurrentAttempt(finishedAt, message)
	j.leaseExpiry = nil

	if !j.retryPolicy.AllowsRetry(j.attemptsMade) {
		j.state = vo.StateDead
		j.diedAt = &finishedAt
		return FailureOutcome{ShouldRetry: false, Attempt: j.attemptsMade}, nil
	}

	delay := j.retryPolicy.NextDelay(j.attemptsMade)
	retryAt := finishedAt.Add(delay)
	j.state = vo.StateRetrying
	j.processAt = retryAt

	return FailureOutcome{ShouldRetry: true, RetryAt: retryAt, Attempt: j.attemptsMade}, nil
}

// Kill moves a job straight to the dead-letter queue, skipping any remaining
// retries. It backs the operator action "stop trying this job".
func (j *Job) Kill(reason string, now time.Time) error {
	if err := j.guard(vo.StateDead); err != nil {
		return err
	}
	diedAt := now.UTC()
	j.state = vo.StateDead
	j.diedAt = &diedAt
	j.leaseExpiry = nil
	if reason != "" {
		j.lastError = reason
	}
	j.closeCurrentAttempt(diedAt, j.lastError)
	return nil
}

// Requeue returns a dead job to the pending set and resets its attempt counter,
// giving it a fresh retry budget. It backs the dashboard's "retry" button.
func (j *Job) Requeue(now time.Time) error {
	if j.state != vo.StateDead {
		return errors.Invalid(
			jobErr.EJobNotRetryable,
			"only a dead job can be requeued, current state: "+j.state.String(),
		)
	}
	j.state = vo.StatePending
	j.processAt = now.UTC()
	j.attemptsMade = 0
	j.diedAt = nil
	j.leaseExpiry = nil
	j.workerID = ""
	return nil
}

// ReclaimExpiredLease returns an active job whose lease has lapsed to the
// pending set. The failed attempt is closed with an explicit reason so the trail
// shows the job was orphaned rather than silently restarted.
func (j *Job) ReclaimExpiredLease(now time.Time) error {
	if j.state != vo.StateActive {
		return errors.Invalid(
			jobErr.EIllegalStateTransition,
			"only an active job can be reclaimed, current state: "+j.state.String(),
		)
	}
	reclaimedAt := now.UTC()
	j.closeCurrentAttempt(reclaimedAt, "lease expired: worker did not report completion")
	j.lastError = "lease expired: worker did not report completion"
	j.state = vo.StatePending
	j.processAt = reclaimedAt
	j.leaseExpiry = nil
	j.workerID = ""
	return nil
}

// IsLeaseExpired reports whether an active job's lease has lapsed as of now.
func (j *Job) IsLeaseExpired(now time.Time) bool {
	if j.state != vo.StateActive || j.leaseExpiry == nil {
		return false
	}
	return now.UTC().After(*j.leaseExpiry)
}

// IsDue reports whether a scheduled or retrying job is ready to be promoted.
func (j *Job) IsDue(now time.Time) bool {
	return !j.processAt.After(now.UTC())
}

// guard rejects an illegal transition using the lifecycle graph owned by the
// JobState value object.
func (j *Job) guard(next vo.JobState) error {
	if !j.state.CanTransitionTo(next) {
		return errors.Invalid(
			jobErr.EIllegalStateTransition,
			"cannot transition from "+j.state.String()+" to "+next.String(),
		)
	}
	return nil
}

// closeCurrentAttempt stamps the open attempt with its outcome. It is a no-op
// when there is no open attempt, which keeps Kill safe to call on a job that has
// never run.
func (j *Job) closeCurrentAttempt(finishedAt time.Time, errMessage string) {
	if len(j.attempts) == 0 {
		return
	}
	current := &j.attempts[len(j.attempts)-1]
	if !current.FinishedAt.IsZero() {
		return
	}
	current.FinishedAt = finishedAt
	current.Error = errMessage
}
