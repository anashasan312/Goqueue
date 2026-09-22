package redis

import (
	"encoding/base64"
	"encoding/json"
	"strconv"
	"time"

	"github.com/anashasan/goqueue/pkg/common/errors"
	jobAgg "github.com/anashasan/goqueue/pkg/domain/job_aggregate"
	jobErr "github.com/anashasan/goqueue/pkg/domain/job_aggregate/error"
	jobVO "github.com/anashasan/goqueue/pkg/domain/job_aggregate/value_objects"
)

// Hash field names. They are snake_case and stable: changing one is a storage
// migration, so they are declared as constants rather than written inline where
// a typo would silently produce a second field.
const (
	fieldID             = "id"
	fieldQueue          = "queue"
	fieldTaskType       = "task_type"
	fieldPayload        = "payload"
	fieldPriority       = "priority"
	fieldState          = "state"
	fieldMaxRetries     = "max_retries"
	fieldBaseDelayMS    = "base_delay_ms"
	fieldMaxDelayMS     = "max_delay_ms"
	fieldBackoff        = "backoff"
	fieldIdempotencyKey = "idempotency_key"
	fieldTimeoutMS      = "timeout_ms"
	fieldAttemptsMade   = "attempts_made"
	fieldAttempts       = "attempts"
	fieldLastError      = "last_error"
	fieldCreatedAtMS    = "created_at_ms"
	fieldProcessAtMS    = "process_at_ms"
	fieldCompletedAtMS  = "completed_at_ms"
	fieldDiedAtMS       = "died_at_ms"
	fieldLeaseExpiryMS  = "lease_expiry_ms"
	fieldWorkerID       = "worker_id"
)

// attemptModel is the persisted shape of one execution record.
//
// The attempt trail is stored as a single JSON field rather than as separate
// hash fields because it is read and written as a unit, and because a JSON array
// keeps the hash's field count fixed no matter how many times a job is retried.
type attemptModel struct {
	Number       uint32 `json:"number"`
	StartedAtMS  int64  `json:"started_at_ms"`
	FinishedAtMS int64  `json:"finished_at_ms"`
	Error        string `json:"error,omitempty"`
	WorkerID     string `json:"worker_id,omitempty"`
}

// toJobFields flattens a Job aggregate into the field/value pairs a Lua script
// passes to HSET.
//
// The payload is base64 encoded. Redis strings are binary safe, but the job
// document is also read by the dashboard and by redis-cli during an incident,
// and a raw binary payload makes both unreadable; base64 keeps the whole
// document inspectable at the cost of a third more bytes.
func toJobFields(job *jobAgg.Job) ([]any, error) {
	attempts := make([]attemptModel, 0, len(job.Attempts()))
	for _, a := range job.Attempts() {
		attempts = append(attempts, attemptModel{
			Number:       a.Number,
			StartedAtMS:  toMillis(a.StartedAt),
			FinishedAtMS: toMillis(a.FinishedAt),
			Error:        a.Error,
			WorkerID:     a.WorkerID,
		})
	}

	attemptsJSON, err := json.Marshal(attempts)
	if err != nil {
		return nil, errors.Internal("job_encode_failed", "failed to encode job attempts", err)
	}

	policy := job.RetryPolicy()

	fields := []any{
		fieldID, job.ID().String(),
		fieldQueue, job.Queue().String(),
		fieldTaskType, job.TaskType().String(),
		fieldPayload, base64.StdEncoding.EncodeToString(job.Payload()),
		fieldPriority, strconv.FormatUint(uint64(job.Priority().Uint8()), 10),
		fieldState, job.State().String(),
		fieldMaxRetries, strconv.FormatUint(uint64(policy.MaxRetries()), 10),
		fieldBaseDelayMS, strconv.FormatInt(policy.BaseDelay().Milliseconds(), 10),
		fieldMaxDelayMS, strconv.FormatInt(policy.MaxDelay().Milliseconds(), 10),
		fieldBackoff, policy.StrategyName(),
		fieldIdempotencyKey, job.IdempotencyKey().String(),
		fieldTimeoutMS, strconv.FormatInt(job.Timeout().Milliseconds(), 10),
		fieldAttemptsMade, strconv.FormatUint(uint64(job.AttemptsMade()), 10),
		fieldAttempts, string(attemptsJSON),
		fieldLastError, job.LastError(),
		fieldCreatedAtMS, strconv.FormatInt(toMillis(job.CreatedAt()), 10),
		fieldProcessAtMS, strconv.FormatInt(toMillis(job.ProcessAt()), 10),
		fieldCompletedAtMS, strconv.FormatInt(toMillisPtr(job.CompletedAt()), 10),
		fieldDiedAtMS, strconv.FormatInt(toMillisPtr(job.DiedAt()), 10),
		fieldLeaseExpiryMS, strconv.FormatInt(toMillisPtr(job.LeaseExpiry()), 10),
		fieldWorkerID, job.WorkerID(),
	}

	return fields, nil
}

// toAggregate rebuilds a Job from a Redis hash.
//
// Decoding is lenient about missing optional fields and strict about the ones
// that define identity: a document without an id or a task type is corrupt, and
// silently substituting a default would hand the worker pool a job it cannot
// route.
func toAggregate(hash map[string]string) (*jobAgg.Job, error) {
	if len(hash) == 0 {
		return nil, errors.NotFound(jobErr.EJobNotFound, "job document is empty")
	}

	id, err := jobVO.NewJobID(hash[fieldID])
	if err != nil {
		return nil, err
	}
	queue, err := jobVO.NewQueueName(hash[fieldQueue])
	if err != nil {
		return nil, err
	}
	taskType, err := jobVO.NewTaskType(hash[fieldTaskType])
	if err != nil {
		return nil, err
	}
	state, err := jobVO.NewJobState(hash[fieldState])
	if err != nil {
		return nil, err
	}
	priority, err := jobVO.NewPriority(uint8(parseUint(hash[fieldPriority], uint64(jobVO.PriorityNormal))))
	if err != nil {
		return nil, err
	}
	idempotencyKey, err := jobVO.NewIdempotencyKey(hash[fieldIdempotencyKey])
	if err != nil {
		return nil, err
	}

	payload, err := base64.StdEncoding.DecodeString(hash[fieldPayload])
	if err != nil {
		return nil, errors.Internal("job_decode_failed", "failed to decode job payload", err)
	}

	policy, err := jobAgg.NewRetryPolicy(
		uint32(parseUint(hash[fieldMaxRetries], uint64(jobAgg.DefaultMaxRetries))),
		time.Duration(parseInt(hash[fieldBaseDelayMS], jobAgg.DefaultBaseDelay.Milliseconds()))*time.Millisecond,
		time.Duration(parseInt(hash[fieldMaxDelayMS], jobAgg.DefaultMaxDelay.Milliseconds()))*time.Millisecond,
		hash[fieldBackoff],
	)
	if err != nil {
		return nil, err
	}

	var attemptModels []attemptModel
	if raw := hash[fieldAttempts]; raw != "" {
		if err := json.Unmarshal([]byte(raw), &attemptModels); err != nil {
			return nil, errors.Internal("job_decode_failed", "failed to decode job attempts", err)
		}
	}
	attempts := make([]jobAgg.Attempt, 0, len(attemptModels))
	for _, a := range attemptModels {
		attempts = append(attempts, jobAgg.Attempt{
			Number:     a.Number,
			StartedAt:  fromMillis(a.StartedAtMS),
			FinishedAt: fromMillis(a.FinishedAtMS),
			Error:      a.Error,
			WorkerID:   a.WorkerID,
		})
	}

	return jobAgg.Reconstitute(jobAgg.ReconstituteParams{
		ID:             id,
		Queue:          queue,
		TaskType:       taskType,
		Payload:        payload,
		Priority:       priority,
		State:          state,
		RetryPolicy:    policy,
		IdempotencyKey: idempotencyKey,
		Timeout:        time.Duration(parseInt(hash[fieldTimeoutMS], jobAgg.DefaultTimeout.Milliseconds())) * time.Millisecond,
		AttemptsMade:   uint32(parseUint(hash[fieldAttemptsMade], 0)),
		Attempts:       attempts,
		LastError:      hash[fieldLastError],
		CreatedAt:      fromMillis(parseInt(hash[fieldCreatedAtMS], 0)),
		ProcessAt:      fromMillis(parseInt(hash[fieldProcessAtMS], 0)),
		CompletedAt:    fromMillisPtr(parseInt(hash[fieldCompletedAtMS], 0)),
		DiedAt:         fromMillisPtr(parseInt(hash[fieldDiedAtMS], 0)),
		LeaseExpiry:    fromMillisPtr(parseInt(hash[fieldLeaseExpiryMS], 0)),
		WorkerID:       hash[fieldWorkerID],
	}), nil
}

// flatArrayToMap converts the flat [field, value, ...] array a Lua HGETALL
// returns into a map. An odd-length input means the script and this decoder
// disagree, which is a bug rather than a data condition.
func flatArrayToMap(raw []any) (map[string]string, error) {
	if len(raw)%2 != 0 {
		return nil, errors.Internal(
			"job_decode_failed",
			"malformed job hash: odd number of elements",
			nil,
		)
	}
	out := make(map[string]string, len(raw)/2)
	for i := 0; i < len(raw); i += 2 {
		key, ok := raw[i].(string)
		if !ok {
			continue
		}
		value, _ := raw[i+1].(string)
		out[key] = value
	}
	return out, nil
}

// toMillis renders a time as unix milliseconds, mapping the zero time to 0 so
// "unset" is representable in a numeric field.
func toMillis(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UTC().UnixMilli()
}

// toMillisPtr renders an optional time, mapping nil to 0.
func toMillisPtr(t *time.Time) int64 {
	if t == nil {
		return 0
	}
	return toMillis(*t)
}

// fromMillis parses unix milliseconds, mapping 0 back to the zero time.
func fromMillis(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

// fromMillisPtr parses an optional unix millisecond stamp.
func fromMillisPtr(ms int64) *time.Time {
	if ms == 0 {
		return nil
	}
	t := time.UnixMilli(ms).UTC()
	return &t
}

// parseInt parses a decimal integer, falling back to a default.
func parseInt(raw string, fallback int64) int64 {
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return fallback
	}
	return v
}

// parseUint parses a decimal unsigned integer, falling back to a default.
func parseUint(raw string, fallback uint64) uint64 {
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return fallback
	}
	return v
}
