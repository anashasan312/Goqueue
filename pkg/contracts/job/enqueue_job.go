// Package job holds the request and response DTOs for the job API.
//
// Contracts are deliberately separate from aggregates. An aggregate is shaped by
// the invariants it must protect; a contract is shaped by what a client needs to
// send and is allowed to see. Letting one type do both jobs means every domain
// refactor becomes a breaking API change.
package job

import "encoding/json"

// EnqueueJobReq is the request body for creating a job.
type EnqueueJobReq struct {
	// TaskType routes the job to a registered handler. Required.
	TaskType string `json:"task_type" binding:"required"`

	// Queue names the target queue. Optional; defaults to "default".
	Queue string `json:"queue"`

	// Payload is the handler's input, passed through opaquely.
	Payload json.RawMessage `json:"payload"`

	// Priority is one of low, normal, high, critical. Optional.
	Priority string `json:"priority" binding:"omitempty,oneof=low normal high critical"`

	// DelaySeconds schedules the job that many seconds into the future.
	// Mutually exclusive with RunAt.
	DelaySeconds *int64 `json:"delay_seconds" binding:"omitempty,min=0"`

	// RunAt schedules the job at an absolute RFC3339 instant.
	// Mutually exclusive with DelaySeconds.
	RunAt *string `json:"run_at"`

	// MaxRetries overrides the default retry budget. Optional.
	MaxRetries *uint32 `json:"max_retries" binding:"omitempty,max=100"`

	// RetryBaseDelaySeconds is the unit delay fed to the backoff strategy.
	RetryBaseDelaySeconds *int64 `json:"retry_base_delay_seconds" binding:"omitempty,min=0,max=3600"`

	// RetryMaxDelaySeconds caps any computed backoff.
	RetryMaxDelaySeconds *int64 `json:"retry_max_delay_seconds" binding:"omitempty,min=0,max=86400"`

	// BackoffStrategy is one of exponential, linear, constant. Optional.
	BackoffStrategy string `json:"backoff_strategy" binding:"omitempty,oneof=exponential linear constant"`

	// TimeoutSeconds is the per-attempt execution budget.
	TimeoutSeconds *int64 `json:"timeout_seconds" binding:"omitempty,min=1,max=86400"`

	// IdempotencyKey deduplicates the enqueue. While the key is held, a repeat
	// request returns the original job instead of creating a second one.
	IdempotencyKey string `json:"idempotency_key" binding:"omitempty,max=128"`
}

// EnqueueJobRes is returned after a successful enqueue.
type EnqueueJobRes struct {
	ID        string `json:"id"`
	Queue     string `json:"queue"`
	TaskType  string `json:"task_type"`
	State     string `json:"state"`
	Priority  string `json:"priority"`
	ProcessAt string `json:"process_at"`
	CreatedAt string `json:"created_at"`

	// Deduplicated is true when an idempotency key matched an existing job and
	// this response describes that job rather than a newly created one.
	Deduplicated bool `json:"deduplicated"`
}
