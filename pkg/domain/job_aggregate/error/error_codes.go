// Package error holds the stable error codes owned by the job aggregate.
//
// Codes are declared next to the aggregate that enforces the invariant so that
// there is exactly one place to look when a client asks what a code means.
package error

// Job aggregate error codes.
const (
	// EInvalidJobID is returned when a job identifier is empty or malformed.
	EInvalidJobID = "invalid_job_id"
	// EInvalidTaskType is returned when a task type is empty or too long.
	EInvalidTaskType = "invalid_task_type"
	// EInvalidQueueName is returned when a queue name is empty or malformed.
	EInvalidQueueName = "invalid_queue_name"
	// EInvalidPriority is returned when a priority falls outside the allowed range.
	EInvalidPriority = "invalid_priority"
	// EInvalidJobState is returned when a state string has no domain equivalent.
	EInvalidJobState = "invalid_job_state"
	// EInvalidPayload is returned when a payload exceeds the maximum size.
	EInvalidPayload = "invalid_payload"
	// EInvalidRetryPolicy is returned when a retry policy is internally inconsistent.
	EInvalidRetryPolicy = "invalid_retry_policy"
	// EInvalidTimeout is returned when a job timeout is zero or negative.
	EInvalidTimeout = "invalid_timeout"
	// EInvalidScheduleTime is returned when a schedule instant is in the past.
	EInvalidScheduleTime = "invalid_schedule_time"
	// EInvalidIdempotencyKey is returned when an idempotency key is too long.
	EInvalidIdempotencyKey = "invalid_idempotency_key"
	// EIllegalStateTransition is returned when a lifecycle transition is not allowed.
	EIllegalStateTransition = "illegal_state_transition"
	// EJobNotFound is returned when no job exists for an identifier.
	EJobNotFound = "job_not_found"
	// EDuplicateJob is returned when an idempotency key is already claimed.
	EDuplicateJob = "duplicate_job"
	// EJobNotRetryable is returned when a retry is requested for a job that is
	// not in the dead-letter queue.
	EJobNotRetryable = "job_not_retryable"
	// EHandlerNotFound is returned when no handler is registered for a task type.
	EHandlerNotFound = "handler_not_found"
	// EJobTimeout is returned when a handler exceeds the job timeout.
	EJobTimeout = "job_timeout"
	// EHandlerPanic is returned when a handler panics.
	EHandlerPanic = "handler_panic"
)
