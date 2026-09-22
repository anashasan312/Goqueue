package job

import "encoding/json"

// AttemptRes is one execution record in a job's history.
type AttemptRes struct {
	Number     uint32 `json:"number"`
	StartedAt  string `json:"started_at"`
	FinishedAt string `json:"finished_at,omitempty"`
	DurationMS int64  `json:"duration_ms"`
	Error      string `json:"error,omitempty"`
	WorkerID   string `json:"worker_id,omitempty"`
}

// RetryPolicyRes describes a job's failure policy.
type RetryPolicyRes struct {
	MaxRetries            uint32 `json:"max_retries"`
	RetryBaseDelaySeconds int64  `json:"retry_base_delay_seconds"`
	RetryMaxDelaySeconds  int64  `json:"retry_max_delay_seconds"`
	BackoffStrategy       string `json:"backoff_strategy"`
}

// JobRes is the full representation of a job.
type JobRes struct {
	ID       string          `json:"id"`
	Queue    string          `json:"queue"`
	TaskType string          `json:"task_type"`
	State    string          `json:"state"`
	Priority string          `json:"priority"`
	Payload  json.RawMessage `json:"payload,omitempty"`

	AttemptsMade     uint32 `json:"attempts_made"`
	RemainingRetries uint32 `json:"remaining_retries"`
	LastError        string `json:"last_error,omitempty"`
	TimeoutSeconds   int64  `json:"timeout_seconds"`
	IdempotencyKey   string `json:"idempotency_key,omitempty"`

	RetryPolicy RetryPolicyRes `json:"retry_policy"`
	Attempts    []AttemptRes   `json:"attempts"`

	CreatedAt   string `json:"created_at"`
	ProcessAt   string `json:"process_at"`
	CompletedAt string `json:"completed_at,omitempty"`
	DiedAt      string `json:"died_at,omitempty"`
	LeaseExpiry string `json:"lease_expiry,omitempty"`
	WorkerID    string `json:"worker_id,omitempty"`
}

// JobSummaryRes is the trimmed representation used in listings, where the
// payload and the full attempt trail would dominate the response size.
type JobSummaryRes struct {
	ID               string `json:"id"`
	Queue            string `json:"queue"`
	TaskType         string `json:"task_type"`
	State            string `json:"state"`
	Priority         string `json:"priority"`
	AttemptsMade     uint32 `json:"attempts_made"`
	RemainingRetries uint32 `json:"remaining_retries"`
	LastError        string `json:"last_error,omitempty"`
	CreatedAt        string `json:"created_at"`
	ProcessAt        string `json:"process_at"`
	WorkerID         string `json:"worker_id,omitempty"`
}
