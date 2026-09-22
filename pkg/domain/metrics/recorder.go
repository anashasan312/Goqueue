// Package metrics defines the observability port.
//
// The port lives in the domain so that the worker pool and the application
// services can emit measurements without importing Prometheus. Swapping in
// OpenTelemetry later is a di change, not a rewrite.
package metrics

import (
	"time"

	jobVO "github.com/anashasan/goqueue/pkg/domain/job_aggregate/value_objects"
)

// Outcome labels a finished job execution.
type Outcome string

const (
	// OutcomeSuccess means the handler returned nil.
	OutcomeSuccess Outcome = "success"
	// OutcomeFailure means the handler returned an error and the job will retry.
	OutcomeFailure Outcome = "failure"
	// OutcomeDead means the job exhausted its retries.
	OutcomeDead Outcome = "dead"
)

// String renders the outcome as a metric label value.
func (o Outcome) String() string { return string(o) }

// Recorder is the measurement port. Implementations must be safe for concurrent
// use by every worker goroutine and must never block or return an error: an
// observability failure may not take down job processing.
type Recorder interface {
	// RecordJobProcessed increments the throughput counter.
	RecordJobProcessed(queue jobVO.QueueName, taskType jobVO.TaskType, outcome Outcome)

	// RecordJobDuration observes end-to-end handler latency.
	RecordJobDuration(queue jobVO.QueueName, taskType jobVO.TaskType, d time.Duration)

	// RecordJobRetry increments the retry counter.
	RecordJobRetry(queue jobVO.QueueName, taskType jobVO.TaskType)

	// RecordJobFailed increments the failure counter, labelled by reason so a
	// timeout is distinguishable from a handler error on the dashboard.
	RecordJobFailed(queue jobVO.QueueName, taskType jobVO.TaskType, reason string)

	// SetQueueDepth publishes the current depth of one queue/state pair.
	SetQueueDepth(queue jobVO.QueueName, state jobVO.JobState, depth int64)

	// SetActiveWorkers publishes how many workers are currently executing a job.
	SetActiveWorkers(count int64)
}

// NopRecorder discards every measurement. It is the default binding for tests
// and for a process started with metrics disabled.
type NopRecorder struct{}

// NewNopRecorder builds a discarding Recorder.
func NewNopRecorder() *NopRecorder { return &NopRecorder{} }

// RecordJobProcessed discards the measurement.
func (NopRecorder) RecordJobProcessed(jobVO.QueueName, jobVO.TaskType, Outcome) {}

// RecordJobDuration discards the measurement.
func (NopRecorder) RecordJobDuration(jobVO.QueueName, jobVO.TaskType, time.Duration) {}

// RecordJobRetry discards the measurement.
func (NopRecorder) RecordJobRetry(jobVO.QueueName, jobVO.TaskType) {}

// RecordJobFailed discards the measurement.
func (NopRecorder) RecordJobFailed(jobVO.QueueName, jobVO.TaskType, string) {}

// SetQueueDepth discards the measurement.
func (NopRecorder) SetQueueDepth(jobVO.QueueName, jobVO.JobState, int64) {}

// SetActiveWorkers discards the measurement.
func (NopRecorder) SetActiveWorkers(int64) {}
