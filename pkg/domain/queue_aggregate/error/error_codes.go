// Package error holds the stable error codes owned by the queue aggregate.
package error

// Queue aggregate error codes.
const (
	// EInvalidQueueWeight is returned when a queue weight is outside 1..100.
	EInvalidQueueWeight = "invalid_queue_weight"
	// EQueueNotFound is returned when no queue is registered under a name.
	EQueueNotFound = "queue_not_found"
	// EQueuePaused is returned when work is dispatched to a paused queue.
	EQueuePaused = "queue_paused"
	// ENoQueuesConfigured is returned when a worker pool has no queues to drain.
	ENoQueuesConfigured = "no_queues_configured"
)
