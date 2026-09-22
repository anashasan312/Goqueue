package services

import (
	"context"

	jobAgg "github.com/anashasan/goqueue/pkg/domain/job_aggregate"
	jobVO "github.com/anashasan/goqueue/pkg/domain/job_aggregate/value_objects"
)

// JobContext is what a handler receives. It exposes the job's identity and
// payload and nothing else — a handler has no business transitioning a job's
// state, and the type system says so.
type JobContext struct {
	ID       jobVO.JobID
	Queue    jobVO.QueueName
	TaskType jobVO.TaskType
	Payload  []byte
	Attempt  uint32
	// MaxRetries is exposed so a handler can behave differently on its last
	// attempt, for example by writing to a fallback store instead of failing.
	MaxRetries uint32
}

// Handler executes one unit of work.
//
// Returning nil marks the job complete. Returning an error marks the attempt
// failed and lets the job's retry policy decide what happens next. A handler
// should honour ctx cancellation: the pool cancels it on timeout and on
// shutdown, and a handler that ignores it will be abandoned rather than waited
// for.
type Handler interface {
	Handle(ctx context.Context, job JobContext) error
}

// HandlerFunc adapts a plain function to the Handler interface.
type HandlerFunc func(ctx context.Context, job JobContext) error

// Handle calls the underlying function.
func (f HandlerFunc) Handle(ctx context.Context, job JobContext) error { return f(ctx, job) }

// IHandlerRegistry resolves a task type to the handler that executes it.
//
// Registration is how new work types enter the system: adding one never
// requires editing the pool, the processor, or any switch statement. That is the
// open/closed principle doing real work here, not as decoration.
type IHandlerRegistry interface {
	// Register binds a handler to a task type. Registering a task type twice is
	// an error, because the second registration would silently shadow the first.
	Register(taskType jobVO.TaskType, handler Handler) error

	// Resolve returns the handler for a task type, or a KindNotFound error.
	Resolve(taskType jobVO.TaskType) (Handler, error)

	// RegisteredTypes lists every bound task type, for the readiness endpoint.
	RegisteredTypes() []jobVO.TaskType
}

// IJobProcessorService owns the lifecycle of a job during execution.
//
// The worker pool deals with concurrency: how many goroutines run, when to stop,
// how to drain. This service deals with what a single job's execution *means*:
// resolve its handler, run it under a timeout, and record the outcome by asking
// the aggregate what should happen next. Separating them is what lets the retry
// rules be unit-tested without starting a goroutine, and the shutdown behaviour
// be tested without a Redis.
type IJobProcessorService interface {
	// Process executes one leased job and persists its outcome. The returned
	// error describes a failure of the *processing machinery* — an unreachable
	// Redis, say. A handler that returns an error is an expected outcome, not a
	// processing failure, and yields a nil error here.
	Process(ctx context.Context, job *jobAgg.Job) error
}
