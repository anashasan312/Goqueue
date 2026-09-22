// Package processor owns what a single job's execution means: resolving its
// handler, running it under a timeout, and recording the outcome.
package processor

import (
	"sort"
	"sync"

	"github.com/anashasan/goqueue/pkg/application/services"
	"github.com/anashasan/goqueue/pkg/common/errors"
	jobErr "github.com/anashasan/goqueue/pkg/domain/job_aggregate/error"
	jobVO "github.com/anashasan/goqueue/pkg/domain/job_aggregate/value_objects"
)

var _ services.IHandlerRegistry = (*HandlerRegistry)(nil)

// HandlerRegistry maps task types to handlers.
//
// It is the extension point of the whole system: a new kind of work is a call to
// Register, and no existing file changes. The alternative — a switch on task
// type inside the worker — would make every new job type an edit to the most
// concurrency-sensitive code in the project.
type HandlerRegistry struct {
	// mu guards handlers. Registration normally happens once at startup, but a
	// guard costs nothing and removes a whole class of "we added a handler at
	// runtime and it raced" bug reports.
	mu       sync.RWMutex
	handlers map[jobVO.TaskType]services.Handler
}

// NewHandlerRegistry builds an empty registry.
func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{handlers: make(map[jobVO.TaskType]services.Handler)}
}

// Register binds a handler to a task type.
//
// A duplicate registration is rejected rather than allowed to overwrite.
// Silently shadowing the first handler would mean jobs quietly stop running the
// code someone thinks they run, which is far harder to diagnose than a startup
// failure.
func (r *HandlerRegistry) Register(taskType jobVO.TaskType, handler services.Handler) error {
	if taskType == "" {
		return errors.Invalid(jobErr.EInvalidTaskType, "task type is required to register a handler")
	}
	if handler == nil {
		return errors.Invalid(jobErr.EHandlerNotFound, "handler must not be nil")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.handlers[taskType]; exists {
		return errors.Conflict(
			"duplicate_handler",
			"a handler is already registered for task type "+taskType.String(),
		)
	}
	r.handlers[taskType] = handler
	return nil
}

// RegisterFunc is a convenience wrapper around Register for plain functions.
func (r *HandlerRegistry) RegisterFunc(taskType jobVO.TaskType, fn services.HandlerFunc) error {
	return r.Register(taskType, fn)
}

// Resolve returns the handler for a task type.
func (r *HandlerRegistry) Resolve(taskType jobVO.TaskType) (services.Handler, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	handler, ok := r.handlers[taskType]
	if !ok {
		return nil, errors.NotFound(
			jobErr.EHandlerNotFound,
			"no handler registered for task type "+taskType.String(),
		)
	}
	return handler, nil
}

// RegisteredTypes lists every bound task type in sorted order.
func (r *HandlerRegistry) RegisteredTypes() []jobVO.TaskType {
	r.mu.RLock()
	defer r.mu.RUnlock()

	types := make([]jobVO.TaskType, 0, len(r.handlers))
	for taskType := range r.handlers {
		types = append(types, taskType)
	}
	sort.Slice(types, func(i, j int) bool { return types[i] < types[j] })
	return types
}
