package value_objects

import (
	"github.com/anashasan/goqueue/pkg/common/errors"
	jobErr "github.com/anashasan/goqueue/pkg/domain/job_aggregate/error"
)

// JobState is the lifecycle position of a Job aggregate.
//
// The legal transitions are encoded here, next to the states themselves, so that
// no service can invent a transition the domain does not permit.
type JobState string

const (
	// StateScheduled means the job is waiting for its process-at instant.
	StateScheduled JobState = "scheduled"
	// StatePending means the job is queued and eligible for dequeue.
	StatePending JobState = "pending"
	// StateActive means a worker holds a lease on the job and is executing it.
	StateActive JobState = "active"
	// StateRetrying means the job failed and is waiting for its next attempt.
	StateRetrying JobState = "retrying"
	// StateCompleted means the job finished successfully.
	StateCompleted JobState = "completed"
	// StateDead means the job exhausted its retries and sits in the dead-letter
	// queue awaiting manual intervention.
	StateDead JobState = "dead"
)

// allowedTransitions is the single source of truth for the lifecycle graph.
var allowedTransitions = map[JobState]map[JobState]bool{
	StateScheduled: {StatePending: true, StateDead: true},
	StatePending:   {StateActive: true, StateDead: true},
	StateActive:    {StateCompleted: true, StateRetrying: true, StateDead: true, StatePending: true},
	StateRetrying:  {StatePending: true, StateDead: true},
	StateDead:      {StatePending: true},
	StateCompleted: {},
}

// AllStates lists every state, in lifecycle order, for use by the API layer.
func AllStates() []JobState {
	return []JobState{
		StateScheduled, StatePending, StateActive,
		StateRetrying, StateCompleted, StateDead,
	}
}

// NewJobState validates and constructs a JobState from its wire representation.
func NewJobState(raw string) (JobState, error) {
	state := JobState(raw)
	if _, ok := allowedTransitions[state]; !ok {
		return "", errors.Invalid(jobErr.EInvalidJobState, "unknown job state: "+raw)
	}
	return state, nil
}

// String renders the state.
func (s JobState) String() string { return string(s) }

// IsTerminal reports whether no further transition out of this state happens
// automatically. Dead is terminal for the worker pool but an operator may still
// requeue it, which is why it is listed in allowedTransitions.
func (s JobState) IsTerminal() bool {
	return s == StateCompleted || s == StateDead
}

// CanTransitionTo reports whether moving to next is legal.
func (s JobState) CanTransitionTo(next JobState) bool {
	return allowedTransitions[s][next]
}
