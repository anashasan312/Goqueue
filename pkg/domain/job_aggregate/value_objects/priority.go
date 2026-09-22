package value_objects

import (
	"fmt"
	"strings"

	"github.com/anashasan/goqueue/pkg/common/errors"
	jobErr "github.com/anashasan/goqueue/pkg/domain/job_aggregate/error"
)

// Priority ranks a job against its siblings inside the same queue. Higher values
// are dequeued first; jobs of equal priority are dequeued in FIFO order.
type Priority uint8

// Supported priority levels. The range is deliberately small and closed: an open
// integer range invites callers to invent their own scale and makes the dequeue
// score arithmetic unbounded.
const (
	PriorityLow      Priority = 1
	PriorityNormal   Priority = 5
	PriorityHigh     Priority = 8
	PriorityCritical Priority = 10
)

// MinPriority and MaxPriority bound the accepted range.
const (
	MinPriority = Priority(1)
	MaxPriority = Priority(10)
)

// NewPriority validates and constructs a Priority from a numeric level. Zero is
// treated as "unspecified" and resolves to PriorityNormal.
func NewPriority(level uint8) (Priority, error) {
	if level == 0 {
		return PriorityNormal, nil
	}
	p := Priority(level)
	if p < MinPriority || p > MaxPriority {
		return 0, errors.Invalid(
			jobErr.EInvalidPriority,
			fmt.Sprintf("priority must be between %d and %d", MinPriority, MaxPriority),
		)
	}
	return p, nil
}

// ParsePriority resolves a named level such as "high". An empty name resolves to
// PriorityNormal so that the field is optional on the wire.
func ParsePriority(name string) (Priority, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "":
		return PriorityNormal, nil
	case "low":
		return PriorityLow, nil
	case "normal", "default":
		return PriorityNormal, nil
	case "high":
		return PriorityHigh, nil
	case "critical":
		return PriorityCritical, nil
	default:
		return 0, errors.Invalid(
			jobErr.EInvalidPriority,
			"priority must be one of low, normal, high, critical",
		)
	}
}

// Uint8 renders the numeric level.
func (p Priority) Uint8() uint8 { return uint8(p) }

// String renders the closest named level, falling back to the number.
func (p Priority) String() string {
	switch p {
	case PriorityLow:
		return "low"
	case PriorityNormal:
		return "normal"
	case PriorityHigh:
		return "high"
	case PriorityCritical:
		return "critical"
	default:
		return fmt.Sprintf("p%d", uint8(p))
	}
}
