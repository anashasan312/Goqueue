package value_objects

import (
	"github.com/anashasan/goqueue/pkg/common/errors"
	queueErr "github.com/anashasan/goqueue/pkg/domain/queue_aggregate/error"
)

// Weight bounds.
const (
	MinWeight = Weight(1)
	MaxWeight = Weight(100)
	// DefaultWeight is applied when a queue is configured without one.
	DefaultWeight = Weight(1)
)

// Weight expresses how much of a worker pool's attention a queue receives
// relative to its siblings. A queue with weight 6 is polled six times as often
// as a queue with weight 1 over a long run.
//
// Weight is queue-level fairness across queues; Priority is job-level ordering
// within a queue. Keeping them as separate concepts is what lets an operator say
// "critical jobs go first, but the low-traffic queue never starves".
type Weight uint8

// NewWeight validates and constructs a Weight. Zero resolves to DefaultWeight.
func NewWeight(raw uint8) (Weight, error) {
	if raw == 0 {
		return DefaultWeight, nil
	}
	w := Weight(raw)
	if w < MinWeight || w > MaxWeight {
		return 0, errors.Invalid(
			queueErr.EInvalidQueueWeight,
			"queue weight must be between 1 and 100",
		)
	}
	return w, nil
}

// Uint8 renders the numeric weight.
func (w Weight) Uint8() uint8 { return uint8(w) }

// Int renders the weight for slice sizing.
func (w Weight) Int() int { return int(w) }
