package job_aggregate

import (
	"math"
	"math/rand"
	"strings"
	"time"

	"github.com/anashasan/goqueue/pkg/common/errors"
	jobErr "github.com/anashasan/goqueue/pkg/domain/job_aggregate/error"
)

// BackoffStrategy computes how long to wait before attempt number `attempt`
// (1-based: attempt 1 is the first retry).
//
// It is an interface so that a new waiting curve can be added without editing
// RetryPolicy or the processor: the open/closed principle applied to the one
// piece of retry behaviour that genuinely varies between workloads.
type BackoffStrategy interface {
	// Name identifies the strategy for persistence and for the dashboard.
	Name() string
	// Delay returns the wait before the given attempt, clamped to [0, maxDelay].
	Delay(attempt uint32, base, maxDelay time.Duration) time.Duration
}

// Strategy names as persisted on the job document.
const (
	BackoffExponential = "exponential"
	BackoffLinear      = "linear"
	BackoffConstant    = "constant"
)

// NewBackoffStrategy resolves a persisted strategy name back to its behaviour.
// An empty name resolves to exponential, which is the documented default.
func NewBackoffStrategy(name string) (BackoffStrategy, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", BackoffExponential:
		return ExponentialBackoff{}, nil
	case BackoffLinear:
		return LinearBackoff{}, nil
	case BackoffConstant:
		return ConstantBackoff{}, nil
	default:
		return nil, errors.Invalid(
			jobErr.EInvalidRetryPolicy,
			"backoff strategy must be one of exponential, linear, constant",
		)
	}
}

// ExponentialBackoff doubles the base delay on every attempt and adds full
// jitter. Jitter matters in a distributed queue: without it, a downstream outage
// synchronises every failed job onto the same retry instant and the recovering
// dependency is hit by the entire backlog at once.
type ExponentialBackoff struct{}

// Name identifies the strategy.
func (ExponentialBackoff) Name() string { return BackoffExponential }

// Delay returns base * 2^(attempt-1) with full jitter, clamped to maxDelay.
func (ExponentialBackoff) Delay(attempt uint32, base, maxDelay time.Duration) time.Duration {
	if attempt == 0 {
		attempt = 1
	}
	// Cap the exponent before shifting so the multiplication cannot overflow.
	exponent := math.Min(float64(attempt-1), 32)
	scaled := float64(base) * math.Pow(2, exponent)
	if scaled > float64(maxDelay) {
		scaled = float64(maxDelay)
	}
	// Full jitter: uniformly sample [base, scaled] so retries spread out while
	// never dropping below the configured floor.
	if scaled <= float64(base) {
		return clampDelay(base, maxDelay)
	}
	jittered := float64(base) + rand.Float64()*(scaled-float64(base)) //nolint:gosec // jitter needs no crypto randomness
	return clampDelay(time.Duration(jittered), maxDelay)
}

// LinearBackoff grows the delay by base on every attempt.
type LinearBackoff struct{}

// Name identifies the strategy.
func (LinearBackoff) Name() string { return BackoffLinear }

// Delay returns base * attempt, clamped to maxDelay.
func (LinearBackoff) Delay(attempt uint32, base, maxDelay time.Duration) time.Duration {
	if attempt == 0 {
		attempt = 1
	}
	return clampDelay(time.Duration(attempt)*base, maxDelay)
}

// ConstantBackoff waits the same amount before every attempt.
type ConstantBackoff struct{}

// Name identifies the strategy.
func (ConstantBackoff) Name() string { return BackoffConstant }

// Delay returns base, clamped to maxDelay.
func (ConstantBackoff) Delay(_ uint32, base, maxDelay time.Duration) time.Duration {
	return clampDelay(base, maxDelay)
}

// clampDelay keeps a computed delay inside [0, maxDelay].
func clampDelay(d, maxDelay time.Duration) time.Duration {
	if d < 0 {
		return 0
	}
	if maxDelay > 0 && d > maxDelay {
		return maxDelay
	}
	return d
}
