package job_aggregate

import (
	"time"

	"github.com/anashasan/goqueue/pkg/common/errors"
	jobErr "github.com/anashasan/goqueue/pkg/domain/job_aggregate/error"
)

// Retry policy defaults. They are deliberately conservative: five attempts over
// roughly a minute of exponential backoff covers a transient dependency blip
// without holding a worker slot hostage.
const (
	DefaultMaxRetries   uint32        = 5
	DefaultBaseDelay    time.Duration = 1 * time.Second
	DefaultMaxDelay     time.Duration = 5 * time.Minute
	MaxAllowedRetries   uint32        = 100
	MaxAllowedBaseDelay time.Duration = 1 * time.Hour
)

// RetryPolicy is an immutable value object describing how a job reacts to
// failure. It owns the decision of *whether* to retry and *when*; it delegates
// the shape of the waiting curve to a BackoffStrategy.
type RetryPolicy struct {
	maxRetries uint32
	baseDelay  time.Duration
	maxDelay   time.Duration
	strategy   BackoffStrategy
}

// NewRetryPolicy validates and constructs a RetryPolicy. Zero-valued inputs fall
// back to the documented defaults so that callers may omit the whole block.
func NewRetryPolicy(
	maxRetries uint32,
	baseDelay time.Duration,
	maxDelay time.Duration,
	strategyName string,
) (RetryPolicy, error) {
	if maxRetries > MaxAllowedRetries {
		return RetryPolicy{}, errors.Invalid(
			jobErr.EInvalidRetryPolicy,
			"max retries must not exceed 100",
		)
	}
	if baseDelay < 0 || baseDelay > MaxAllowedBaseDelay {
		return RetryPolicy{}, errors.Invalid(
			jobErr.EInvalidRetryPolicy,
			"base delay must be between 0 and 1h",
		)
	}
	if baseDelay == 0 {
		baseDelay = DefaultBaseDelay
	}
	if maxDelay == 0 {
		maxDelay = DefaultMaxDelay
	}
	if maxDelay < baseDelay {
		return RetryPolicy{}, errors.Invalid(
			jobErr.EInvalidRetryPolicy,
			"max delay must be greater than or equal to base delay",
		)
	}

	strategy, err := NewBackoffStrategy(strategyName)
	if err != nil {
		return RetryPolicy{}, err
	}

	return RetryPolicy{
		maxRetries: maxRetries,
		baseDelay:  baseDelay,
		maxDelay:   maxDelay,
		strategy:   strategy,
	}, nil
}

// DefaultRetryPolicy builds the policy applied when a caller supplies none.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		maxRetries: DefaultMaxRetries,
		baseDelay:  DefaultBaseDelay,
		maxDelay:   DefaultMaxDelay,
		strategy:   ExponentialBackoff{},
	}
}

// MaxRetries returns the number of retries allowed after the first attempt.
func (p RetryPolicy) MaxRetries() uint32 { return p.maxRetries }

// BaseDelay returns the unit delay fed to the strategy.
func (p RetryPolicy) BaseDelay() time.Duration { return p.baseDelay }

// MaxDelay returns the ceiling applied to every computed delay.
func (p RetryPolicy) MaxDelay() time.Duration { return p.maxDelay }

// StrategyName returns the persisted strategy identifier.
func (p RetryPolicy) StrategyName() string {
	if p.strategy == nil {
		return BackoffExponential
	}
	return p.strategy.Name()
}

// AllowsRetry reports whether a job that has already been attempted
// `attemptsMade` times may be retried again.
func (p RetryPolicy) AllowsRetry(attemptsMade uint32) bool {
	return attemptsMade <= p.maxRetries
}

// NextDelay returns how long to wait before the given retry attempt.
func (p RetryPolicy) NextDelay(attempt uint32) time.Duration {
	strategy := p.strategy
	if strategy == nil {
		strategy = ExponentialBackoff{}
	}
	return strategy.Delay(attempt, p.baseDelay, p.maxDelay)
}
