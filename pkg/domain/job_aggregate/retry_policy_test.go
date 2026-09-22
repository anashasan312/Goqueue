package job_aggregate_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	jobAgg "github.com/anashasan/goqueue/pkg/domain/job_aggregate"
	jobErr "github.com/anashasan/goqueue/pkg/domain/job_aggregate/error"
)

func TestNewRetryPolicy_AppliesDefaultsForZeroValues(t *testing.T) {
	policy, err := jobAgg.NewRetryPolicy(0, 0, 0, "")
	require.NoError(t, err)

	assert.Equal(t, uint32(0), policy.MaxRetries())
	assert.Equal(t, jobAgg.DefaultBaseDelay, policy.BaseDelay())
	assert.Equal(t, jobAgg.DefaultMaxDelay, policy.MaxDelay())
	assert.Equal(t, jobAgg.BackoffExponential, policy.StrategyName())
}

func TestNewRetryPolicy_RejectsInconsistentDelays(t *testing.T) {
	_, err := jobAgg.NewRetryPolicy(3, time.Minute, time.Second, jobAgg.BackoffLinear)

	assertCode(t, err, jobErr.EInvalidRetryPolicy)
}

func TestNewRetryPolicy_RejectsAnUnknownStrategy(t *testing.T) {
	_, err := jobAgg.NewRetryPolicy(3, time.Second, time.Minute, "fibonacci")

	assertCode(t, err, jobErr.EInvalidRetryPolicy)
}

func TestRetryPolicy_AllowsRetryUpToTheBudget(t *testing.T) {
	policy, err := jobAgg.NewRetryPolicy(2, time.Second, time.Minute, jobAgg.BackoffConstant)
	require.NoError(t, err)

	assert.True(t, policy.AllowsRetry(1))
	assert.True(t, policy.AllowsRetry(2))
	assert.False(t, policy.AllowsRetry(3))
}

func TestExponentialBackoff_GrowsAndStaysWithinBounds(t *testing.T) {
	strategy := jobAgg.ExponentialBackoff{}
	base := time.Second
	maxDelay := 30 * time.Second

	// Jitter makes each delay a sample rather than a fixed value, so the useful
	// assertions are the bounds, not an exact number. Asserting on a specific
	// jittered value would produce a test that fails one run in a thousand.
	for attempt := uint32(1); attempt <= 10; attempt++ {
		delay := strategy.Delay(attempt, base, maxDelay)
		assert.GreaterOrEqual(t, delay, base,
			"attempt %d fell below the base delay", attempt)
		assert.LessOrEqual(t, delay, maxDelay,
			"attempt %d exceeded the cap", attempt)
	}
}

func TestExponentialBackoff_DoesNotOverflowAtExtremeAttempts(t *testing.T) {
	// 2^1000 would overflow a naive implementation and produce a negative or
	// infinite duration; the exponent is clamped precisely to stop that.
	delay := jobAgg.ExponentialBackoff{}.Delay(1000, time.Second, time.Hour)

	assert.Positive(t, delay)
	assert.LessOrEqual(t, delay, time.Hour)
}

func TestLinearBackoff_GrowsByOneBaseDelayPerAttempt(t *testing.T) {
	strategy := jobAgg.LinearBackoff{}

	assert.Equal(t, 1*time.Second, strategy.Delay(1, time.Second, time.Minute))
	assert.Equal(t, 5*time.Second, strategy.Delay(5, time.Second, time.Minute))
	// Clamped at the ceiling.
	assert.Equal(t, time.Minute, strategy.Delay(120, time.Second, time.Minute))
}

func TestConstantBackoff_IgnoresTheAttemptNumber(t *testing.T) {
	strategy := jobAgg.ConstantBackoff{}

	assert.Equal(t, 5*time.Second, strategy.Delay(1, 5*time.Second, time.Minute))
	assert.Equal(t, 5*time.Second, strategy.Delay(99, 5*time.Second, time.Minute))
}

func TestNewBackoffStrategy_ResolvesEveryRegisteredName(t *testing.T) {
	cases := map[string]string{
		"":                        jobAgg.BackoffExponential,
		jobAgg.BackoffExponential: jobAgg.BackoffExponential,
		jobAgg.BackoffLinear:      jobAgg.BackoffLinear,
		jobAgg.BackoffConstant:    jobAgg.BackoffConstant,
		"EXPONENTIAL":             jobAgg.BackoffExponential,
	}

	for input, expected := range cases {
		strategy, err := jobAgg.NewBackoffStrategy(input)
		require.NoError(t, err, "input %q", input)
		assert.Equal(t, expected, strategy.Name(), "input %q", input)
	}
}
