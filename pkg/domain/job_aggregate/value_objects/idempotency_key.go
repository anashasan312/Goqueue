package value_objects

import (
	"strings"

	"github.com/anashasan/goqueue/pkg/common/errors"
	jobErr "github.com/anashasan/goqueue/pkg/domain/job_aggregate/error"
)

// maxIdempotencyKeyLength bounds the key so the Redis key space stays bounded.
const maxIdempotencyKeyLength = 128

// IdempotencyKey is a caller-supplied deduplication token. While a key is held,
// a second enqueue carrying the same key is rejected instead of creating a
// duplicate job. The key is optional: the zero value means "no deduplication".
type IdempotencyKey string

// NewIdempotencyKey validates and constructs an IdempotencyKey. An empty input
// is legal and yields the zero value.
func NewIdempotencyKey(raw string) (IdempotencyKey, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", nil
	}
	if len(trimmed) > maxIdempotencyKeyLength {
		return "", errors.Invalid(
			jobErr.EInvalidIdempotencyKey,
			"idempotency key must not exceed 128 characters",
		)
	}
	if strings.ContainsAny(trimmed, " \t\n") {
		return "", errors.Invalid(
			jobErr.EInvalidIdempotencyKey,
			"idempotency key must not contain whitespace",
		)
	}
	return IdempotencyKey(trimmed), nil
}

// String renders the key.
func (k IdempotencyKey) String() string { return string(k) }

// IsZero reports whether deduplication is disabled for the job.
func (k IdempotencyKey) IsZero() bool { return k == "" }
