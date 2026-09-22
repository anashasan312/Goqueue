package value_objects

import (
	"strings"

	"github.com/anashasan/goqueue/pkg/common/errors"
	jobErr "github.com/anashasan/goqueue/pkg/domain/job_aggregate/error"
)

// maxJobIDLength bounds the identifier so a malicious caller cannot blow up the
// Redis key space with an unbounded key.
const maxJobIDLength = 64

// JobID is the immutable identity of a Job aggregate.
//
// It is a distinct type rather than a bare string so that the compiler rejects
// passing a queue name or a task type where an identity is expected.
type JobID string

// NewJobID validates and constructs a JobID.
func NewJobID(raw string) (JobID, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", errors.Invalid(jobErr.EInvalidJobID, "job id must not be empty")
	}
	if len(trimmed) > maxJobIDLength {
		return "", errors.Invalid(jobErr.EInvalidJobID, "job id must not exceed 64 characters")
	}
	if strings.ContainsAny(trimmed, " \t\n{}:") {
		return "", errors.Invalid(jobErr.EInvalidJobID, "job id must not contain whitespace or reserved characters")
	}
	return JobID(trimmed), nil
}

// String renders the identity.
func (id JobID) String() string { return string(id) }

// IsZero reports whether the identity is unset.
func (id JobID) IsZero() bool { return id == "" }
