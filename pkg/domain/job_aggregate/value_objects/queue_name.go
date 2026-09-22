package value_objects

import (
	"regexp"
	"strings"

	"github.com/anashasan/goqueue/pkg/common/errors"
	jobErr "github.com/anashasan/goqueue/pkg/domain/job_aggregate/error"
)

// DefaultQueueName is used when a caller enqueues without naming a queue.
const DefaultQueueName = "default"

// queueNamePattern constrains queue names to characters that are safe inside a
// Redis key and readable in a Grafana label.
var queueNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,62}$`)

// QueueName identifies a logical queue. Jobs in different queues are isolated:
// they are drained independently and reported on independently.
type QueueName string

// NewQueueName validates and constructs a QueueName. An empty input yields the
// default queue, which keeps the common enqueue path free of ceremony.
func NewQueueName(raw string) (QueueName, error) {
	trimmed := strings.ToLower(strings.TrimSpace(raw))
	if trimmed == "" {
		return DefaultQueueName, nil
	}
	if !queueNamePattern.MatchString(trimmed) {
		return "", errors.Invalid(
			jobErr.EInvalidQueueName,
			"queue name must match ^[a-z0-9][a-z0-9_.-]{0,62}$",
		)
	}
	return QueueName(trimmed), nil
}

// MustNewQueueName constructs a QueueName and panics on failure. It is intended
// only for package level constants and configuration parsed at startup, where a
// bad value should stop the process rather than surface at request time.
func MustNewQueueName(raw string) QueueName {
	name, err := NewQueueName(raw)
	if err != nil {
		panic(err)
	}
	return name
}

// String renders the queue name.
func (q QueueName) String() string { return string(q) }
