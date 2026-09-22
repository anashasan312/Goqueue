package value_objects

import (
	"regexp"
	"strings"

	"github.com/anashasan/goqueue/pkg/common/errors"
	jobErr "github.com/anashasan/goqueue/pkg/domain/job_aggregate/error"
)

// taskTypePattern allows dotted names such as "email.welcome" or "image:resize".
var taskTypePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,126}$`)

// TaskType names the unit of work a job represents. The worker pool routes a job
// to a handler by this value, so it is part of the aggregate's identity contract
// rather than free-form metadata.
type TaskType string

// NewTaskType validates and constructs a TaskType.
func NewTaskType(raw string) (TaskType, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", errors.Invalid(jobErr.EInvalidTaskType, "task type must not be empty")
	}
	if !taskTypePattern.MatchString(trimmed) {
		return "", errors.Invalid(
			jobErr.EInvalidTaskType,
			"task type must match ^[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,126}$",
		)
	}
	return TaskType(trimmed), nil
}

// String renders the task type.
func (t TaskType) String() string { return string(t) }
