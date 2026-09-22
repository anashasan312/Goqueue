// Package persistence declares the outbound ports owned by the domain.
//
// The interfaces live here, beside the aggregates they serve, and the concrete
// Redis types live in pkg/infrastructure. Dependencies therefore point inwards:
// infrastructure imports domain, never the reverse.
package persistence

import (
	"context"

	jobAgg "github.com/anashasan/goqueue/pkg/domain/job_aggregate"
	jobVO "github.com/anashasan/goqueue/pkg/domain/job_aggregate/value_objects"
)

// JobFilter narrows a job listing. A zero value matches everything.
type JobFilter struct {
	// Queue restricts the listing to one queue. Empty means all queues.
	Queue jobVO.QueueName
	// State restricts the listing to one lifecycle state. Empty means all.
	State jobVO.JobState
	// TaskType restricts the listing to one handler routing key.
	TaskType jobVO.TaskType
	// Offset is the number of matches to skip.
	Offset int64
	// Limit caps the page size. Zero means the repository's default.
	Limit int64
}

// JobPage is one page of a job listing plus the total match count, which the
// dashboard needs to render pagination.
type JobPage struct {
	Jobs  []*jobAgg.Job
	Total int64
}

// IJobRepo persists and retrieves Job aggregates.
//
// It is intentionally separate from IJobBroker: this port answers "what is the
// state of job X", the broker answers "give me the next job to run". Splitting
// them keeps read-only callers such as the dashboard from depending on queueing
// operations they must never invoke.
type IJobRepo interface {
	// Save writes the full job document, creating or overwriting it.
	Save(ctx context.Context, job *jobAgg.Job) error

	// FindByID returns one job. It returns a KindNotFound error when the id is
	// unknown.
	FindByID(ctx context.Context, id jobVO.JobID) (*jobAgg.Job, error)

	// List returns a filtered, paginated page of jobs.
	List(ctx context.Context, filter JobFilter) (JobPage, error)

	// Delete removes a job document and its index entries.
	Delete(ctx context.Context, id jobVO.JobID) error

	// PurgeCompleted removes completed jobs whose retention window has elapsed
	// and reports how many were removed.
	PurgeCompleted(ctx context.Context, queue jobVO.QueueName, limit int64) (int64, error)
}
