// Package hydrator maps domain aggregates onto API contracts.
//
// The mapping lives in its own package so that neither side has to know about
// the other: the aggregate stays free of json tags, and the contract stays free
// of value-object types. It is one-directional by design — inbound requests are
// translated inside the application services, where the validation errors they
// can produce belong.
package hydrator

import (
	"encoding/json"
	"time"

	jobContr "github.com/anashasan/goqueue/pkg/contracts/job"
	queueContr "github.com/anashasan/goqueue/pkg/contracts/queue"
	jobAgg "github.com/anashasan/goqueue/pkg/domain/job_aggregate"
	queueAgg "github.com/anashasan/goqueue/pkg/domain/queue_aggregate"
)

// ToJobRes maps a Job aggregate onto its full API representation.
func ToJobRes(job *jobAgg.Job) jobContr.JobRes {
	policy := job.RetryPolicy()

	return jobContr.JobRes{
		ID:               job.ID().String(),
		Queue:            job.Queue().String(),
		TaskType:         job.TaskType().String(),
		State:            job.State().String(),
		Priority:         job.Priority().String(),
		Payload:          toRawPayload(job.Payload()),
		AttemptsMade:     job.AttemptsMade(),
		RemainingRetries: job.RemainingRetries(),
		LastError:        job.LastError(),
		TimeoutSeconds:   int64(job.Timeout().Seconds()),
		IdempotencyKey:   job.IdempotencyKey().String(),
		RetryPolicy: jobContr.RetryPolicyRes{
			MaxRetries:            policy.MaxRetries(),
			RetryBaseDelaySeconds: int64(policy.BaseDelay().Seconds()),
			RetryMaxDelaySeconds:  int64(policy.MaxDelay().Seconds()),
			BackoffStrategy:       policy.StrategyName(),
		},
		Attempts:    toAttemptsRes(job.Attempts()),
		CreatedAt:   formatTime(job.CreatedAt()),
		ProcessAt:   formatTime(job.ProcessAt()),
		CompletedAt: formatTimePtr(job.CompletedAt()),
		DiedAt:      formatTimePtr(job.DiedAt()),
		LeaseExpiry: formatTimePtr(job.LeaseExpiry()),
		WorkerID:    job.WorkerID(),
	}
}

// ToJobSummaryRes maps a Job aggregate onto its listing representation.
func ToJobSummaryRes(job *jobAgg.Job) jobContr.JobSummaryRes {
	return jobContr.JobSummaryRes{
		ID:               job.ID().String(),
		Queue:            job.Queue().String(),
		TaskType:         job.TaskType().String(),
		State:            job.State().String(),
		Priority:         job.Priority().String(),
		AttemptsMade:     job.AttemptsMade(),
		RemainingRetries: job.RemainingRetries(),
		LastError:        job.LastError(),
		CreatedAt:        formatTime(job.CreatedAt()),
		ProcessAt:        formatTime(job.ProcessAt()),
		WorkerID:         job.WorkerID(),
	}
}

// ToJobSummaryResList maps a page of aggregates.
func ToJobSummaryResList(jobs []*jobAgg.Job) []jobContr.JobSummaryRes {
	out := make([]jobContr.JobSummaryRes, 0, len(jobs))
	for _, job := range jobs {
		out = append(out, ToJobSummaryRes(job))
	}
	return out
}

// ToEnqueueJobRes maps a freshly created job onto its enqueue acknowledgement.
func ToEnqueueJobRes(job *jobAgg.Job, deduplicated bool) jobContr.EnqueueJobRes {
	return jobContr.EnqueueJobRes{
		ID:           job.ID().String(),
		Queue:        job.Queue().String(),
		TaskType:     job.TaskType().String(),
		State:        job.State().String(),
		Priority:     job.Priority().String(),
		ProcessAt:    formatTime(job.ProcessAt()),
		CreatedAt:    formatTime(job.CreatedAt()),
		Deduplicated: deduplicated,
	}
}

// ToQueueStatsRes maps a queue stats read model onto its API representation.
func ToQueueStatsRes(stats queueAgg.Stats) queueContr.QueueStatsRes {
	return queueContr.QueueStatsRes{
		Queue:     stats.Queue.String(),
		Paused:    stats.Paused,
		Weight:    stats.Weight.Uint8(),
		Pending:   stats.Pending,
		Active:    stats.Active,
		Scheduled: stats.Scheduled,
		Retrying:  stats.Retrying,
		Completed: stats.Completed,
		Dead:      stats.Dead,
		Backlog:   stats.Backlog(),
		Total:     stats.Total(),
	}
}

// ToListQueueStatsRes maps every queue and rolls up the totals.
func ToListQueueStatsRes(stats []queueAgg.Stats) queueContr.ListQueueStatsRes {
	queues := make([]queueContr.QueueStatsRes, 0, len(stats))
	totals := queueContr.QueueStatsRes{Queue: "all"}

	for _, s := range stats {
		queues = append(queues, ToQueueStatsRes(s))
		totals.Pending += s.Pending
		totals.Active += s.Active
		totals.Scheduled += s.Scheduled
		totals.Retrying += s.Retrying
		totals.Completed += s.Completed
		totals.Dead += s.Dead
	}
	totals.Backlog = totals.Pending + totals.Active + totals.Scheduled + totals.Retrying
	totals.Total = totals.Backlog + totals.Completed + totals.Dead

	return queueContr.ListQueueStatsRes{Queues: queues, Totals: totals}
}

// toAttemptsRes maps the execution trail.
func toAttemptsRes(attempts []jobAgg.Attempt) []jobContr.AttemptRes {
	out := make([]jobContr.AttemptRes, 0, len(attempts))
	for _, a := range attempts {
		out = append(out, jobContr.AttemptRes{
			Number:     a.Number,
			StartedAt:  formatTime(a.StartedAt),
			FinishedAt: formatTime(a.FinishedAt),
			DurationMS: a.Duration().Milliseconds(),
			Error:      a.Error,
			WorkerID:   a.WorkerID,
		})
	}
	return out
}

// toRawPayload passes a JSON payload through untouched and wraps a non-JSON one
// as a JSON string, so the response is always valid JSON regardless of what a
// producer enqueued.
func toRawPayload(payload []byte) json.RawMessage {
	if len(payload) == 0 {
		return nil
	}
	if json.Valid(payload) {
		return json.RawMessage(payload)
	}
	encoded, err := json.Marshal(string(payload))
	if err != nil {
		return nil
	}
	return json.RawMessage(encoded)
}

// formatTime renders an instant as RFC3339, mapping the zero time to an empty
// string so clients can distinguish "not set" from "set to the epoch".
func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// formatTimePtr renders an optional instant.
func formatTimePtr(t *time.Time) string {
	if t == nil {
		return ""
	}
	return formatTime(*t)
}
