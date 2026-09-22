// Package inspector implements the read and operate side of the job API.
package inspector

import (
	"context"

	"github.com/anashasan/goqueue/pkg/application/services"
	"github.com/anashasan/goqueue/pkg/common/clock"
	"github.com/anashasan/goqueue/pkg/common/logger"
	jobContr "github.com/anashasan/goqueue/pkg/contracts/job"
	queueContr "github.com/anashasan/goqueue/pkg/contracts/queue"
	jobVO "github.com/anashasan/goqueue/pkg/domain/job_aggregate/value_objects"
	"github.com/anashasan/goqueue/pkg/domain/persistence"
	"github.com/anashasan/goqueue/pkg/hydrator"
)

var _ services.IInspectorService = (*InspectorService)(nil)

// maxBulkRetry caps a single "retry all dead" sweep so an operator cannot flood
// a queue with a hundred thousand jobs in one click.
const maxBulkRetry int64 = 1000

// InspectorService serves the dashboard and the operator endpoints.
type InspectorService struct {
	jobRepo   persistence.IJobRepo
	queueRepo persistence.IQueueRepo
	broker    persistence.IJobBroker
	clock     clock.Clock
	log       logger.Logger
}

// NewInspectorService builds an InspectorService.
func NewInspectorService(
	jobRepo persistence.IJobRepo,
	queueRepo persistence.IQueueRepo,
	broker persistence.IJobBroker,
	clk clock.Clock,
	log logger.Logger,
) *InspectorService {
	return &InspectorService{
		jobRepo:   jobRepo,
		queueRepo: queueRepo,
		broker:    broker,
		clock:     clk,
		log:       log,
	}
}

// GetJob returns one job in full.
func (s *InspectorService) GetJob(ctx context.Context, rawID string) (*jobContr.JobRes, error) {
	id, err := jobVO.NewJobID(rawID)
	if err != nil {
		return nil, err
	}
	job, err := s.jobRepo.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}
	res := hydrator.ToJobRes(job)
	return &res, nil
}

// ListJobs returns a filtered, paginated page of jobs.
func (s *InspectorService) ListJobs(
	ctx context.Context,
	query jobContr.ListJobsQuery,
) (*jobContr.ListJobsRes, error) {
	filter, err := toFilter(query)
	if err != nil {
		return nil, err
	}

	page, err := s.jobRepo.List(ctx, filter)
	if err != nil {
		return nil, err
	}

	return &jobContr.ListJobsRes{
		Jobs:   hydrator.ToJobSummaryResList(page.Jobs),
		Total:  page.Total,
		Offset: filter.Offset,
		Limit:  filter.Limit,
	}, nil
}

// RetryJob returns a dead job to its queue with a fresh retry budget.
//
// The aggregate's Requeue method is what rejects a job that is not dead, so this
// service does not re-check the state: one rule, one place.
func (s *InspectorService) RetryJob(ctx context.Context, rawID string) (*jobContr.JobActionRes, error) {
	id, err := jobVO.NewJobID(rawID)
	if err != nil {
		return nil, err
	}

	job, err := s.jobRepo.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := job.Requeue(s.clock.Now()); err != nil {
		return nil, err
	}
	if err := s.broker.Requeue(ctx, job); err != nil {
		return nil, err
	}

	s.log.Info(ctx, "job requeued by operator",
		logger.F("job_id", job.ID().String()),
		logger.F("queue", job.Queue().String()),
	)

	return &jobContr.JobActionRes{ID: job.ID().String(), State: job.State().String()}, nil
}

// KillJob moves a job to the dead-letter queue, skipping remaining retries.
func (s *InspectorService) KillJob(ctx context.Context, rawID string) (*jobContr.JobActionRes, error) {
	id, err := jobVO.NewJobID(rawID)
	if err != nil {
		return nil, err
	}

	job, err := s.jobRepo.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if err := job.Kill("killed by operator", s.clock.Now()); err != nil {
		return nil, err
	}
	if err := s.broker.Kill(ctx, job); err != nil {
		return nil, err
	}

	s.log.Info(ctx, "job killed by operator", logger.F("job_id", job.ID().String()))

	return &jobContr.JobActionRes{ID: job.ID().String(), State: job.State().String()}, nil
}

// DeleteJob removes a job entirely.
func (s *InspectorService) DeleteJob(ctx context.Context, rawID string) error {
	id, err := jobVO.NewJobID(rawID)
	if err != nil {
		return err
	}
	if err := s.jobRepo.Delete(ctx, id); err != nil {
		return err
	}
	s.log.Info(ctx, "job deleted by operator", logger.F("job_id", id.String()))
	return nil
}

// RetryAllDead requeues every dead job in a queue.
//
// Failures are counted rather than aborted on: one job whose document expired
// mid-sweep should not stop the other nine hundred from being retried, and the
// caller gets an honest count of what actually moved.
func (s *InspectorService) RetryAllDead(
	ctx context.Context,
	rawQueue string,
) (*jobContr.BulkActionRes, error) {
	queue, err := jobVO.NewQueueName(rawQueue)
	if err != nil {
		return nil, err
	}

	page, err := s.jobRepo.List(ctx, persistence.JobFilter{
		Queue: queue,
		State: jobVO.StateDead,
		Limit: maxBulkRetry,
	})
	if err != nil {
		return nil, err
	}

	now := s.clock.Now()
	var affected int64

	for _, job := range page.Jobs {
		if err := job.Requeue(now); err != nil {
			continue
		}
		if err := s.broker.Requeue(ctx, job); err != nil {
			s.log.Error(ctx, "failed to requeue dead job during bulk retry", err,
				logger.F("job_id", job.ID().String()))
			continue
		}
		affected++
	}

	s.log.Info(ctx, "bulk retry completed",
		logger.F("queue", queue.String()),
		logger.F("affected", affected),
	)

	return &jobContr.BulkActionRes{Affected: affected}, nil
}

// ListQueueStats returns the depth snapshot of every queue.
func (s *InspectorService) ListQueueStats(ctx context.Context) (*queueContr.ListQueueStatsRes, error) {
	stats, err := s.queueRepo.StatsAll(ctx)
	if err != nil {
		return nil, err
	}
	res := hydrator.ToListQueueStatsRes(stats)
	return &res, nil
}

// SetQueuePaused suspends or resumes consumption of a queue.
func (s *InspectorService) SetQueuePaused(
	ctx context.Context,
	rawQueue string,
	paused bool,
) (*queueContr.QueueActionRes, error) {
	queue, err := jobVO.NewQueueName(rawQueue)
	if err != nil {
		return nil, err
	}
	if _, err := s.queueRepo.FindByName(ctx, queue); err != nil {
		return nil, err
	}
	if err := s.queueRepo.SetPaused(ctx, queue, paused); err != nil {
		return nil, err
	}

	s.log.Info(ctx, "queue pause state changed",
		logger.F("queue", queue.String()),
		logger.F("paused", paused),
	)

	return &queueContr.QueueActionRes{Queue: queue.String(), Paused: paused}, nil
}

// toFilter translates the query DTO into a domain filter.
func toFilter(query jobContr.ListJobsQuery) (persistence.JobFilter, error) {
	filter := persistence.JobFilter{Offset: query.Offset, Limit: query.Limit}
	if filter.Limit <= 0 {
		filter.Limit = 50
	}

	if query.Queue != "" {
		queue, err := jobVO.NewQueueName(query.Queue)
		if err != nil {
			return persistence.JobFilter{}, err
		}
		filter.Queue = queue
	}
	if query.State != "" {
		state, err := jobVO.NewJobState(query.State)
		if err != nil {
			return persistence.JobFilter{}, err
		}
		filter.State = state
	}
	if query.TaskType != "" {
		taskType, err := jobVO.NewTaskType(query.TaskType)
		if err != nil {
			return persistence.JobFilter{}, err
		}
		filter.TaskType = taskType
	}

	return filter, nil
}
