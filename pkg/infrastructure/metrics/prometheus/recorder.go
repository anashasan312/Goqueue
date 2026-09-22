// Package prometheus implements the metrics port on the Prometheus client.
//
// Four metrics carry the operational story of a queue — how much work flows
// through, how long it takes, how often it is retried, and how often it fails —
// and everything else on the dashboard is derived from them. They are defined
// here and nowhere else, so their names and labels cannot drift between the code
// that emits them and the alerts that read them.
package prometheus

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	jobVO "github.com/anashasan/goqueue/pkg/domain/job_aggregate/value_objects"
	"github.com/anashasan/goqueue/pkg/domain/metrics"
)

var _ metrics.Recorder = (*Recorder)(nil)

// Namespace and subsystem prefix every metric name.
const (
	namespace = "goqueue"
	subsystem = ""
)

// Label names. Every label here has bounded cardinality by construction: queue
// names and task types come from a registry, states from a closed enum, and
// failure reasons from the error-code constants. No label ever carries a job id.
const (
	labelQueue    = "queue"
	labelTaskType = "task_type"
	labelStatus   = "status"
	labelState    = "state"
	labelReason   = "reason"
)

// Recorder implements the metrics port.
type Recorder struct {
	jobsProcessed *prometheus.CounterVec
	jobDuration   *prometheus.HistogramVec
	jobRetries    *prometheus.CounterVec
	jobsFailed    *prometheus.CounterVec

	queueDepth    *prometheus.GaugeVec
	activeWorkers prometheus.Gauge
}

// NewRecorder builds a Recorder and registers its collectors.
//
// The registry is injected rather than taken from prometheus.DefaultRegisterer
// so that a test can build a Recorder against a throwaway registry, and so two
// Recorders in one process cannot collide on a duplicate registration.
func NewRecorder(registry prometheus.Registerer) *Recorder {
	r := &Recorder{
		// Metric 1 — throughput. Rate over this counter is the queue's
		// jobs-per-second, and splitting by status gives the success rate
		// without a second metric.
		jobsProcessed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "jobs_processed_total",
			Help:      "Total jobs that finished execution, by terminal outcome.",
		}, []string{labelQueue, labelTaskType, labelStatus}),

		// Metric 2 — latency. Buckets span 5ms to 60s because job handlers live
		// on a very different scale from HTTP requests: the default client
		// buckets top out at 10s and would collapse every slow job into +Inf.
		jobDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "job_processing_duration_seconds",
			Help:      "Wall-clock duration of a single job execution attempt.",
			Buckets: []float64{
				0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
				1, 2.5, 5, 10, 30, 60,
			},
		}, []string{labelQueue, labelTaskType}),

		// Metric 3 — retries. Compared against jobs_processed_total it shows how
		// much of the throughput is rework rather than progress.
		jobRetries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "job_retries_total",
			Help:      "Total retry attempts scheduled after a failed execution.",
		}, []string{labelQueue, labelTaskType}),

		// Metric 4 — failures, labelled by reason so a timeout is
		// distinguishable from a handler error or an unroutable task type.
		jobsFailed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "jobs_failed_total",
			Help:      "Total failed job executions, by failure reason.",
		}, []string{labelQueue, labelTaskType, labelReason}),

		queueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "queue_depth",
			Help:      "Number of jobs currently in a given queue and lifecycle state.",
		}, []string{labelQueue, labelState}),

		activeWorkers: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "active_workers",
			Help:      "Number of workers currently executing a job.",
		}),
	}

	registry.MustRegister(
		r.jobsProcessed,
		r.jobDuration,
		r.jobRetries,
		r.jobsFailed,
		r.queueDepth,
		r.activeWorkers,
	)

	return r
}

// RecordJobProcessed increments the throughput counter.
func (r *Recorder) RecordJobProcessed(
	queue jobVO.QueueName,
	taskType jobVO.TaskType,
	outcome metrics.Outcome,
) {
	r.jobsProcessed.WithLabelValues(queue.String(), taskType.String(), outcome.String()).Inc()
}

// RecordJobDuration observes handler latency.
func (r *Recorder) RecordJobDuration(
	queue jobVO.QueueName,
	taskType jobVO.TaskType,
	d time.Duration,
) {
	r.jobDuration.WithLabelValues(queue.String(), taskType.String()).Observe(d.Seconds())
}

// RecordJobRetry increments the retry counter.
func (r *Recorder) RecordJobRetry(queue jobVO.QueueName, taskType jobVO.TaskType) {
	r.jobRetries.WithLabelValues(queue.String(), taskType.String()).Inc()
}

// RecordJobFailed increments the failure counter.
func (r *Recorder) RecordJobFailed(
	queue jobVO.QueueName,
	taskType jobVO.TaskType,
	reason string,
) {
	if reason == "" {
		reason = "unknown"
	}
	r.jobsFailed.WithLabelValues(queue.String(), taskType.String(), reason).Inc()
}

// SetQueueDepth publishes the depth of one queue/state pair.
func (r *Recorder) SetQueueDepth(queue jobVO.QueueName, state jobVO.JobState, depth int64) {
	r.queueDepth.WithLabelValues(queue.String(), state.String()).Set(float64(depth))
}

// SetActiveWorkers publishes how many workers are executing a job.
func (r *Recorder) SetActiveWorkers(count int64) {
	r.activeWorkers.Set(float64(count))
}
