package server

import (
	"github.com/gin-gonic/gin"

	h "github.com/anashasan/goqueue/pkg/api/handlers"
)

// registerJobRoutes mounts the job resource.
//
//	POST   /api/v1/jobs                 enqueue a job, immediately or scheduled
//	GET    /api/v1/jobs                 list jobs, filtered and paginated
//	GET    /api/v1/jobs/:job_id         fetch one job with its full attempt trail
//	POST   /api/v1/jobs/:job_id/retry   requeue a dead job with a fresh budget
//	POST   /api/v1/jobs/:job_id/kill    move a job to the dead-letter queue
//	DELETE /api/v1/jobs/:job_id         remove a job entirely
//
// Retry and kill are POSTs on a sub-path rather than a PATCH on the job, because
// they are commands with side effects beyond the job document — they move it
// between queues — and modelling them as a field update would hide that.
func registerJobRoutes(api *gin.RouterGroup, handler *h.JobHandler) {
	jobs := api.Group("/jobs")

	jobs.POST("", handler.EnqueueJob)
	jobs.GET("", handler.ListJobs)
	jobs.GET("/:job_id", handler.GetJob)
	jobs.POST("/:job_id/retry", handler.RetryJob)
	jobs.POST("/:job_id/kill", handler.KillJob)
	jobs.DELETE("/:job_id", handler.DeleteJob)
}
