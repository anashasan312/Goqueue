// Package handlers holds the HTTP presentation layer.
//
// A handler's job is narrow on purpose: bind and validate the request shape,
// call one service method, and render the result. There is no business logic
// here, and no error-to-status mapping — that belongs to the error middleware.
package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/anashasan/goqueue/pkg/application/services"
	jobContr "github.com/anashasan/goqueue/pkg/contracts/job"
)

// JobHandler serves the job endpoints.
//
// It depends on the two service interfaces separately rather than on one
// combined façade, so the dependency list states exactly what this handler can
// do — and a reviewer can see at a glance that it cannot pause a queue.
type JobHandler struct {
	enqueuer  services.IEnqueuerService
	inspector services.IInspectorService
}

// NewJobHandler builds a JobHandler.
func NewJobHandler(
	enqueuer services.IEnqueuerService,
	inspector services.IInspectorService,
) *JobHandler {
	return &JobHandler{enqueuer: enqueuer, inspector: inspector}
}

// EnqueueJob handles POST /api/v1/jobs.
//
// Responses:
//   - 201 the job was created
//   - 200 an idempotency key matched an existing job, which is returned
//   - 400 the request violated a domain invariant
//   - 409 the idempotency key is held by a job that no longer exists
func (h *JobHandler) EnqueueJob(ctx *gin.Context) {
	var req jobContr.EnqueueJobReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		_ = ctx.Error(bindingError(err))
		return
	}

	res, err := h.enqueuer.Enqueue(ctx.Request.Context(), req)
	if err != nil {
		_ = ctx.Error(err)
		return
	}

	// A deduplicated enqueue created nothing, so 200 is the honest answer; 201
	// would tell the client a resource came into existence on this call.
	status := http.StatusCreated
	if res.Deduplicated {
		status = http.StatusOK
	}
	ctx.JSON(status, res)
}

// GetJob handles GET /api/v1/jobs/:job_id.
func (h *JobHandler) GetJob(ctx *gin.Context) {
	res, err := h.inspector.GetJob(ctx.Request.Context(), ctx.Param(paramJobID))
	if err != nil {
		_ = ctx.Error(err)
		return
	}
	ctx.JSON(http.StatusOK, res)
}

// ListJobs handles GET /api/v1/jobs.
func (h *JobHandler) ListJobs(ctx *gin.Context) {
	var query jobContr.ListJobsQuery
	if err := ctx.ShouldBindQuery(&query); err != nil {
		_ = ctx.Error(bindingError(err))
		return
	}

	res, err := h.inspector.ListJobs(ctx.Request.Context(), query)
	if err != nil {
		_ = ctx.Error(err)
		return
	}
	ctx.JSON(http.StatusOK, res)
}

// RetryJob handles POST /api/v1/jobs/:job_id/retry.
func (h *JobHandler) RetryJob(ctx *gin.Context) {
	res, err := h.inspector.RetryJob(ctx.Request.Context(), ctx.Param(paramJobID))
	if err != nil {
		_ = ctx.Error(err)
		return
	}
	ctx.JSON(http.StatusOK, res)
}

// KillJob handles POST /api/v1/jobs/:job_id/kill.
func (h *JobHandler) KillJob(ctx *gin.Context) {
	res, err := h.inspector.KillJob(ctx.Request.Context(), ctx.Param(paramJobID))
	if err != nil {
		_ = ctx.Error(err)
		return
	}
	ctx.JSON(http.StatusOK, res)
}

// DeleteJob handles DELETE /api/v1/jobs/:job_id.
func (h *JobHandler) DeleteJob(ctx *gin.Context) {
	if err := h.inspector.DeleteJob(ctx.Request.Context(), ctx.Param(paramJobID)); err != nil {
		_ = ctx.Error(err)
		return
	}
	ctx.Status(http.StatusNoContent)
}
