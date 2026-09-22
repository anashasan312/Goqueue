package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/anashasan/goqueue/pkg/application/services"
	queueContr "github.com/anashasan/goqueue/pkg/contracts/queue"
)

// QueueHandler serves the queue endpoints.
type QueueHandler struct {
	inspector services.IInspectorService
}

// NewQueueHandler builds a QueueHandler.
func NewQueueHandler(inspector services.IInspectorService) *QueueHandler {
	return &QueueHandler{inspector: inspector}
}

// ListQueueStats handles GET /api/v1/queues.
func (h *QueueHandler) ListQueueStats(ctx *gin.Context) {
	res, err := h.inspector.ListQueueStats(ctx.Request.Context())
	if err != nil {
		_ = ctx.Error(err)
		return
	}
	ctx.JSON(http.StatusOK, res)
}

// SetQueuePaused handles PATCH /api/v1/queues/:queue_name/pause.
func (h *QueueHandler) SetQueuePaused(ctx *gin.Context) {
	var req queueContr.PauseQueueReq
	if err := ctx.ShouldBindJSON(&req); err != nil {
		_ = ctx.Error(bindingError(err))
		return
	}

	res, err := h.inspector.SetQueuePaused(
		ctx.Request.Context(), ctx.Param(paramQueueName), req.Paused,
	)
	if err != nil {
		_ = ctx.Error(err)
		return
	}
	ctx.JSON(http.StatusOK, res)
}

// RetryAllDead handles POST /api/v1/queues/:queue_name/dead/retry.
func (h *QueueHandler) RetryAllDead(ctx *gin.Context) {
	res, err := h.inspector.RetryAllDead(ctx.Request.Context(), ctx.Param(paramQueueName))
	if err != nil {
		_ = ctx.Error(err)
		return
	}
	ctx.JSON(http.StatusOK, res)
}
