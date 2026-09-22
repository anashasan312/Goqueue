package server

import (
	"github.com/gin-gonic/gin"

	h "github.com/anashasan/goqueue/pkg/api/handlers"
)

// registerQueueRoutes mounts the queue resource.
//
//	GET   /api/v1/queues                           depth snapshot of every queue
//	PATCH /api/v1/queues/:queue_name/pause         suspend or resume consumption
//	POST  /api/v1/queues/:queue_name/dead/retry    requeue every dead job
func registerQueueRoutes(api *gin.RouterGroup, handler *h.QueueHandler) {
	queues := api.Group("/queues")

	queues.GET("", handler.ListQueueStats)
	queues.PATCH("/:queue_name/pause", handler.SetQueuePaused)
	queues.POST("/:queue_name/dead/retry", handler.RetryAllDead)
}
