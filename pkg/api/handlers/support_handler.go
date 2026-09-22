package handlers

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/anashasan/goqueue/pkg/application/services"
	"github.com/anashasan/goqueue/pkg/common/errors"
)

// Route parameter names, declared once so a typo in a handler cannot silently
// read an always-empty parameter.
const (
	paramJobID     = "job_id"
	paramQueueName = "queue_name"
)

// HealthRes is the liveness payload.
type HealthRes struct {
	Status  string `json:"status"`
	Service string `json:"service"`
	Version string `json:"version"`
	Uptime  string `json:"uptime"`
}

// ReadyRes is the readiness payload.
type ReadyRes struct {
	Status          string   `json:"status"`
	RegisteredTasks []string `json:"registered_tasks"`
}

// SupportHandler serves health, readiness and service metadata.
type SupportHandler struct {
	registry  services.IHandlerRegistry
	version   string
	startedAt time.Time
}

// NewSupportHandler builds a SupportHandler.
func NewSupportHandler(registry services.IHandlerRegistry, version string) *SupportHandler {
	return &SupportHandler{
		registry:  registry,
		version:   version,
		startedAt: time.Now(),
	}
}

// Health handles GET /health.
//
// Liveness answers "is this process alive", so it deliberately checks nothing
// external. A Redis outage must not make Kubernetes restart every pod: the pods
// are fine, the dependency is not, and restarting them makes the incident worse.
func (h *SupportHandler) Health(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, HealthRes{
		Status:  "ok",
		Service: "goqueue",
		Version: h.version,
		Uptime:  time.Since(h.startedAt).Round(time.Second).String(),
	})
}

// Ready handles GET /ready.
//
// Readiness answers "can this process do useful work". A worker with no
// registered handlers would accept jobs and dead-letter every one of them, so it
// reports unready and stays out of rotation.
func (h *SupportHandler) Ready(ctx *gin.Context) {
	types := h.registry.RegisteredTypes()
	names := make([]string, 0, len(types))
	for _, t := range types {
		names = append(names, t.String())
	}

	if len(names) == 0 {
		_ = ctx.Error(errors.New(
			errors.KindUnavailable,
			"no_handlers_registered",
			"no task handlers are registered",
		))
		return
	}

	ctx.JSON(http.StatusOK, ReadyRes{Status: "ready", RegisteredTasks: names})
}

// bindingError converts a gin binding failure into a domain error so it flows
// through the same middleware as every other error.
//
// The validator message is passed through because it names the offending field,
// which is exactly what a client needs; there is no sensitive information in a
// struct tag.
func bindingError(err error) error {
	return errors.Invalid("invalid_request", err.Error())
}
