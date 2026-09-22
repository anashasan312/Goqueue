package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/anashasan/goqueue/pkg/infrastructure/web"
)

// DashboardHandler serves the embedded web dashboard.
//
// The dashboard is a single embedded HTML file that talks to the same public
// REST API a client would use. That is deliberate: there is no private
// dashboard-only endpoint, so every action the UI can take is one an operator
// can also script, and the UI cannot drift ahead of the API.
type DashboardHandler struct{}

// NewDashboardHandler builds a DashboardHandler.
func NewDashboardHandler() *DashboardHandler { return &DashboardHandler{} }

// Index handles GET /dashboard.
func (h *DashboardHandler) Index(ctx *gin.Context) {
	ctx.Header("Content-Type", "text/html; charset=utf-8")
	// The page is rebuilt from the API on every load, so caching it would only
	// serve a stale shell after a deploy.
	ctx.Header("Cache-Control", "no-store")
	ctx.String(http.StatusOK, web.DashboardHTML)
}
