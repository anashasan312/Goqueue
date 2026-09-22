// Package server owns the HTTP layer's lifecycle: building the two engines,
// registering routes, and starting and stopping them cleanly.
package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	prom "github.com/prometheus/client_golang/prometheus"

	h "github.com/anashasan/goqueue/pkg/api/handlers"
	mw "github.com/anashasan/goqueue/pkg/api/middleware"
	"github.com/anashasan/goqueue/pkg/common/logger"
	"github.com/anashasan/goqueue/pkg/common/uid"
	"github.com/anashasan/goqueue/pkg/infrastructure/config"
	promInfra "github.com/anashasan/goqueue/pkg/infrastructure/metrics/prometheus"
)

// Route prefixes.
const (
	// APIBasePath is the versioned public API.
	APIBasePath = "/api/v1"
	// DashboardPath serves the operator UI on the private engine.
	DashboardPath = "/dashboard"
)

// HTTPServer runs the public and private listeners.
//
// Two engines rather than one is a deployment decision made structural: the
// public port carries the job API that application clients call, and the private
// port carries metrics, health and the operator dashboard. A cluster can then
// expose one through an ingress and keep the other on the internal network,
// which is not possible if a single mux serves both.
type HTTPServer struct {
	publicEngine  *gin.Engine
	privateEngine *gin.Engine
	handlers      *h.Handlers
	registry      *prom.Registry
	log           logger.Logger
	cfg           *config.AppConfig

	servers []*http.Server
	mu      sync.Mutex
}

// NewHTTPServer builds both engines with their middleware and routes.
func NewHTTPServer(
	cfg *config.AppConfig,
	handlers *h.Handlers,
	registry *prom.Registry,
	ids uid.Generator,
	log logger.Logger,
) *HTTPServer {
	if cfg.IsProduction() {
		gin.SetMode(gin.ReleaseMode)
	}

	s := &HTTPServer{
		publicEngine:  gin.New(),
		privateEngine: gin.New(),
		handlers:      handlers,
		registry:      registry,
		log:           log,
		cfg:           cfg,
	}

	// Middleware order matters and is not arbitrary. Recovery is outermost so it
	// catches a panic raised anywhere inside; RequestID comes next so every log
	// line below it is correlated; the error handler runs last so it sees the
	// errors the handlers attach.
	for _, engine := range []*gin.Engine{s.publicEngine, s.privateEngine} {
		engine.Use(
			mw.Recovery(log),
			mw.RequestID(ids),
			mw.RequestLogger(log),
			mw.ErrorHandler(log),
		)
	}

	s.registerPublicRoutes()
	s.registerPrivateRoutes()

	return s
}

// Start launches both listeners and returns once they are accepting.
//
// A listener that dies unexpectedly is reported through the returned channel
// rather than crashing the process from inside a goroutine, so main can decide
// whether to shut the rest down gracefully.
func (s *HTTPServer) Start(ctx context.Context) <-chan error {
	failures := make(chan error, 2)

	s.mu.Lock()
	s.servers = []*http.Server{
		s.newServer(s.cfg.Server.PublicPort, s.publicEngine),
		s.newServer(s.cfg.Server.PrivatePort, s.privateEngine),
	}
	servers := s.servers
	s.mu.Unlock()

	names := []string{"public", "private"}
	for i, srv := range servers {
		go func(name string, srv *http.Server) {
			s.log.Info(ctx, "http listener started",
				logger.F("engine", name),
				logger.F("addr", srv.Addr),
			)
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				failures <- fmt.Errorf("%s listener failed: %w", name, err)
			}
		}(names[i], srv)
	}

	return failures
}

// Stop shuts both listeners down, letting in-flight requests finish.
func (s *HTTPServer) Stop(ctx context.Context) error {
	s.mu.Lock()
	servers := s.servers
	s.mu.Unlock()

	shutdownCtx, cancel := context.WithTimeout(ctx, s.cfg.Server.ShutdownTimeout)
	defer cancel()

	var firstErr error
	for _, srv := range servers {
		if err := srv.Shutdown(shutdownCtx); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	s.log.Info(ctx, "http listeners stopped")
	return firstErr
}

// newServer builds one http.Server with sane timeouts.
//
// The timeouts are explicit because http.Server's zero value has none, and a
// server with no read timeout will happily hold a connection open forever for a
// client that never finishes sending.
func (s *HTTPServer) newServer(port int, engine *gin.Engine) *http.Server {
	return &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           engine,
		ReadTimeout:       s.cfg.Server.ReadTimeout,
		WriteTimeout:      s.cfg.Server.WriteTimeout,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

// registerPrivateRoutes wires metrics, health and the dashboard.
func (s *HTTPServer) registerPrivateRoutes() {
	engine := s.privateEngine

	engine.GET("/health", s.handlers.SupportHandler.Health)
	engine.GET("/ready", s.handlers.SupportHandler.Ready)
	engine.GET("/metrics", gin.WrapH(promInfra.NewHandler(s.registry)))

	engine.GET(DashboardPath, s.handlers.DashboardHandler.Index)
	engine.GET("/", func(ctx *gin.Context) {
		ctx.Redirect(http.StatusFound, DashboardPath)
	})

	// The dashboard is a browser page served from the private port, and it calls
	// the same /api/v1 routes as any other client. Mounting them here too means
	// the UI works without cross-origin configuration and without a second
	// hostname, while the public port still exposes them independently.
	s.registerAPIRoutes(engine)
}

// registerPublicRoutes wires the client-facing API.
func (s *HTTPServer) registerPublicRoutes() {
	engine := s.publicEngine

	engine.GET("/health", s.handlers.SupportHandler.Health)
	s.registerAPIRoutes(engine)
}
