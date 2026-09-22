package prometheus

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// NewRegistry builds a dedicated Prometheus registry with the Go runtime and
// process collectors attached.
//
// A dedicated registry rather than the package-global default keeps this
// process's metrics self-contained: nothing a library imports can quietly add a
// collector, and tests can build a registry per case without cleanup.
func NewRegistry() *prometheus.Registry {
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return registry
}

// NewHandler builds the HTTP handler that serves the metrics endpoint.
func NewHandler(registry *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{
		// A scrape must never be able to take the process down, so a collector
		// error is reported in the response body rather than by panicking.
		ErrorHandling: promhttp.ContinueOnError,
		Registry:      registry,
	})
}
