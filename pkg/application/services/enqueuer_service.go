// Package services declares the application-layer interfaces.
//
// Every service interface lives here, separate from its implementation package,
// for two reasons. First, handlers depend on this package alone, so a handler
// cannot accidentally reach into a service's internals. Second, the interfaces
// are split by *caller need* rather than by implementation: the HTTP API needs
// enqueueing and inspection, the worker pool needs processing, and neither is
// forced to depend on methods it never calls.
package services

import (
	"context"

	jobContr "github.com/anashasan/goqueue/pkg/contracts/job"
)

// IEnqueuerService is the write side of the job API: it turns a client request
// into a persisted, queued job.
type IEnqueuerService interface {
	// Enqueue validates a request, applies idempotency, and places the job on
	// its queue — immediately, or at its scheduled instant.
	Enqueue(ctx context.Context, req jobContr.EnqueueJobReq) (*jobContr.EnqueueJobRes, error)
}
