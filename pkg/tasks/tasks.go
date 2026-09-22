// Package tasks holds the example handlers this binary registers at startup.
//
// They exist so the project runs end to end out of the box: start it, enqueue a
// job, and watch the dashboard and Grafana fill. In a real deployment this
// package is where an application's own handlers live, and adding one is a
// single Register call — nothing in the queue engine changes.
package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/anashasan/goqueue/pkg/application/services"
	"github.com/anashasan/goqueue/pkg/common/errors"
	"github.com/anashasan/goqueue/pkg/common/logger"
	jobVO "github.com/anashasan/goqueue/pkg/domain/job_aggregate/value_objects"
)

// Task types registered by this package.
const (
	TypeEmailSend      = "email.send"
	TypeReportGenerate = "report.generate"
	TypeFlaky          = "demo.flaky"
	TypeSlow           = "demo.slow"
)

// RegisterAll binds every example handler.
//
// It returns on the first failure rather than continuing, because a registry
// that is half built would leave the process advertising itself as ready for
// work it cannot route.
func RegisterAll(registry services.IHandlerRegistry, log logger.Logger) error {
	handlers := map[string]services.Handler{
		TypeEmailSend:      NewEmailHandler(log),
		TypeReportGenerate: NewReportHandler(log),
		TypeFlaky:          NewFlakyHandler(log, 0.5),
		TypeSlow:           NewSlowHandler(log),
	}

	for rawType, handler := range handlers {
		taskType, err := jobVO.NewTaskType(rawType)
		if err != nil {
			return err
		}
		if err := registry.Register(taskType, handler); err != nil {
			return err
		}
	}
	return nil
}

// EmailPayload is the input to the email handler.
type EmailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

// EmailHandler simulates sending an email.
type EmailHandler struct{ log logger.Logger }

// NewEmailHandler builds an EmailHandler.
func NewEmailHandler(log logger.Logger) *EmailHandler { return &EmailHandler{log: log} }

// Handle sends the email.
func (h *EmailHandler) Handle(ctx context.Context, job services.JobContext) error {
	var payload EmailPayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		// A malformed payload will be malformed on every retry, so it is worth
		// distinguishing in the logs from a transient send failure.
		return errors.Invalid("invalid_payload", "email payload is not valid JSON")
	}
	if payload.To == "" {
		return errors.Invalid("invalid_payload", "email payload requires a 'to' address")
	}

	if err := sleepCtx(ctx, 80*time.Millisecond); err != nil {
		return err
	}

	h.log.Info(ctx, "email sent",
		logger.F("to", payload.To),
		logger.F("subject", payload.Subject),
	)
	return nil
}

// ReportHandler simulates a CPU-bound report build.
type ReportHandler struct{ log logger.Logger }

// NewReportHandler builds a ReportHandler.
func NewReportHandler(log logger.Logger) *ReportHandler { return &ReportHandler{log: log} }

// Handle builds the report.
func (h *ReportHandler) Handle(ctx context.Context, _ services.JobContext) error {
	if err := sleepCtx(ctx, time.Duration(200+rand.Intn(600))*time.Millisecond); err != nil {
		return err
	}
	h.log.Info(ctx, "report generated")
	return nil
}

// FlakyHandler fails a configurable fraction of the time.
//
// It exists to exercise the retry path: with the default policy a flaky job
// climbs the backoff curve and, if it keeps failing, lands in the dead-letter
// queue where the dashboard's retry button can pick it up.
type FlakyHandler struct {
	log         logger.Logger
	failureRate float64
}

// NewFlakyHandler builds a FlakyHandler with a failure probability in [0,1].
func NewFlakyHandler(log logger.Logger, failureRate float64) *FlakyHandler {
	return &FlakyHandler{log: log, failureRate: failureRate}
}

// Handle succeeds or fails at random.
func (h *FlakyHandler) Handle(ctx context.Context, job services.JobContext) error {
	if err := sleepCtx(ctx, 50*time.Millisecond); err != nil {
		return err
	}

	if rand.Float64() < h.failureRate { //nolint:gosec // demo handler, no crypto needed
		return fmt.Errorf("simulated downstream failure on attempt %d", job.Attempt)
	}

	h.log.Info(ctx, "flaky task succeeded")
	return nil
}

// SlowHandler runs longer than a short job timeout, to demonstrate that a job
// which overruns is cancelled and retried rather than holding its worker slot.
type SlowHandler struct{ log logger.Logger }

// NewSlowHandler builds a SlowHandler.
func NewSlowHandler(log logger.Logger) *SlowHandler { return &SlowHandler{log: log} }

// Handle sleeps for five seconds, honouring cancellation.
func (h *SlowHandler) Handle(ctx context.Context, _ services.JobContext) error {
	if err := sleepCtx(ctx, 5*time.Second); err != nil {
		return err
	}
	h.log.Info(ctx, "slow task finished")
	return nil
}

// sleepCtx sleeps for d unless the context is cancelled first.
//
// Every handler uses it rather than time.Sleep, because a handler that ignores
// cancellation cannot be stopped by its timeout and will be abandoned at
// shutdown instead of finishing cleanly.
func sleepCtx(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
