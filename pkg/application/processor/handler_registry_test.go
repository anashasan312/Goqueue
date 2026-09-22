package processor_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anashasan/goqueue/pkg/application/processor"
	"github.com/anashasan/goqueue/pkg/application/services"
	jobVO "github.com/anashasan/goqueue/pkg/domain/job_aggregate/value_objects"
)

func noopHandler() services.HandlerFunc {
	return func(context.Context, services.JobContext) error { return nil }
}

func TestRegistry_ResolvesARegisteredHandler(t *testing.T) {
	registry := processor.NewHandlerRegistry()
	require.NoError(t, registry.Register("email.send", noopHandler()))

	handler, err := registry.Resolve("email.send")

	require.NoError(t, err)
	assert.NotNil(t, handler)
}

func TestRegistry_RejectsADuplicateRegistration(t *testing.T) {
	registry := processor.NewHandlerRegistry()
	require.NoError(t, registry.Register("email.send", noopHandler()))

	// Overwriting silently would mean jobs stop running the code someone thinks
	// they run — far harder to diagnose than a startup failure.
	err := registry.Register("email.send", noopHandler())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate_handler")
}

func TestRegistry_RejectsAnEmptyTaskTypeOrNilHandler(t *testing.T) {
	registry := processor.NewHandlerRegistry()

	require.Error(t, registry.Register("", noopHandler()))
	require.Error(t, registry.Register("email.send", nil))
}

func TestRegistry_ReportsAnUnknownTaskType(t *testing.T) {
	registry := processor.NewHandlerRegistry()

	_, err := registry.Resolve("never.registered")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "handler_not_found")
}

func TestRegistry_ListsRegisteredTypesInSortedOrder(t *testing.T) {
	registry := processor.NewHandlerRegistry()
	require.NoError(t, registry.Register("zeta.task", noopHandler()))
	require.NoError(t, registry.Register("alpha.task", noopHandler()))

	assert.Equal(t,
		[]jobVO.TaskType{"alpha.task", "zeta.task"},
		registry.RegisteredTypes(),
	)
}

func TestRegistry_IsSafeForConcurrentUse(t *testing.T) {
	registry := processor.NewHandlerRegistry()
	require.NoError(t, registry.Register("email.send", noopHandler()))

	// Run under -race: every worker goroutine resolves against this map on every
	// job, so an unguarded read would be a live data race in production.
	done := make(chan struct{})
	for i := 0; i < 16; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 100; j++ {
				_, _ = registry.Resolve("email.send")
				_ = registry.RegisteredTypes()
			}
		}()
	}
	for i := 0; i < 16; i++ {
		<-done
	}
}
