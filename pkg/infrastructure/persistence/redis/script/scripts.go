// Package script embeds the Lua sources that give the broker its atomicity.
//
// The scripts live in .lua files rather than Go string literals so they keep
// syntax highlighting, so a reviewer can read the queueing logic without wading
// through escaping, and so the comments explaining each invariant sit next to
// the code they describe.
package script

import (
	_ "embed"

	"github.com/redis/go-redis/v9"
)

//go:embed enqueue.lua
var enqueueSrc string

//go:embed dequeue.lua
var dequeueSrc string

//go:embed transition.lua
var transitionSrc string

//go:embed promote.lua
var promoteSrc string

//go:embed reclaim.lua
var reclaimSrc string

//go:embed extend_lease.lua
var extendLeaseSrc string

//go:embed purge_completed.lua
var purgeCompletedSrc string

// Compiled scripts. go-redis caches each script's SHA after the first EVAL and
// uses EVALSHA thereafter, so the source crosses the wire once per connection
// pool rather than once per job.
var (
	// Enqueue writes a job document and inserts it into a lifecycle set.
	Enqueue = redis.NewScript(enqueueSrc)
	// Dequeue claims one job for a worker across a weighted queue order.
	Dequeue = redis.NewScript(dequeueSrc)
	// Transition moves a job between two lifecycle sets.
	Transition = redis.NewScript(transitionSrc)
	// Promote moves due scheduled or retrying jobs into pending.
	Promote = redis.NewScript(promoteSrc)
	// Reclaim returns jobs with lapsed leases to pending.
	Reclaim = redis.NewScript(reclaimSrc)
	// ExtendLease renews an active job's lease.
	ExtendLease = redis.NewScript(extendLeaseSrc)
	// PurgeCompleted trims expired entries from the completed set.
	PurgeCompleted = redis.NewScript(purgeCompletedSrc)
)

// All lists every script so that di can preload them into the Redis script
// cache at startup, turning the first job of the process into an EVALSHA like
// every other one.
func All() []*redis.Script {
	return []*redis.Script{
		Enqueue, Dequeue, Transition, Promote, Reclaim, ExtendLease, PurgeCompleted,
	}
}
