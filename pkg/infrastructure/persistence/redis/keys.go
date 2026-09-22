// Package redis implements the domain persistence ports on top of Redis.
//
// Everything Redis-specific — key layout, score arithmetic, Lua scripts, hash
// encoding — is confined to this package. The rest of the codebase talks only to
// the interfaces in pkg/domain/persistence.
package redis

import (
	"fmt"

	jobVO "github.com/anashasan/goqueue/pkg/domain/job_aggregate/value_objects"
)

// DefaultNamespace prefixes every key so that several GoQueue deployments can
// share one Redis instance without colliding.
const DefaultNamespace = "goqueue"

// priorityBand is the score multiplier that separates priority levels inside the
// pending sorted set.
//
// A pending job's score is (MaxPriority-priority)*priorityBand + enqueuedAtMs,
// so ZPOPMIN yields the highest priority first and, within one priority, the
// oldest job first. The band is 1e14: wide enough that millisecond timestamps
// can never bleed into the next band for the next few thousand years, and small
// enough that the largest score (9e14 + now) stays well inside the 2^53 range
// where float64 arithmetic in Lua is exact.
const priorityBand = 1e14

// KeyBuilder renders every Redis key used by GoQueue.
//
// Centralising key construction means the layout is documented in exactly one
// file, and a migration to a new layout touches one type.
type KeyBuilder struct {
	namespace string
}

// NewKeyBuilder builds a KeyBuilder. An empty namespace falls back to the
// default so a misconfigured deployment still produces well-formed keys.
func NewKeyBuilder(namespace string) *KeyBuilder {
	if namespace == "" {
		namespace = DefaultNamespace
	}
	return &KeyBuilder{namespace: namespace}
}

// Namespace returns the configured prefix.
func (k *KeyBuilder) Namespace() string { return k.namespace }

// Job returns the hash key holding one job document.
func (k *KeyBuilder) Job(id jobVO.JobID) string {
	return fmt.Sprintf("%s:job:%s", k.namespace, id)
}

// JobPrefix returns the prefix Lua scripts concatenate with a job id.
func (k *KeyBuilder) JobPrefix() string {
	return fmt.Sprintf("%s:job:", k.namespace)
}

// Pending returns the sorted set of jobs eligible for dequeue.
func (k *KeyBuilder) Pending(queue jobVO.QueueName) string {
	return fmt.Sprintf("%s:queue:%s:pending", k.namespace, queue)
}

// Active returns the sorted set of leased jobs, scored by lease expiry so the
// reclaim sweep is a single range query.
func (k *KeyBuilder) Active(queue jobVO.QueueName) string {
	return fmt.Sprintf("%s:queue:%s:active", k.namespace, queue)
}

// Scheduled returns the sorted set of future jobs, scored by process-at.
func (k *KeyBuilder) Scheduled(queue jobVO.QueueName) string {
	return fmt.Sprintf("%s:queue:%s:scheduled", k.namespace, queue)
}

// Retrying returns the sorted set of failed jobs awaiting their next attempt.
func (k *KeyBuilder) Retrying(queue jobVO.QueueName) string {
	return fmt.Sprintf("%s:queue:%s:retrying", k.namespace, queue)
}

// Completed returns the sorted set of succeeded jobs, scored by retention
// deadline so the janitor can trim it with one range query.
func (k *KeyBuilder) Completed(queue jobVO.QueueName) string {
	return fmt.Sprintf("%s:queue:%s:completed", k.namespace, queue)
}

// Dead returns the dead-letter sorted set, scored by time of death.
func (k *KeyBuilder) Dead(queue jobVO.QueueName) string {
	return fmt.Sprintf("%s:queue:%s:dead", k.namespace, queue)
}

// StateSet returns the sorted set backing a given lifecycle state.
func (k *KeyBuilder) StateSet(queue jobVO.QueueName, state jobVO.JobState) string {
	switch state {
	case jobVO.StatePending:
		return k.Pending(queue)
	case jobVO.StateActive:
		return k.Active(queue)
	case jobVO.StateScheduled:
		return k.Scheduled(queue)
	case jobVO.StateRetrying:
		return k.Retrying(queue)
	case jobVO.StateCompleted:
		return k.Completed(queue)
	case jobVO.StateDead:
		return k.Dead(queue)
	default:
		return ""
	}
}

// QueueRegistry returns the set holding every known queue name.
func (k *KeyBuilder) QueueRegistry() string {
	return fmt.Sprintf("%s:queues", k.namespace)
}

// QueueMeta returns the hash holding a queue's weight and paused flag.
func (k *KeyBuilder) QueueMeta(queue jobVO.QueueName) string {
	return fmt.Sprintf("%s:queue:%s:meta", k.namespace, queue)
}

// Idempotency returns the string key holding a deduplication claim.
func (k *KeyBuilder) Idempotency(key jobVO.IdempotencyKey) string {
	return fmt.Sprintf("%s:idempotency:%s", k.namespace, key)
}

// PendingScore computes the sorted-set score for a pending job.
func PendingScore(priority jobVO.Priority, enqueuedAtMs int64) float64 {
	band := float64(jobVO.MaxPriority-priority) * priorityBand
	return band + float64(enqueuedAtMs)
}
