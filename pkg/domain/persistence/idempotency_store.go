package persistence

import (
	"context"
	"time"

	jobVO "github.com/anashasan/goqueue/pkg/domain/job_aggregate/value_objects"
)

// IIdempotencyStore guarantees that a given idempotency key produces at most one
// job while the key is held.
//
// It is a port of its own rather than a method on IJobRepo because deduplication
// is an independent concern with its own lifetime: the claim expires on a TTL
// while the job it protected may live on forever in the completed set.
type IIdempotencyStore interface {
	// Claim atomically reserves a key for a job.
	//
	// It returns (true, "", nil) when the key was free and is now held by jobID.
	// It returns (false, existingJobID, nil) when the key was already claimed,
	// so the caller can return the original job instead of creating a duplicate.
	Claim(
		ctx context.Context,
		key jobVO.IdempotencyKey,
		jobID jobVO.JobID,
		ttl time.Duration,
	) (claimed bool, existingJobID jobVO.JobID, err error)

	// Release drops a claim, allowing the key to be reused immediately. The
	// enqueue path calls it to roll back when job persistence fails after the
	// claim succeeded.
	Release(ctx context.Context, key jobVO.IdempotencyKey) error
}
