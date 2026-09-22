package redis

import (
	"context"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/anashasan/goqueue/pkg/common/errors"
	jobVO "github.com/anashasan/goqueue/pkg/domain/job_aggregate/value_objects"
	"github.com/anashasan/goqueue/pkg/domain/persistence"
)

var _ persistence.IIdempotencyStore = (*IdempotencyStore)(nil)

// IdempotencyStore implements deduplication on Redis SET NX.
//
// SET NX is the whole mechanism, and it is enough: it is a single-round-trip
// compare-and-set that Redis executes atomically, so two concurrent enqueues
// carrying the same key cannot both win. The loser learns the winner's job id
// from the same call and returns that job to its caller, which is what makes a
// retried HTTP request return the original job instead of a duplicate.
type IdempotencyStore struct {
	client goredis.UniversalClient
	keys   *KeyBuilder
}

// NewIdempotencyStore builds an IdempotencyStore.
func NewIdempotencyStore(client goredis.UniversalClient, keys *KeyBuilder) *IdempotencyStore {
	return &IdempotencyStore{client: client, keys: keys}
}

// Claim reserves a key for a job.
func (s *IdempotencyStore) Claim(
	ctx context.Context,
	key jobVO.IdempotencyKey,
	jobID jobVO.JobID,
	ttl time.Duration,
) (bool, jobVO.JobID, error) {
	if key.IsZero() {
		// No key means the caller opted out of deduplication; treat it as an
		// uncontested claim so the enqueue path has no special case.
		return true, "", nil
	}

	redisKey := s.keys.Idempotency(key)

	claimed, err := s.client.SetNX(ctx, redisKey, jobID.String(), ttl).Result()
	if err != nil {
		return false, "", errors.Internal(
			"idempotency_claim_failed", "failed to claim idempotency key", err,
		)
	}
	if claimed {
		return true, "", nil
	}

	existing, err := s.client.Get(ctx, redisKey).Result()
	if err != nil {
		// The claim expired between SETNX and GET. Report the key as taken
		// rather than retrying: the caller surfaces a conflict, and the next
		// attempt will find the key free. Silently looping here would make an
		// expiring key into an unbounded retry.
		if err == goredis.Nil {
			return false, "", nil
		}
		return false, "", errors.Internal(
			"idempotency_read_failed", "failed to read idempotency claim", err,
		)
	}

	existingID, err := jobVO.NewJobID(existing)
	if err != nil {
		return false, "", nil
	}
	return false, existingID, nil
}

// Release drops a claim so the key can be reused immediately.
func (s *IdempotencyStore) Release(ctx context.Context, key jobVO.IdempotencyKey) error {
	if key.IsZero() {
		return nil
	}
	if err := s.client.Del(ctx, s.keys.Idempotency(key)).Err(); err != nil {
		return errors.Internal(
			"idempotency_release_failed", "failed to release idempotency key", err,
		)
	}
	return nil
}
