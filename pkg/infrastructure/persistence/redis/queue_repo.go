package redis

import (
	"context"
	"strconv"

	goredis "github.com/redis/go-redis/v9"

	"github.com/anashasan/goqueue/pkg/common/errors"
	jobVO "github.com/anashasan/goqueue/pkg/domain/job_aggregate/value_objects"
	"github.com/anashasan/goqueue/pkg/domain/persistence"
	queueAgg "github.com/anashasan/goqueue/pkg/domain/queue_aggregate"
	queueErr "github.com/anashasan/goqueue/pkg/domain/queue_aggregate/error"
	queueVO "github.com/anashasan/goqueue/pkg/domain/queue_aggregate/value_objects"
)

var _ persistence.IQueueRepo = (*QueueRepo)(nil)

// Queue meta hash fields.
const (
	metaFieldWeight = "weight"
	metaFieldPaused = "paused"
)

// QueueRepo implements the queue persistence port on Redis.
type QueueRepo struct {
	client goredis.UniversalClient
	keys   *KeyBuilder
}

// NewQueueRepo builds a QueueRepo.
func NewQueueRepo(client goredis.UniversalClient, keys *KeyBuilder) *QueueRepo {
	return &QueueRepo{client: client, keys: keys}
}

// Register records a queue so it is listed before it has held any job.
//
// The weight is written unconditionally — it comes from configuration, so a
// deployment that changes it should take effect. The paused flag is written
// only if absent, with HSetNX.
//
// That asymmetry is deliberate and load-bearing. Every worker calls Register on
// every start, so writing the paused flag unconditionally would resume a queue
// an operator paused during an incident the moment any pod restarted or scaled
// up — and it would do so silently, at exactly the wrong moment.
func (r *QueueRepo) Register(ctx context.Context, queue *queueAgg.Queue) error {
	metaKey := r.keys.QueueMeta(queue.Name())

	pipe := r.client.TxPipeline()
	pipe.SAdd(ctx, r.keys.QueueRegistry(), queue.Name().String())
	pipe.HSet(ctx, metaKey, metaFieldWeight, strconv.Itoa(queue.Weight().Int()))
	pipe.HSetNX(ctx, metaKey, metaFieldPaused, boolToFlag(queue.IsPaused()))

	if _, err := pipe.Exec(ctx); err != nil {
		return errors.Internal("queue_register_failed", "failed to register queue", err)
	}
	return nil
}

// FindByName returns one queue.
func (r *QueueRepo) FindByName(ctx context.Context, name jobVO.QueueName) (*queueAgg.Queue, error) {
	exists, err := r.client.SIsMember(ctx, r.keys.QueueRegistry(), name.String()).Result()
	if err != nil {
		return nil, errors.Internal("queue_read_failed", "failed to read queue", err)
	}
	if !exists {
		return nil, errors.NotFound(queueErr.EQueueNotFound, "no queue named "+name.String())
	}
	return r.loadMeta(ctx, name)
}

// ListAll returns every registered queue.
func (r *QueueRepo) ListAll(ctx context.Context) ([]*queueAgg.Queue, error) {
	names, err := r.client.SMembers(ctx, r.keys.QueueRegistry()).Result()
	if err != nil {
		return nil, errors.Internal("queue_list_failed", "failed to list queues", err)
	}

	queues := make([]*queueAgg.Queue, 0, len(names))
	for _, raw := range names {
		name, err := jobVO.NewQueueName(raw)
		if err != nil {
			continue
		}
		queue, err := r.loadMeta(ctx, name)
		if err != nil {
			return nil, err
		}
		queues = append(queues, queue)
	}
	return queues, nil
}

// SetPaused suspends or resumes dequeue for a queue.
func (r *QueueRepo) SetPaused(ctx context.Context, name jobVO.QueueName, paused bool) error {
	if err := r.client.HSet(
		ctx, r.keys.QueueMeta(name), metaFieldPaused, boolToFlag(paused),
	).Err(); err != nil {
		return errors.Internal("queue_pause_failed", "failed to update queue pause state", err)
	}
	return nil
}

// Stats returns the per-state depth snapshot for one queue.
func (r *QueueRepo) Stats(ctx context.Context, name jobVO.QueueName) (queueAgg.Stats, error) {
	queue, err := r.loadMeta(ctx, name)
	if err != nil {
		return queueAgg.Stats{}, err
	}
	return r.statsFor(ctx, queue)
}

// StatsAll returns the depth snapshot for every registered queue.
func (r *QueueRepo) StatsAll(ctx context.Context) ([]queueAgg.Stats, error) {
	queues, err := r.ListAll(ctx)
	if err != nil {
		return nil, err
	}

	stats := make([]queueAgg.Stats, 0, len(queues))
	for _, queue := range queues {
		s, err := r.statsFor(ctx, queue)
		if err != nil {
			return nil, err
		}
		stats = append(stats, s)
	}
	queueAgg.SortStatsByName(stats)
	return stats, nil
}

// statsFor counts every lifecycle set of one queue in a single pipeline.
func (r *QueueRepo) statsFor(ctx context.Context, queue *queueAgg.Queue) (queueAgg.Stats, error) {
	name := queue.Name()

	pipe := r.client.Pipeline()
	pending := pipe.ZCard(ctx, r.keys.Pending(name))
	active := pipe.ZCard(ctx, r.keys.Active(name))
	scheduled := pipe.ZCard(ctx, r.keys.Scheduled(name))
	retrying := pipe.ZCard(ctx, r.keys.Retrying(name))
	completed := pipe.ZCard(ctx, r.keys.Completed(name))
	dead := pipe.ZCard(ctx, r.keys.Dead(name))

	if _, err := pipe.Exec(ctx); err != nil && err != goredis.Nil {
		return queueAgg.Stats{}, errors.Internal("queue_stats_failed", "failed to read queue stats", err)
	}

	return queueAgg.Stats{
		Queue:     name,
		Paused:    queue.IsPaused(),
		Weight:    queue.Weight(),
		Pending:   pending.Val(),
		Active:    active.Val(),
		Scheduled: scheduled.Val(),
		Retrying:  retrying.Val(),
		Completed: completed.Val(),
		Dead:      dead.Val(),
	}, nil
}

// loadMeta reads a queue's weight and paused flag, defaulting a queue that has
// no meta hash yet rather than failing: a queue can be created implicitly by an
// enqueue, and a missing meta hash simply means nobody has configured it.
func (r *QueueRepo) loadMeta(ctx context.Context, name jobVO.QueueName) (*queueAgg.Queue, error) {
	meta, err := r.client.HGetAll(ctx, r.keys.QueueMeta(name)).Result()
	if err != nil {
		return nil, errors.Internal("queue_read_failed", "failed to read queue metadata", err)
	}

	weight, err := queueVO.NewWeight(uint8(parseUint(meta[metaFieldWeight], uint64(queueVO.DefaultWeight))))
	if err != nil {
		weight = queueVO.DefaultWeight
	}

	return queueAgg.Reconstitute(name, weight, meta[metaFieldPaused] == "1"), nil
}

// boolToFlag renders a boolean as the "1"/"0" the Lua scripts compare against.
func boolToFlag(v bool) string {
	if v {
		return "1"
	}
	return "0"
}
