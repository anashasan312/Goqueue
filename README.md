# GoQueue

A distributed task queue in Go, backed by Redis — delayed and scheduled jobs,
priority queues, configurable worker pools, and the reliability machinery that
makes at-least-once delivery actually hold: retries with exponential backoff, a
dead-letter queue, idempotency keys, and graceful shutdown that finishes
in-flight work.

Four Prometheus metrics, a provisioned Grafana dashboard, and an embedded web
dashboard for inspecting and retrying jobs.

Built on **DDD + Clean Architecture** with **Google Wire** for dependency
injection.

---

## Quickstart

### The whole stack in one command

```bash
docker compose -f deployment/docker-compose.yml up --build
./scripts/seed.sh
```

| | |
|---|---|
| Operator dashboard | http://localhost:9090/dashboard |
| Job API | http://localhost:8080/api/v1 |
| Metrics | http://localhost:9090/metrics |
| Prometheus | http://localhost:9091 |
| Grafana | http://localhost:3000 (admin / admin) |

### From source

```bash
make deps        # resolve dependencies and write go.sum — run this first
make env-up      # start Redis on 127.0.0.1:6379
make run         # start the service
make seed        # enqueue a spread of demo jobs
```

`make help` lists every target.

---

## Why it is built this way

### Dependencies point inwards

```
cmd/                      process lifecycle, HTTP server, routes
 └── pkg/
     ├── api/             handlers and middleware — HTTP in, HTTP out
     ├── application/     services — orchestration, no business rules
     ├── domain/          aggregates, value objects, ports  ← nothing above depends on
     ├── contracts/       request/response DTOs
     ├── hydrator/        domain → contract mapping
     ├── infrastructure/  Redis, Prometheus, worker pool, schedulers
     └── di/              the composition root (Wire)
```

The domain imports nothing from the layers around it. It does not know Redis
exists, does not know Gin exists, and has never heard of Prometheus. It declares
the interfaces it needs — `IJobRepo`, `IJobBroker`, `IQueueRepo`,
`IIdempotencyStore`, `metrics.Recorder` — and `pkg/di` is the single file that
decides which concrete type satisfies each one.

The practical payoff is `pkg/domain/persistence/fake`: a complete in-memory
implementation of every port. The enqueuer, the processor and the worker pool
are all tested against it, with no Redis, no Docker and no network. Swapping
Redis for Postgres would change `pkg/infrastructure` and `pkg/di`, and nothing
else.

### Business rules live on the aggregate, not in services

`Job` is the consistency boundary. Its fields are unexported and every lifecycle
change goes through a method that enforces an invariant:

```go
outcome, err := job.Fail(cause, now)   // the JOB decides: retry, or dead-letter?
if outcome.ShouldRetry {
    broker.Retry(ctx, job, outcome.RetryAt)
} else {
    broker.Kill(ctx, job)
}
```

There is no retry arithmetic in `JobProcessorService`. It asks the job what
happened and reacts. "Is this job out of retries" is an invariant of the job, so
it is answered in one place that can be unit-tested without a broker — and two
callers cannot drift apart on it.

The legal transitions are a table in `value_objects/job_state.go`. Nothing can
move a job from `completed` back to `pending`, because the type refuses.

### SOLID, concretely

**Single responsibility.** The three pieces of job execution are separate types
with separate reasons to change: `worker.Pool` owns concurrency and shutdown;
`JobProcessorService` owns what one execution means; `JobBroker` owns atomic
movement between Redis sets. That split is why graceful shutdown is testable
without a Redis and retry policy is testable without a goroutine.

**Open/closed.** Two real extension points. A new task type is a call to
`registry.Register(...)` — the pool, the processor and the routes are untouched.
A new backoff curve is a type implementing `BackoffStrategy` — `RetryPolicy`
does not change.

**Liskov.** The fakes carry compile-time assertions that they satisfy the same
ports as the Redis types, and the fake `Dequeue` holds a mutex for exactly as
long as the Lua script holds Redis — so a concurrency test written against the
fake means something about the real broker.

**Interface segregation.** `IEnqueuerService` and `IInspectorService` are
separate because a producer that only submits work should not be handed a
dependency that can also delete jobs and pause queues. Configuration is the same
idea: `GetWorkerPoolConfig(cfg)` rather than passing `*AppConfig` everywhere, so
the worker pool does not depend on the Redis section.

**Dependency inversion.** Interfaces are declared by the consumer, in
`pkg/domain/persistence`, and implemented in `pkg/infrastructure`. `pkg/di` is
the only place both are named together.

---

## How the queue works

### Redis data model

| Key | Type | Purpose |
|---|---|---|
| `{ns}:job:{id}` | hash | the job document |
| `{ns}:queue:{q}:pending` | zset | eligible for dequeue, scored by priority then age |
| `{ns}:queue:{q}:active` | zset | leased, scored by lease expiry |
| `{ns}:queue:{q}:scheduled` | zset | future jobs, scored by process-at |
| `{ns}:queue:{q}:retrying` | zset | failed jobs, scored by next attempt |
| `{ns}:queue:{q}:completed` | zset | succeeded, scored by retention deadline |
| `{ns}:queue:{q}:dead` | zset | dead-letter, scored by time of death |
| `{ns}:queues` | set | the queue registry |
| `{ns}:idempotency:{key}` | string | deduplication claim, with TTL |

A pending job's score is `(MaxPriority − priority) × 10¹⁴ + enqueuedAtMs`, so
`ZPOPMIN` returns the highest priority first and, within one priority, the oldest
first. The band is `10¹⁴`: wide enough that millisecond timestamps cannot bleed
into the next band for thousands of years, small enough that the largest score
stays inside the range where float64 arithmetic in Lua is exact.

### Atomicity

Seven Lua scripts in `pkg/infrastructure/persistence/redis/script/` do the work
that must be indivisible. `dequeue.lua` is the important one: `ZPOPMIN` and the
move into the active set happen in a single Redis execution, which is what makes
it impossible for two workers to claim the same job. Everything else about job
processing can be retried safely; that step cannot, so it is the one that gets a
script.

`test/integration` proves it with twenty goroutines racing for a hundred jobs
against a real Redis, and fifty goroutines racing for one idempotency key.

### At-least-once delivery

A dequeued job carries a lease — its timeout plus a 30-second grace margin —
written by the same script that claimed it, so a worker that dies between the
claim and its own bookkeeping still leaves a reclaimable job behind. The reclaim
sweep returns jobs with lapsed leases to `pending`. The failed attempt stays
counted, so an orphaned job does not get an unlimited retry budget, and
`last_error` records that it was orphaned rather than that it failed on its own
merits.

The consequence worth stating plainly: **handlers must be idempotent.** A job can
run more than once — that is the trade this design makes on purpose, because the
alternative (at-most-once) silently drops work when a worker dies.

### Graceful shutdown

On `SIGTERM`, in order: HTTP listeners stop accepting, then the worker pool
drains, then the schedulers stop. Reversing that would let the API keep enqueuing
into a queue nobody is draining.

The pool's cancellation breaks the *dequeue* loop, but a job already running
holds a context derived from its own timeout, not from the pool — so it runs to
completion. Jobs still running when `shutdown_timeout` elapses are abandoned;
their leases lapse and the reclaim sweep requeues them. A slow shutdown is a
latency event, not data loss.

Verified end to end: `TestPool_StopWaitsForInFlightJobs`.

### Priority vs. weight

Two different fairness knobs, deliberately kept separate:

- **Priority** orders jobs *within* a queue. Critical goes before normal.
- **Weight** shares worker attention *across* queues. A queue with weight 6 is
  polled six times as often as one with weight 1.

Together they express "critical work goes first, but the low-traffic queue never
starves" — which neither knob can say on its own.

---

## API

| Method | Path | |
|---|---|---|
| `POST` | `/api/v1/jobs` | enqueue, immediately or scheduled |
| `GET` | `/api/v1/jobs` | list, filtered and paginated |
| `GET` | `/api/v1/jobs/:job_id` | fetch one job with its full attempt trail |
| `POST` | `/api/v1/jobs/:job_id/retry` | requeue a dead job with a fresh budget |
| `POST` | `/api/v1/jobs/:job_id/kill` | move a job to the dead-letter queue |
| `DELETE` | `/api/v1/jobs/:job_id` | remove a job |
| `GET` | `/api/v1/queues` | depth snapshot of every queue |
| `PATCH` | `/api/v1/queues/:queue_name/pause` | suspend or resume consumption |
| `POST` | `/api/v1/queues/:queue_name/dead/retry` | requeue every dead job |

Health, readiness, metrics and the dashboard live on the **private** port (9090),
separate from the client-facing API (8080), so a deployment can expose one
through an ingress and keep the other internal.

### Enqueue

```bash
curl -X POST localhost:8080/api/v1/jobs -H 'Content-Type: application/json' -d '{
  "task_type": "email.send",
  "queue": "critical",
  "priority": "critical",
  "payload": {"to": "user@example.com", "subject": "Welcome"},
  "max_retries": 5,
  "retry_base_delay_seconds": 2,
  "backoff_strategy": "exponential",
  "timeout_seconds": 30,
  "idempotency_key": "welcome-user-42"
}'
```

Optional: `delay_seconds` or `run_at` (RFC3339) to schedule — supplying both is
rejected rather than silently resolved.

Repeating a request with the same `idempotency_key` returns `200` with the
**original** job and `"deduplicated": true`, rather than `409`. That is what makes
the endpoint safe for a client to retry after a timeout.

### Adding a task handler

```go
registry.Register("invoice.generate", services.HandlerFunc(
    func(ctx context.Context, job services.JobContext) error {
        var p InvoicePayload
        if err := json.Unmarshal(job.Payload, &p); err != nil {
            return err  // the job's retry policy decides what happens next
        }
        return generate(ctx, p)  // honour ctx: it carries the job timeout
    },
))
```

Returning `nil` completes the job; returning an error fails the attempt. A panic
is caught and treated as a failure — one bad handler must not take the pool down.
`pkg/tasks` has four worked examples, including a deliberately flaky one that
exercises the retry and dead-letter paths.

---

## Observability

Four metrics carry the operational story; everything on the Grafana dashboard is
derived from them.

| Metric | Type | Labels |
|---|---|---|
| `goqueue_jobs_processed_total` | counter | `queue`, `task_type`, `status` |
| `goqueue_job_processing_duration_seconds` | histogram | `queue`, `task_type` |
| `goqueue_job_retries_total` | counter | `queue`, `task_type` |
| `goqueue_jobs_failed_total` | counter | `queue`, `task_type`, `reason` |

Plus `goqueue_queue_depth{queue,state}` and `goqueue_active_workers`.

Every label has bounded cardinality by construction. Queue names and task types
come from a registry, states from a closed enum, and failure reasons from the
error-code constants — a raw error message never becomes a label value, because
handler errors embed ids and a label with unbounded cardinality will take the
Prometheus server down long before it tells anyone anything.

Latency buckets span 5 ms to 60 s rather than using the client defaults, which
top out at 10 s and would collapse every slow job into `+Inf`.

The Grafana dashboard is provisioned automatically and shows throughput, success
rate, p50/p95/p99 latency, retry rate, failures by reason, backlog by state, and
active workers.

---

## Configuration

`config/config.local.yaml` documents every setting; environment variables
prefixed `GOQUEUE_` override the file. Secrets only ever come from the
environment.

```bash
GOQUEUE_REDIS_ADDRS=redis-1:6379,redis-2:6379
GOQUEUE_REDIS_PASSWORD=...
GOQUEUE_WORKER_ENABLED=false     # API-only pod
GOQUEUE_SCHEDULER_ENABLED=false  # worker-only pod
GOQUEUE_WORKER_CONCURRENCY=50
```

One binary serves every role. A cluster can run API-only pods behind a load
balancer and worker-only pods on a different node pool, from the same image, with
no build-time variants to keep in sync. Exactly one deployment should run the
scheduler.

Configuration is validated at startup, so a bad value fails at boot rather than
at 3am when the first job of that shape arrives.

---

## Testing

```bash
make test              # unit tests, race detector, no dependencies
make test-integration  # against a live Redis
make cover             # coverage report
make validate          # fmt, tidy, wire, vet, test
```

The unit suite needs nothing installed — that is the point of the fakes, and it
is why people actually run it. What genuinely cannot be faked (Lua atomicity,
score ordering, TTL behaviour) lives behind the `integration` build tag.

Worth reading as documentation of intent:

- `TestRedis_ConcurrentDequeueNeverDuplicatesAJob` — 20 goroutines, 100 jobs, one claim each
- `TestRedis_IdempotencyKeyAdmitsExactlyOneClaim` — 50 goroutines, one winner, 49 told who won
- `TestRedis_ExpiredLeaseIsReclaimed` — a worker dies mid-job and the job comes back
- `TestPool_StopWaitsForInFlightJobs` — shutdown blocks until running handlers finish
- `TestProcess_ContainsAPanickingHandler` — a panicking handler becomes an ordinary failure

---

## Design decisions, and what they cost

**Polling, not blocking pops.** The pending set is a sorted set, so priority
ordering is possible — but `ZPOPMIN` cannot block, so workers poll on
`poll_interval` (500 ms by default). The cost is up to one interval of pickup
latency on an idle queue; the gain is priority ordering, which a blocking `BLPOP`
on a list cannot give.

**Bulk sweeps bypass the aggregate.** `promote.lua` and `reclaim.lua` perform, in
one script, the transition that `Job.Promote()` and `Job.ReclaimExpiredLease()`
express per job. Running the domain method per job would cost two round trips
each and turn a routine sweep into the bottleneck. The aggregate methods remain
the specification and are unit-tested; the scripts are their batch form. This is
a deliberate trade, not an oversight.

**Task type is not indexed.** Filtering a listing by task type happens in memory
over a bounded page. A dedicated index would be the answer if it became a hot
query; for a dashboard filter it is not worth the write amplification.

**Base64 payloads.** Costs a third more bytes, buys a job document that stays
readable in `redis-cli` during an incident.

**At-least-once, not exactly-once.** Exactly-once across a network and a handler
that has side effects is not something a queue can offer. GoQueue gives
at-least-once delivery plus idempotency keys at the enqueue boundary, and asks
handlers to be idempotent at the execution boundary.

---

## Project layout

```
cmd/
  main.go                     process lifecycle, signal handling, ordered shutdown
  server/                     HTTP engines and route registration
pkg/
  domain/
    job_aggregate/            Job root, RetryPolicy, BackoffStrategy, value objects
    queue_aggregate/          Queue root, Stats read model, weighted dequeue order
    persistence/              the ports (+ fake/, an in-memory implementation)
    metrics/                  the observability port
  application/
    services/                 service interfaces — what each caller is allowed to do
    enqueuer/                 request → validated, deduplicated, queued job
    inspector/                the dashboard and operator actions
    processor/                one execution: resolve, run, record. Plus the registry
  infrastructure/
    persistence/redis/        key layout, Lua scripts, repositories, broker
    metrics/prometheus/       the four metrics
    worker/                   the pool: concurrency and graceful shutdown
    scheduler/                promote, reclaim, janitor, depth sampling
    web/                      the embedded dashboard
  api/handlers, api/middleware
  contracts/, hydrator/, common/, di/, tasks/
deployment/                   Dockerfile, compose, Prometheus, Grafana
test/integration/             live-Redis tests
```

---

## License

MIT
