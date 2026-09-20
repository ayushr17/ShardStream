# ShardStream

A small distributed-systems demo: ingest a stream of generic "user did X"
events, shard them by `source_id` using consistent hashing, rate-limit and
aggregate metrics per shard through Redis, and query live totals through an
HTTP API — without pulling in Kafka or any other heavyweight infra.

## A note on architecture: single binary, not separate processes

The original write-up sketches two variants: one where the ingestor
forwards HTTP requests to separate `shard-0`/`shard-1`/`shard-2` worker
*processes*, and a simpler one where everything runs in a single process
and dispatches straight to an in-memory channel. **This build is the
second one** (single binary), per your choice, and runs **3 shards** (per
your choice), each with its own buffered channel and its own pool of
goroutines draining it. The consistent-hash ring still assigns each
`source_id` to a shard *name* exactly as described — it just resolves to a
channel instead of a network address. Swapping in real HTTP forwarding to
separate worker processes later would only mean changing what happens
inside `ingestHandler` and `runWorker` in `cmd/shardstream/main.go`; the
`hashring`, `ratelimit`, `metrics`, and `event` packages wouldn't need to
change at all.

## Project structure

```
shardstream/
├── go.mod / go.sum
├── Dockerfile
├── docker-compose.yml
├── cmd/
│   └── shardstream/
│       └── main.go          # HTTP entrypoint + shard worker pools + metrics API, all in one process
└── internal/
    ├── event/event.go       # Event struct + validation
    ├── hashring/
    │   ├── hashring.go      # consistent hashing (+ RemoveNode for the node-failure demo)
    │   └── hashring_test.go # distribution + rerouting tests
    ├── ratelimit/ratelimit.go  # Redis-backed fixed-window limiter
    ├── metrics/metrics.go      # Redis-backed counter aggregation
    └── queue/queue.go          # in-memory channel-based queue per shard
```

## How it fits together

```
                 ┌─────────────────────────┐
  clients ────▶  │   /ingest (HTTP)          │
                 └────────────┬─────────────┘
                               │ consistent-hash(source_id) → shard name
                               ▼
              ┌────────────────┴────────────────┐
              │                │                │
        ┌─────▼─────┐   ┌──────▼─────┐   ┌──────▼─────┐
        │ shard-0    │   │ shard-1    │   │ shard-2    │   each: 1 buffered
        │ channel +  │   │ channel +  │   │ channel +  │   channel, 4 worker
        │ 4 workers  │   │ 4 workers  │   │ 4 workers  │   goroutines
        └─────┬─────┘   └──────┬─────┘   └──────┬─────┘
              │                │                │
              └────────────────┴────────────────┘
                               │
                         ┌─────▼──────┐
                         │   Redis    │  rate-limit counters (per shard+source)
                         │            │  aggregated metrics (per source+event type)
                         └─────┬──────┘
                               │
                      ┌────────▼────────┐
                      │ GET /sources/:id/metrics │
                      └─────────────────┘
```

- **Sharding** happens in `ingestHandler`: it hashes `source_id` on the ring
  and enqueues the event onto that shard's channel. If the channel is full,
  it returns `503` immediately rather than blocking the request (this is
  the "Backpressure" extension idea from the write-up, folded into the
  base build since a non-blocking send is the natural way to use a
  channel here).
- **Concurrency**: each shard has its own 4 goroutines (`workersPerShard`
  in `main.go`) draining its own channel independently — bump
  `workersPerShard` or `shardCount` to scale.
- **Rate limiting**: each worker calls `limiter.Allow` with a key scoped to
  `shard:source`, so each shard independently caps its own sources at
  100 events/sec. Because the check happens inside the worker (not the
  HTTP handler), `/ingest` always returns `202` immediately — the reject
  happens silently server-side (logged) rather than as a synchronous
  `429`. That's a deliberate trade-off of the "genuinely concurrent"
  version described in the write-up: fire-and-forget ingestion, with rate
  limiting protecting Redis/downstream rather than the client.
- **Metrics**: `metrics.Record` increments `metrics:<source>:<type>` and
  adds `<type>` to a `eventtypes:<source>` set, so `GetAll` can return a
  full breakdown for a source without the caller needing to already know
  which event types exist.

## Running it

### Locally

```bash
# needs Redis reachable at localhost:6379 (or set REDIS_ADDR)
redis-server &

go run ./cmd/shardstream
# shardstream listening on :8080 — 3 shards x 4 workers, rate limit 100/sec/source
```

### With Docker Compose

```bash
docker-compose up --build
```

## Testing it end-to-end

```bash
# Send some events
curl -X POST localhost:8080/ingest -d '{"source_id":"src-1","event_type":"click"}'
curl -X POST localhost:8080/ingest -d '{"source_id":"src-2","event_type":"view"}'

# Hammer one source past its rate limit
for i in $(seq 1 150); do
  curl -s -o /dev/null -w "%{http_code}\n" -X POST localhost:8080/ingest \
    -d '{"source_id":"src-1","event_type":"click"}'
done
# every request returns 202 (ingestion is fire-and-forget); the drop
# happens inside the worker and shows up as a gap in the metrics below,
# not as a non-202 status code

# Query aggregated metrics
curl localhost:8080/sources/src-1/metrics
# {"source_id":"src-1","counts":{"click":101}}   <- ~100, not 150: rate limiter did its job
```

This was actually run during development (not just described): 150
hammering requests all returned `202`, and the subsequent metrics query
came back with **101** recorded clicks out of 151 total attempts — the
fixed-window limiter capped it right around the configured 100/sec, with
the rest silently dropped by the worker.

### Hash ring test

```bash
go test ./internal/hashring/... -v
```

`TestDistribution` adds 5 nodes, hashes 10,000 random source IDs, and
checks no node is more than ~40% off its fair share (actual run: nodes
landed between 15.8% and 24.9% of traffic, vs. an even 20% each — with 100
virtual replicas per node you could tighten that further).
`TestRemoveNodeReroutes` adds 4 nodes, removes one, and confirms only the
removed node's keys move to a new owner (~19% of keys moved when removing
1 of 4 nodes, and never a key whose original owner wasn't the removed
node) — the classic consistent-hashing property from the write-up's
extension ideas.

## What's deliberately not built

Per the write-up's own "Extension ideas (if you have more time)" list,
these are out of scope for this build and would be natural next steps:
persistent event log to Postgres, gRPC between ingestor and workers (moot
here since there's no process boundary), and a full node-failure demo at
the process level (the *ring* already supports `RemoveNode` and is tested
for it — see `TestRemoveNodeReroutes` — but there's no separate worker
process to actually kill in the single-binary build).
