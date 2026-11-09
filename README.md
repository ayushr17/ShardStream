# ShardStream

[![CI](https://github.com/ayushr17/ShardStream/actions/workflows/ci.yml/badge.svg)](https://github.com/ayushr17/ShardStream/actions/workflows/ci.yml)

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

## Testing, CI and load testing

```bash
go test -race ./...          # unit tests: hash ring, queue backpressure, event validation
```

GitHub Actions ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs
`gofmt`, `go vet`, `go build`, `go test -race`, and a Docker image build on every
push and pull request.

A [k6 load test](loadtest/README.md) is included. On a single shared vCPU the
pipeline sustained ~2,000 events/sec at p99 under 40 ms with zero errors, and
under a simulated Redis outage it shed load with immediate `503`s (p99 21.5 ms)
rather than hanging, then recovered without a restart. Full results and the
reproduction steps are in [`loadtest/README.md`](loadtest/README.md).
