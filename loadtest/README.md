# Load testing

[`ingest.js`](ingest.js) is a [k6](https://k6.io) script that drives `POST /ingest`
at a fixed arrival rate and reports throughput and latency percentiles.

## Reproduce

```bash
docker compose up -d            # Redis + ShardStream on :8080
k6 run -e RATE=2000 -e DURATION=30s loadtest/ingest.js
```

| Env var    | Default                 | Meaning                                       |
|------------|-------------------------|-----------------------------------------------|
| `BASE_URL` | `http://localhost:8080` | Target                                        |
| `RATE`     | `2000`                  | Requests/sec to sustain                       |
| `DURATION` | `30s`                   | Test length                                   |
| `SOURCES`  | `5000`                  | Distinct `source_id`s to spread load across   |

Many distinct sources are used deliberately: the per-source rate limiter
(100 events/sec/source) would otherwise drop events inside the workers and
hide the pipeline's real throughput.

## Results

**Environment:** 1 vCPU / 4 GB sandbox, Redis 7.2.5, k6 0.54.0. The load
generator, ShardStream, and Redis all shared that single core, so these are
conservative numbers; expect more on a multi-core machine. 3 shards x 4 workers,
queue buffer 1000 per shard. 5,000 distinct sources, 15 s per run.

### Steady state

| Target (req/s) | Achieved (req/s) | p50 (ms) | p95 (ms) | p99 (ms) | 202 | 503 | Failed |
|---------------:|-----------------:|---------:|---------:|---------:|----:|----:|-------:|
| 1,000          | 995              | 0.33     | 5.4      | 25.2     | all | 0   | 0      |
| 2,000          | 1,971            | 0.76     | 14.1     | 36.7     | all | 0   | 0      |
| 4,000          | 2,899            | 107.9    | 215.3    | 278.4    | all | 0   | 0      |

Around 2,000 req/s is comfortable on this hardware. At 4,000 req/s the single
shared core is the bottleneck (latency climbs, throughput plateaus near 2,900
req/s) while the pipeline itself keeps accepting. "Achieved" is slightly below
"target" mostly because k6 skips iterations when it cannot schedule them on the
same core.

### Correctness: no accepted event is lost

After a 2,000 req/s run, every one of the 29,861 events answered `202` was
recorded in Redis (sum of all `metrics:*` counters = 29,861).

### Backpressure: Redis stalled (`SIGSTOP`), 2,000 req/s for 10 s

Freezing Redis blocks the shard workers, so the bounded queues fill up.

| Metric                    | Result                         |
|---------------------------|--------------------------------|
| Accepted (`202`)          | 3,032 (~3 x 1000 queue slots + in-flight) |
| Shed (`503`)              | 16,715 (~85% of requests)      |
| Latency p99 / max         | 21.5 ms / 137 ms               |
| Hung or failed requests   | 0                              |

The ingest endpoint fails fast instead of blocking. After Redis was resumed
(`SIGCONT`) with no restart, a 1,000 req/s run showed 0 `503`s and p99 8.6 ms,
and the queued events were drained.

**Known limitation found by this test:** events a worker has already dequeued
when Redis stalls are dropped after the Redis client timeout (20 events in this
run, exactly matching the 20 `rate limiter error` log lines). Events still
waiting in the queue are not lost. Delivery is therefore at-most-once for
in-flight events during a Redis outage; retrying with backoff or a dead-letter
queue would close that gap.
