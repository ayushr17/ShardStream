// ShardStream — single-binary build.
//
// Instead of separate ingestor / worker / api processes talking over HTTP,
// this binary runs all three roles in one process:
//   - an HTTP handler receives events and hashes source_id to a *logical*
//     shard name via the consistent hash ring
//   - each logical shard has its own buffered channel and its own pool of
//     goroutines draining it — this is what stands in for a separate
//     worker process, and is where the concurrency actually happens
//   - a second HTTP handler reads aggregated counts straight out of Redis
//
// Redis is still the one piece of real shared state, used for two
// different things: rate limiting per (shard, source) and durable
// aggregated counters that the metrics API reads.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/yourname/shardstream/internal/event"
	"github.com/yourname/shardstream/internal/hashring"
	"github.com/yourname/shardstream/internal/metrics"
	"github.com/yourname/shardstream/internal/queue"
	"github.com/yourname/shardstream/internal/ratelimit"
)

const (
	shardCount      = 3 // matches the write-up's shard-0/1/2
	workersPerShard = 4 // goroutines draining each shard's channel
	queueBufferSize = 1000
	ringReplicas    = 100 // virtual nodes per shard, for even distribution
	rateLimitPerSec = 100 // events/sec allowed per (shard, source)
)

func shardNames(n int) []string {
	names := make([]string, n)
	for i := 0; i < n; i++ {
		names[i] = fmt.Sprintf("shard-%d", i)
	}
	return names
}

func main() {
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("cannot connect to redis at %s: %v", redisAddr, err)
	}

	shards := shardNames(shardCount)

	ring := hashring.New(ringReplicas)
	for _, s := range shards {
		ring.AddNode(s)
	}

	limiter := ratelimit.New(rdb)
	agg := metrics.New(rdb)
	qm := queue.NewManager(shards, queueBufferSize)

	// Each shard gets its own pool of goroutines draining its own channel,
	// independently of every other shard — this is the concurrent
	// stream-processing piece the write-up calls out explicitly.
	for _, shardName := range shards {
		ch := qm.Channel(shardName)
		for w := 0; w < workersPerShard; w++ {
			go runWorker(shardName, ch, limiter, agg)
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ingest", ingestHandler(ring, qm))
	mux.HandleFunc("/sources/", metricsHandler(agg))

	addr := ":8080"
	log.Printf("shardstream listening on %s — %d shards x %d workers, rate limit %d/sec/source", addr, shardCount, workersPerShard, rateLimitPerSec)
	log.Fatal(http.ListenAndServe(addr, mux))
}

// runWorker drains one shard's queue, rate-limiting and recording metrics
// for every event that comes through. In the multi-process version of this
// project this loop would be the entire body of a separate worker binary;
// here it's a goroutine instead, but it does exactly the same two things
// per event: check the limiter, then record the metric.
func runWorker(shardName string, ch <-chan event.Event, limiter *ratelimit.Limiter, agg *metrics.Aggregator) {
	ctx := context.Background()
	for e := range ch {
		rlKey := fmt.Sprintf("ratelimit:%s:%s", shardName, e.SourceID)
		ok, err := limiter.Allow(ctx, rlKey, rateLimitPerSec, time.Second)
		if err != nil {
			log.Printf("[%s] rate limiter error for source=%s: %v", shardName, e.SourceID, err)
			continue
		}
		if !ok {
			log.Printf("[%s] rate limit exceeded, dropping event: source=%s type=%s", shardName, e.SourceID, e.EventType)
			continue
		}

		if err := agg.Record(ctx, e.SourceID, e.EventType); err != nil {
			log.Printf("[%s] failed to record metric for source=%s type=%s: %v", shardName, e.SourceID, e.EventType, err)
		}
	}
}

// ingestHandler is the single public entrypoint. It hashes source_id to
// find which shard owns it, then hands the event to that shard's queue.
func ingestHandler(ring *hashring.Ring, qm *queue.Manager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var e event.Event
		if err := json.NewDecoder(r.Body).Decode(&e); err != nil {
			http.Error(w, "bad request: invalid JSON", http.StatusBadRequest)
			return
		}
		if e.Timestamp.IsZero() {
			e.Timestamp = time.Now().UTC()
		}
		if err := e.Validate(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		shard := ring.GetNode(e.SourceID)
		if shard == "" {
			http.Error(w, "no shards available", http.StatusInternalServerError)
			return
		}

		if !qm.Enqueue(shard, e) {
			// The shard's buffer is full — fail fast rather than block the
			// request indefinitely (see "Backpressure" in the write-up's
			// extension ideas).
			http.Error(w, "shard queue full, try again shortly", http.StatusServiceUnavailable)
			return
		}

		w.Header().Set("X-Shard", shard)
		w.WriteHeader(http.StatusAccepted)
	}
}

// metricsHandler serves GET /sources/{id}/metrics, reading aggregated
// counts straight out of Redis — the API never has to ask a specific
// shard/worker for data.
func metricsHandler(agg *metrics.Aggregator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		path := strings.TrimPrefix(r.URL.Path, "/sources/")
		path = strings.TrimSuffix(path, "/")
		sourceID, suffix, found := strings.Cut(path, "/")
		if !found || suffix != "metrics" || sourceID == "" {
			http.Error(w, "not found: expected /sources/{id}/metrics", http.StatusNotFound)
			return
		}

		counts, err := agg.GetAll(r.Context(), sourceID)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"source_id": sourceID,
			"counts":    counts,
		})
	}
}
