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

	"github.com/ayushr17/ShardStream/internal/event"
	"github.com/ayushr17/ShardStream/internal/hashring"
	"github.com/ayushr17/ShardStream/internal/metrics"
	"github.com/ayushr17/ShardStream/internal/queue"
	"github.com/ayushr17/ShardStream/internal/ratelimit"
)

const (
	shardCount      = 3
	workersPerShard = 4
	queueBufferSize = 1000
	ringReplicas    = 100
	rateLimitPerSec = 100
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

	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("cannot connect to redis at %s: %v", redisAddr, err)
	}

	shards := shardNames(shardCount)

	ring := hashring.New(ringReplicas)
	for _, shard := range shards {
		ring.AddNode(shard)
	}

	limiter := ratelimit.New(rdb)
	aggregator := metrics.New(rdb)
	queueManager := queue.NewManager(shards, queueBufferSize)

	for _, shard := range shards {
		ch := queueManager.Channel(shard)

		for i := 0; i < workersPerShard; i++ {
			go runWorker(shard, ch, limiter, aggregator)
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ingest", ingestHandler(ring, queueManager))
	mux.HandleFunc("/sources/", metricsHandler(aggregator))

	addr := ":8080"

	log.Printf(
		"shardstream listening on %s — %d shards x %d workers, rate limit %d/sec/source",
		addr,
		shardCount,
		workersPerShard,
		rateLimitPerSec,
	)

	log.Fatal(http.ListenAndServe(addr, mux))
}

func runWorker(
	shardName string,
	ch <-chan event.Event,
	limiter *ratelimit.Limiter,
	aggregator *metrics.Aggregator,
) {
	ctx := context.Background()

	for e := range ch {
		rateLimitKey := fmt.Sprintf(
			"ratelimit:%s:%s",
			shardName,
			e.SourceID,
		)

		allowed, err := limiter.Allow(
			ctx,
			rateLimitKey,
			rateLimitPerSec,
			time.Second,
		)

		if err != nil {
			log.Printf(
				"[%s] rate limiter error for source=%s: %v",
				shardName,
				e.SourceID,
				err,
			)
			continue
		}

		if !allowed {
			log.Printf(
				"[%s] rate limit exceeded, dropping event: source=%s type=%s",
				shardName,
				e.SourceID,
				e.EventType,
			)
			continue
		}

		if err := aggregator.Record(
			ctx,
			e.SourceID,
			e.EventType,
		); err != nil {
			log.Printf(
				"[%s] failed to record metric for source=%s type=%s: %v",
				shardName,
				e.SourceID,
				e.EventType,
				err,
			)
		}
	}
}

func ingestHandler(
	ring *hashring.Ring,
	queueManager *queue.Manager,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(
				w,
				"method not allowed",
				http.StatusMethodNotAllowed,
			)
			return
		}

		var e event.Event

		if err := json.NewDecoder(r.Body).Decode(&e); err != nil {
			http.Error(
				w,
				"bad request: invalid JSON",
				http.StatusBadRequest,
			)
			return
		}

		if e.Timestamp.IsZero() {
			e.Timestamp = time.Now().UTC()
		}

		if err := e.Validate(); err != nil {
			http.Error(
				w,
				err.Error(),
				http.StatusBadRequest,
			)
			return
		}

		shard := ring.GetNode(e.SourceID)

		if shard == "" {
			http.Error(
				w,
				"no shards available",
				http.StatusInternalServerError,
			)
			return
		}

		if !queueManager.Enqueue(shard, e) {
			http.Error(
				w,
				"shard queue full, try again shortly",
				http.StatusServiceUnavailable,
			)
			return
		}

		w.Header().Set("X-Shard", shard)
		w.WriteHeader(http.StatusAccepted)
	}
}

func metricsHandler(
	aggregator *metrics.Aggregator,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(
				w,
				"method not allowed",
				http.StatusMethodNotAllowed,
			)
			return
		}

		path := strings.TrimPrefix(
			r.URL.Path,
			"/sources/",
		)

		path = strings.TrimSuffix(path, "/")

		sourceID, suffix, found := strings.Cut(path, "/")

		if !found || suffix != "metrics" || sourceID == "" {
			http.Error(
				w,
				"not found: expected /sources/{id}/metrics",
				http.StatusNotFound,
			)
			return
		}

		counts, err := aggregator.GetAll(
			r.Context(),
			sourceID,
		)

		if err != nil {
			http.Error(
				w,
				"internal error",
				http.StatusInternalServerError,
			)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		_ = json.NewEncoder(w).Encode(map[string]any{
			"source_id": sourceID,
			"counts":    counts,
		})
	}
}
