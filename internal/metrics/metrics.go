// Package metrics aggregates live event counts in Redis, keyed by source
// and event type, so the query API can read totals directly without ever
// asking a specific shard worker for data.
package metrics

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Aggregator records and reads event counters in Redis.
type Aggregator struct {
	client *redis.Client
}

// New creates an Aggregator using the given Redis client.
func New(client *redis.Client) *Aggregator {
	return &Aggregator{client: client}
}

func counterKey(sourceID, eventType string) string {
	return fmt.Sprintf("metrics:%s:%s", sourceID, eventType)
}

func eventTypesKey(sourceID string) string {
	return "eventtypes:" + sourceID
}

// Record increments the counter for this source+event type, and remembers
// that this event type has been seen for this source (so GetAll can later
// enumerate every event type without the caller having to know them ahead
// of time).
func (a *Aggregator) Record(ctx context.Context, sourceID, eventType string) error {
	if err := a.client.Incr(ctx, counterKey(sourceID, eventType)).Err(); err != nil {
		return err
	}
	return a.client.SAdd(ctx, eventTypesKey(sourceID), eventType).Err()
}

// Get returns the current count for a source+event type.
func (a *Aggregator) Get(ctx context.Context, sourceID, eventType string) (int64, error) {
	val, err := a.client.Get(ctx, counterKey(sourceID, eventType)).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	return val, err
}

// GetAll returns the counts for every event type ever recorded for this
// source, e.g. {"view": 42, "click": 7}. Used by the metrics API so it can
// return a full breakdown for a source in one call.
func (a *Aggregator) GetAll(ctx context.Context, sourceID string) (map[string]int64, error) {
	eventTypes, err := a.client.SMembers(ctx, eventTypesKey(sourceID)).Result()
	if err != nil {
		return nil, err
	}
	result := make(map[string]int64, len(eventTypes))
	for _, et := range eventTypes {
		count, err := a.Get(ctx, sourceID, et)
		if err != nil {
			return nil, err
		}
		result[et] = count
	}
	return result, nil
}
