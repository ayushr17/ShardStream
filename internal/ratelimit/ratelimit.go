// Package ratelimit provides a Redis-backed rate limiter shared across
// shards, so a misbehaving or malicious source can be capped even though
// its events might land on different shard workers over time.
package ratelimit

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Limiter is a fixed-window rate limiter backed by Redis.
type Limiter struct {
	client *redis.Client
}

// New creates a Limiter using the given Redis client.
func New(client *redis.Client) *Limiter {
	return &Limiter{client: client}
}

// Allow uses a fixed-window counter: INCR the key, set TTL on first hit.
// It reports whether the request is within limit.
func (l *Limiter) Allow(ctx context.Context, key string, limit int, window time.Duration) (bool, error) {
	count, err := l.client.Incr(ctx, key).Result()
	if err != nil {
		return false, err
	}
	if count == 1 {
		l.client.Expire(ctx, key, window)
	}
	return count <= int64(limit), nil
}
