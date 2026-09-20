// Package queue provides an in-memory, channel-based queue per shard.
// Since this build runs as a single binary (no separate ingestor/worker
// processes), the hashring picks a shard *name* for an event and this
// package is what actually gets that event to the right pool of
// goroutines, in place of an HTTP hop between processes.
package queue

import "github.com/yourname/shardstream/internal/event"

// Manager owns one buffered channel per shard name.
type Manager struct {
	shards map[string]chan event.Event
}

// NewManager creates a Manager with one buffered channel of the given size
// per shard name.
func NewManager(shardNames []string, bufferSize int) *Manager {
	m := &Manager{shards: make(map[string]chan event.Event, len(shardNames))}
	for _, name := range shardNames {
		m.shards[name] = make(chan event.Event, bufferSize)
	}
	return m
}

// Channel returns the queue channel for a given shard, so worker
// goroutines can range over it. Returns nil if the shard name is unknown.
func (m *Manager) Channel(shard string) chan event.Event {
	return m.shards[shard]
}

// Enqueue pushes an event onto its shard's queue without blocking.
// It reports false if the shard's buffer is currently full (backpressure)
// or the shard name is unknown, so the HTTP handler can fail fast (503)
// instead of blocking the request indefinitely.
func (m *Manager) Enqueue(shard string, e event.Event) bool {
	ch, ok := m.shards[shard]
	if !ok {
		return false
	}
	select {
	case ch <- e:
		return true
	default:
		return false
	}
}
