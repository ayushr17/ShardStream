package queue

import (
	"testing"

	"github.com/ayushr17/ShardStream/internal/event"
)

func newEvent() event.Event {
	return event.Event{SourceID: "src-1", EventType: "view"}
}

// Enqueue must never block: once a shard's buffer is full it reports false
// immediately, which the HTTP handler turns into a 503 (backpressure).
func TestEnqueueAppliesBackpressureWhenFull(t *testing.T) {
	m := NewManager([]string{"shard-0"}, 2)

	if !m.Enqueue("shard-0", newEvent()) || !m.Enqueue("shard-0", newEvent()) {
		t.Fatal("expected first two enqueues to succeed")
	}
	if m.Enqueue("shard-0", newEvent()) {
		t.Fatal("expected enqueue on a full shard to report false")
	}

	// Draining one slot should make room again.
	<-m.Channel("shard-0")
	if !m.Enqueue("shard-0", newEvent()) {
		t.Fatal("expected enqueue to succeed after a slot was freed")
	}
}

func TestEnqueueUnknownShard(t *testing.T) {
	m := NewManager([]string{"shard-0"}, 1)
	if m.Enqueue("does-not-exist", newEvent()) {
		t.Fatal("expected enqueue on an unknown shard to report false")
	}
	if m.Channel("does-not-exist") != nil {
		t.Fatal("expected nil channel for an unknown shard")
	}
}
