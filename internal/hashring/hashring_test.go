package hashring

import (
	"fmt"
	"math/rand"
	"testing"
)

// TestDistribution adds 5 nodes and hashes 10,000 random source IDs,
// confirming the distribution across nodes is roughly even. This is the
// "concrete number to cite if asked about load balancing" mentioned in the
// write-up's testing section.
func TestDistribution(t *testing.T) {
	r := New(100) // virtual replicas per node
	nodes := []string{"node-0", "node-1", "node-2", "node-3", "node-4"}
	for _, n := range nodes {
		r.AddNode(n)
	}

	const total = 10000
	counts := make(map[string]int, len(nodes))
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("source-%d", rng.Intn(1_000_000))
		node := r.GetNode(key)
		counts[node]++
	}

	expected := float64(total) / float64(len(nodes))
	for _, n := range nodes {
		got := counts[n]
		pct := float64(got) / float64(total) * 100
		t.Logf("%-8s got %5d hits (%.1f%%, expected ~%.0f)", n, got, pct, expected)
		if got == 0 {
			t.Fatalf("node %s received zero keys — ring is broken", n)
		}
		// Sanity bound: with 100 virtual replicas/node, no node should be
		// off by more than ~40% of its fair share.
		if deviation := (float64(got) - expected) / expected; deviation > 0.4 || deviation < -0.4 {
			t.Errorf("node %s deviates from even distribution by %.0f%% (got %d, expected ~%.0f)", n, deviation*100, got, expected)
		}
	}
}

// TestRemoveNodeReroutes confirms that removing a node only reroutes the
// keys that node owned, rather than reshuffling everything — the classic
// consistent-hashing property from the write-up's extension ideas.
func TestRemoveNodeReroutes(t *testing.T) {
	r := New(100)
	nodes := []string{"node-0", "node-1", "node-2", "node-3"}
	for _, n := range nodes {
		r.AddNode(n)
	}

	keys := make([]string, 2000)
	before := make(map[string]string, len(keys))
	for i := range keys {
		keys[i] = fmt.Sprintf("source-%d", i)
		before[keys[i]] = r.GetNode(keys[i])
	}

	r.RemoveNode("node-1")

	moved := 0
	for _, k := range keys {
		after := r.GetNode(k)
		if after == "node-1" {
			t.Fatalf("key %s still routed to removed node-1", k)
		}
		if before[k] != "node-1" && before[k] != after {
			t.Fatalf("key %s moved from %s to %s even though its owner wasn't removed", k, before[k], after)
		}
		if before[k] != after {
			moved++
		}
	}
	t.Logf("removing 1 of 4 nodes moved %d/%d keys (%.1f%%) — only the removed node's keys should move", moved, len(keys), float64(moved)/float64(len(keys))*100)
}
