// Package hashring implements a consistent hash ring used to route events
// to shard workers by source_id. Adding or removing a node only reshuffles
// a small fraction of keys, rather than every key, which is the whole
// point of using consistent hashing over a plain modulo hash.
package hashring

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

// Ring is a consistent hash ring. It is safe for concurrent use.
type Ring struct {
	mu       sync.RWMutex
	replicas int
	nodes    map[uint32]string // hash -> node name
	sorted   []uint32
}

// New creates a Ring. replicas is the number of virtual nodes placed on the
// ring per real node — more replicas smooths out load distribution at the
// cost of a bit more memory and lookup time.
func New(replicas int) *Ring {
	return &Ring{replicas: replicas, nodes: make(map[uint32]string)}
}

func (r *Ring) hashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

// AddNode adds a shard worker to the ring, with virtual replicas
// to smooth out load distribution.
func (r *Ring) AddNode(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := 0; i < r.replicas; i++ {
		h := r.hashKey(name + "#" + strconv.Itoa(i))
		r.nodes[h] = name
		r.sorted = append(r.sorted, h)
	}
	sort.Slice(r.sorted, func(i, j int) bool { return r.sorted[i] < r.sorted[j] })
}

// RemoveNode removes a shard worker (and all its virtual replicas) from the
// ring. Keys that were owned by this node fall through to the next node
// clockwise on the ring.
func (r *Ring) RemoveNode(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	filtered := r.sorted[:0]
	for _, h := range r.sorted {
		if r.nodes[h] == name {
			delete(r.nodes, h)
			continue
		}
		filtered = append(filtered, h)
	}
	r.sorted = filtered
}

// GetNode returns which shard worker should own this key.
func (r *Ring) GetNode(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.sorted) == 0 {
		return ""
	}
	h := r.hashKey(key)
	idx := sort.Search(len(r.sorted), func(i int) bool { return r.sorted[i] >= h })
	if idx == len(r.sorted) {
		idx = 0 // wrap around the ring
	}
	return r.nodes[r.sorted[idx]]
}
