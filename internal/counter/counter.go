// Package counter provides a low-contention monotonic counter for the hot path.
package counter

import (
	"math/rand/v2"
	"sync/atomic"
)

const shards = 16 // power of two

// shard is padded to a cache line so shards updated by different cores don't
// false-share.
type shard struct {
	n atomic.Uint64
	_ [56]byte
}

// Sharded is a counter split across shards to avoid a single contended
// cache line when many goroutines increment it. The zero value is ready to use.
type Sharded struct {
	s    [shards]shard
	last atomic.Uint64 // total at the previous Delta call
}

// Inc adds one. Shard choice uses the runtime's per-thread random source,
// which is cheap and needs no lock.
func (c *Sharded) Inc() {
	c.s[rand.Uint32()&(shards-1)].n.Add(1)
}

// Load returns the current total.
func (c *Sharded) Load() uint64 {
	var t uint64
	for i := range c.s {
		t += c.s[i].n.Load()
	}
	return t
}

// Delta returns the increase since the previous Delta call. It must be called
// from a single goroutine (the reporting loop).
func (c *Sharded) Delta() uint64 {
	t := c.Load()
	return t - c.last.Swap(t)
}
