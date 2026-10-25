// Package counter provides a low-contention monotonic counter for the hot path.
package counter

import (
	"math/rand/v2"
	"sync/atomic"
)

const shards = 16

type shard struct {
	n atomic.Uint64
	_ [56]byte
}

// Sharded is a contention-free counter. The zero value is ready to use.
type Sharded struct {
	s    [shards]shard
	last atomic.Uint64
}

// Inc adds one.
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

// Delta returns the increase since the previous call. Not safe for concurrent use.
func (c *Sharded) Delta() uint64 {
	t := c.Load()
	return t - c.last.Swap(t)
}
