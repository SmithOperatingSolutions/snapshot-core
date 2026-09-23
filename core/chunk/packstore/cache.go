package packstore

import (
	"container/list"
	"sync"

	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
)

// cache is a byte-bounded LRU of chunk bytes. It saves the backend read, not
// the verification: Get re-hashes a cached chunk like any other ("SHA-256
// verified on every read, from every backend, cached or not").
type cache struct {
	mu    sync.Mutex
	max   int
	used  int
	order *list.List // front = most recent
	items map[hash.Hash]*list.Element
}

type cached struct {
	h    hash.Hash
	data []byte
}

func newCache(max int) *cache {
	return &cache{max: max, order: list.New(), items: map[hash.Hash]*list.Element{}}
}

func (c *cache) get(h hash.Hash) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.items[h]
	if !ok {
		return nil, false
	}
	c.order.MoveToFront(e)
	return e.Value.(*cached).data, true
}

func (c *cache) put(h hash.Hash, data []byte) {
	if c == nil || len(data) > c.max {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.items[h]; ok {
		return
	}
	c.items[h] = c.order.PushFront(&cached{h: h, data: data})
	c.used += len(data)
	for c.used > c.max {
		last := c.order.Back()
		v := last.Value.(*cached)
		c.order.Remove(last)
		delete(c.items, v.h)
		c.used -= len(v.data)
	}
}
