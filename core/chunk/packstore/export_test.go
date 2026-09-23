package packstore

import (
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
)

// WithBackoff shortens the retry backoff for tests.
func WithBackoff(o Options, d time.Duration) Options {
	o.backoff = d
	return o
}

// CorruptCached flips a byte of a chunk's cached copy, as a memory fault
// would; it reports whether the chunk was cached.
func CorruptCached(s *Store, h hash.Hash) bool {
	if s.cache == nil {
		return false
	}
	s.cache.mu.Lock()
	defer s.cache.mu.Unlock()
	e, ok := s.cache.items[h]
	if !ok {
		return false
	}
	d := e.Value.(*cached).data
	if len(d) == 0 {
		return false
	}
	d[len(d)/2] ^= 1
	return true
}
