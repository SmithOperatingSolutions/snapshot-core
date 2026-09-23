package packstore

import (
	"context"
	"sort"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/dedup"
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

// Recorded lists, in order, the objects the store's manifest records as
// orphans GC deleted.
func Recorded(ctx context.Context, o Options) ([]string, error) {
	r, err := o.Blobs.Root(ctx)
	if err != nil || len(r.Value) == 0 {
		return nil, err
	}
	m, err := openManifest(r.Value, o.Keys, o.Repo)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, c := range m.condemned {
		switch c.kind {
		case deletedPack:
			out = append(out, dedup.PackName(c.sum))
		case deletedIndex:
			out = append(out, indexName(c.sum))
		}
	}
	sort.Strings(out)
	return out, nil
}
