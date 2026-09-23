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

// RepackedNames lists, in order, the packs the store's manifest records as
// repacked.
func RepackedNames(ctx context.Context, o Options) ([]string, error) {
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
		if c.kind == repackedPack {
			out = append(out, dedup.PackName(c.sum))
		}
	}
	sort.Strings(out)
	return out, nil
}

// PackOrder lists the packs the store's manifest indexes, in the order its
// index objects list them.
func PackOrder(ctx context.Context, o Options) ([]string, error) {
	r, err := o.Blobs.Root(ctx)
	if err != nil {
		return nil, err
	}
	m, err := openManifest(r.Value, o.Keys, o.Repo)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, sum := range m.indexes {
		infos, err := loadIndex(ctx, o, sum)
		if err != nil {
			return nil, err
		}
		for _, p := range infos {
			out = append(out, p.Name)
		}
	}
	return out, nil
}

// ForgedSpilled is a spilled index over a table whose records were written
// by hand, to prove the record decoder refuses what it should.
func ForgedSpilled(t *dedup.Table, packs int) interface {
	Lookup(h hash.Hash) (dedup.Location, bool, error)
} {
	sp := &spilled{table: t}
	for range packs {
		sp.packs = append(sp.packs, packRef{})
	}
	return forged{sp}
}

type forged struct{ sp *spilled }

func (f forged) Lookup(h hash.Hash) (dedup.Location, bool, error) { return f.sp.lookup(h) }

// JoinForged runs a round's join over a forged index table with the given
// number of packs, against a live set of every key in it.
func JoinForged(t *dedup.Table, packs int, live Live) error {
	r := &Round{index: &spilled{table: t}}
	for range packs {
		r.packs = append(r.packs, packSummary{})
	}
	_, _, err := r.join(live)
	return err
}
