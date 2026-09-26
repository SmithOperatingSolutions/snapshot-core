package prolly_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
)

// rawCounting is a counting store that also takes the raw hint
// (chunk.RawWriter), and counts the puts that came through it.
type rawCounting struct {
	*counting
	raw atomic.Int64
}

func (r *rawCounting) PutRaw(ctx context.Context, data []byte) (hash.Hash, error) {
	r.raw.Add(1)
	return r.counting.Put(ctx, data)
}

// A flush stores the map's nodes through the store's raw hint where it has
// one (#42): nodes are hashes and short keys, which zstd shrinks little at
// the price of its encoder on every commit. A store without the hint is
// put to as before (the positive control), and the map reads the same.
func TestAFlushStoresItsNodesThroughTheRawHint(t *testing.T) {
	plain := newStore()
	pm := build(t, plain, 5000, "a")
	if plain.added.Load() < 3 {
		t.Fatalf("fixture: the flush added %d nodes, want a map of several", plain.added.Load())
	}

	s := &rawCounting{counting: newStore()}
	m, err := prolly.Empty(ctx, s, prolly.DefaultConfig())
	must(t, err)
	e := m.Editor()
	for i := 0; i < 5000; i++ {
		must(t, e.Put(key(i), val(i, "a")))
	}
	s.puts.Store(0)
	s.raw.Store(0)
	flushed := flush(t, e)
	if flushed.Root() != pm.Root() {
		t.Fatalf("the map flushed through the raw hint has root %s, the plain one %s: the hint changed the tree", flushed.Root().Short(), pm.Root().Short())
	}
	puts, raw := s.puts.Load(), s.raw.Load()
	if puts == 0 {
		t.Fatal("fixture: the flush put no node")
	}
	if raw != puts {
		t.Fatalf("the flush put %d nodes, %d through the raw hint: a tree node would pay the zstd encoder on every commit", puts, raw)
	}
	if v, ok := get(t, flushed, key(4321)); !ok || string(v) != string(val(4321, "a")) {
		t.Fatalf("the map flushed through the raw hint reads key 4321 as %q, %v", v, ok)
	}
}
