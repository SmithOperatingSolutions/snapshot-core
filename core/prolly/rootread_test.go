package prolly_test

import (
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
)

// A map is immutable, so its root node, read and checked when the map is
// made, is never read again (#43): Get reads one node per level below the
// root, and a flush of one value edit reads no more than the path to it
// per level. Every read is a hash and a decode, and a host committing per
// object opens its maps afresh each time, so a root read twice is paid on
// every commit.
func TestTheRootIsReadOnceWhenTheMapIsMade(t *testing.T) {
	s := newStore()
	built := build(t, s, 20000, "a")
	s.gets.Store(0)
	m, err := prolly.Open(ctx, s, prolly.DefaultConfig(), built.Root())
	must(t, err)
	if n := s.gets.Load(); n != 1 {
		t.Fatalf("fixture: opening the map read %d nodes, want its root", n)
	}
	if m.Height() < 1 {
		t.Fatalf("fixture: height %d", m.Height())
	}
	s.gets.Store(0)
	if _, ok := get(t, m, key(12345)); !ok {
		t.Fatal("positive control: the key is missing")
	}
	if n, want := s.gets.Load(), int64(m.Height()); n != want {
		t.Fatalf("a Get in a map of height %d read %d nodes, want %d: the root it already holds was read again", m.Height(), n, want)
	}
	e := m.Editor()
	must(t, e.Put(key(12345), val(12345, "b")))
	s.gets.Store(0)
	flushed := flush(t, e)
	// Each level below the root is sought from the root: level L reads
	// height-L nodes to reach its node, and nothing more for a value edit
	// that moves no boundary.
	h := int64(m.Height())
	if n, want := s.gets.Load(), h*(h+1)/2; n != want {
		t.Fatalf("flushing one value edit into a map of height %d read %d nodes, want %d: the path below the root, once per level", h, n, want)
	}
	s.gets.Store(0)
	if _, ok := get(t, flushed, key(5)); !ok {
		t.Fatal("positive control: the key is missing after the flush")
	}
	if n := s.gets.Load(); n != h {
		t.Fatalf("a Get in the map a flush made read %d nodes, want %d: the flush's own root was read again", n, h)
	}
}
