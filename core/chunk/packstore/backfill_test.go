package packstore_test

import (
	"errors"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/mem"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/packstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
)

// A store whose view predates another writer's commit must refresh before
// calling a conflict: the expected root it was handed is current.
func TestStaleViewIsRefreshedBeforeAConflict(t *testing.T) {
	bs := mem.New()
	kr := keyring(t)
	a, b := open(t, bs, kr), open(t, bs, kr) // b's view: no root
	h, _ := a.Put(ctx, []byte("a's root"))
	if err := a.CompareAndSetRoot(ctx, hash.Hash{}, h); err != nil {
		t.Fatal(err)
	}
	mine, _ := b.Put(ctx, []byte("b's root"))
	if err := b.CompareAndSetRoot(ctx, h, mine); err != nil {
		t.Fatalf("CAS from the current root, by a store that had not seen it yet = %v: a stale cache made a conflict", err)
	}
	if r, _ := a.Root(ctx); r != mine {
		t.Fatalf("root = %s, want %s", r.Short(), mine.Short())
	}
}

// "SHA-256 verified on every read, from every backend, cached or not."
func TestCachedReadsAreVerified(t *testing.T) {
	s := open(t, mem.New(), keyring(t))
	h, _ := s.Put(ctx, payload("cached", 4000))
	if err := s.CompareAndSetRoot(ctx, hash.Hash{}, h); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, h); err != nil { // fills the cache
		t.Fatal(err)
	}
	if !packstore.CorruptCached(s, h) {
		t.Fatal("the chunk is not cached after a read; the fixture tests nothing")
	}
	if b, err := s.Get(ctx, h); !errors.Is(err, chunk.ErrCorrupt) {
		t.Fatalf("a read served from a corrupted cache = %d bytes, %v; want ErrCorrupt", len(b), err)
	}
}
