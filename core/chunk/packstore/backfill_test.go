package packstore_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
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

// packReads counts the reads that reach the backend's packs.
type packReads struct {
	blob.BlobStore
	n atomic.Int64
}

func (p *packReads) Get(ctx context.Context, name string, off, n int64) (io.ReadCloser, error) {
	if strings.HasPrefix(name, "packs/") {
		p.n.Add(1)
	}
	return p.BlobStore.Get(ctx, name, off, n)
}

// The chunk cache holds at most CacheBytes: going over evicts the least
// recently read chunk, and a chunk bigger than the whole cache is never
// kept, nor does it flush what is there on its way through.
func TestTheChunkCacheIsByteBounded(t *testing.T) {
	inner, kr := &packReads{BlobStore: mem.New()}, keyring(t)
	w := open(t, inner, kr)
	a, b, c, big := payload("a", 4000), payload("b", 4000), payload("c", 4000), payload("big", 12000)
	for _, d := range [][]byte{a, b, c, big} {
		if _, err := w.Put(ctx, d); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.CompareAndSetRoot(ctx, hash.Hash{}, hash.Sum(a)); err != nil {
		t.Fatal(err)
	}
	s, err := packstore.Open(ctx, packstore.Options{Blobs: inner, Keys: kr, Repo: repo, CacheBytes: 10000})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	reads := func(d []byte) int64 { // backend reads one Get took
		t.Helper()
		before := inner.n.Load()
		if _, err := s.Get(ctx, hash.Sum(d)); err != nil {
			t.Fatal(err)
		}
		return inner.n.Load() - before
	}
	if reads(a) != 1 || reads(a) != 0 {
		t.Fatal("positive control: a chunk read twice was not cached")
	}
	reads(b) // a and b: 8000 of 10000 bytes
	if reads(a) != 0 {
		t.Fatal("a was evicted while the cache had room") // a is now the most recent
	}
	reads(big)
	if reads(a) != 0 || reads(b) != 0 {
		t.Error("reading a chunk bigger than the whole cache flushed it")
	}
	if reads(big) != 1 {
		t.Error("a chunk bigger than the whole cache was kept")
	}
	reads(c) // 12000 bytes: over the cap, so the least recent (a) goes
	if reads(c) != 0 || reads(b) != 0 {
		t.Error("going over the cap evicted a recent chunk")
	}
	if reads(a) != 1 {
		t.Error("the least recently read chunk survived going over the cap")
	}
}
