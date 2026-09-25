package packstore_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/mem"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
)

// countingPuts counts the pack and index objects put to the backend.
type countingPuts struct {
	blob.BlobStore
	puts atomic.Int64
}

func (c *countingPuts) Put(ctx context.Context, name string, r io.Reader, size int64) error {
	if strings.HasPrefix(name, "packs/") || strings.HasPrefix(name, "index/") {
		c.puts.Add(1)
	}
	return c.BlobStore.Put(ctx, name, r, size)
}

// A writer that lost the race learns it before it writes: a store whose
// view of the root is stale, because another store published meanwhile,
// is refused its publish with ErrRootConflict and puts no pack and no
// index object to the backend; its chunks stay pending and go out with the
// publish that wins. A store whose view is current publishes, packs and
// index objects included.
func TestALoserLearnsBeforeItWrites(t *testing.T) {
	bs := &countingPuts{BlobStore: mem.New()}
	kr := keyring(t)
	winner, loser := open(t, bs, kr), open(t, bs, kr)
	if _, err := loser.Root(ctx); err != nil { // the loser's view: no root yet
		t.Fatal(err)
	}
	w, err := winner.Put(ctx, payload("the winner's", 2000))
	if err != nil {
		t.Fatal(err)
	}
	if err := winner.CompareAndSetRoot(ctx, hash.Hash{}, w); err != nil {
		t.Fatal(err)
	}
	if n := bs.puts.Load(); n == 0 {
		t.Fatal("positive control: the winner's publish put no pack or index object")
	}
	l, err := loser.Put(ctx, payload("the loser's", 2000))
	if err != nil {
		t.Fatal(err)
	}
	before := bs.puts.Load()
	if err := loser.CompareAndSetRoot(ctx, hash.Hash{}, l); !errors.Is(err, chunk.ErrRootConflict) {
		t.Fatalf("a publish over a root that moved = %v, want ErrRootConflict", err)
	}
	if n := bs.puts.Load() - before; n != 0 {
		t.Errorf("a writer that lost the race put %d pack and index objects before it learned: a loser must learn first and write nothing", n)
	}
	if err := loser.CompareAndSetRoot(ctx, w, l); err != nil { // retried over the root it now sees
		t.Fatalf("the loser's retry over the current root: %v", err)
	}
	if bs.puts.Load() == before {
		t.Error("the loser's winning retry put nothing: its chunks must go out with it")
	}
	fresh := open(t, bs, kr)
	for name, h := range map[string]hash.Hash{"the winner's": w, "the loser's": l} {
		if _, err := fresh.Get(ctx, h); err != nil {
			t.Errorf("after both publishes %s chunk does not read from a fresh store: %v", name, err)
		}
	}
}

// midUpload runs a function once, as the first pack is put after it is
// armed: another writer acting while a publish uploads.
type midUpload struct {
	blob.BlobStore
	armed atomic.Bool
	fn    func()
}

func (m *midUpload) Put(ctx context.Context, name string, r io.Reader, size int64) error {
	if strings.HasPrefix(name, "packs/") && m.armed.CompareAndSwap(true, false) {
		m.fn()
	}
	return m.BlobStore.Put(ctx, name, r, size)
}

// A root that moves after the check, while the publish uploads, still loses
// the publish its swap: ErrRootConflict, the other writer's root kept, and
// the loser's pack and index object written but named by no manifest, so
// GC collects them as orphans.
func TestARootThatMovesDuringTheUploadStillLosesTheSwap(t *testing.T) {
	bs := &midUpload{BlobStore: mem.New()}
	kr := keyring(t)
	loser, other := open(t, bs, kr), open(t, bs, kr)
	base, err := loser.Put(ctx, []byte("the root both start from"))
	if err != nil {
		t.Fatal(err)
	}
	if err := loser.CompareAndSetRoot(ctx, hash.Hash{}, base); err != nil {
		t.Fatal(err)
	}
	var moved hash.Hash
	bs.fn = func() {
		h, err := other.Put(ctx, []byte("the other writer's root"))
		if err != nil {
			t.Error(err)
			return
		}
		if err := other.CompareAndSetRoot(ctx, base, h); err != nil {
			t.Errorf("the other writer's publish mid-upload: %v", err)
			return
		}
		moved = h
	}
	mine, err := loser.Put(ctx, []byte("the loser's root"))
	if err != nil {
		t.Fatal(err)
	}
	packs, indexes := objects(t, bs, "packs/"), objects(t, bs, "index/")
	bs.armed.Store(true)
	if err := loser.CompareAndSetRoot(ctx, base, mine); !errors.Is(err, chunk.ErrRootConflict) {
		t.Fatalf("a publish whose root moved while it uploaded = %v, want ErrRootConflict: it would overwrite the other writer's commit", err)
	}
	if moved.IsZero() {
		t.Fatal("fixture: the other writer never published mid-upload")
	}
	if r, err := open(t, bs, kr).Root(ctx); err != nil || r != moved {
		t.Fatalf("after the lost swap a fresh store opens at %s (%v), want the other writer's root %s", r.Short(), err, moved.Short())
	}
	// The loser's pack and index object: written, named by no manifest.
	newPacks := newNames(packs, objects(t, bs, "packs/"))
	newIndexes := newNames(indexes, objects(t, bs, "index/"))
	if len(newPacks) != 2 || len(newIndexes) != 2 { // the other writer's and the loser's
		t.Fatalf("fixture: the two publishes wrote packs %v and index objects %v, want two of each", newPacks, newIndexes)
	}
	// GC handed both new index objects keeps the one the manifest names
	// and collects the loser's.
	out := orphanRound(t, bs, kr, liveSet(moved), t0.Add(2*time.Hour), newIndexes)
	if len(out.Orphans) != 1 {
		t.Fatalf("GC collected %v of the two new index objects, want one: the loser's is an orphan and the other writer's is live", out.Orphans)
	}
}

// A root that moves away and back while a publish uploads, the store having
// seen it away, is where the caller expected it: the publish reads the
// root again before its swap, rather than trust the view that saw it gone,
// and lands.
func TestARootThatMovesAndReturnsDuringTheUploadStillPublishes(t *testing.T) {
	bs := &midUpload{BlobStore: mem.New()}
	kr := keyring(t)
	s, other := open(t, bs, kr), open(t, bs, kr)
	base, err := s.Put(ctx, []byte("the root it returns to"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CompareAndSetRoot(ctx, hash.Hash{}, base); err != nil {
		t.Fatal(err)
	}
	sawAway := false
	bs.fn = func() {
		away, err := other.Put(ctx, []byte("the root it moves to"))
		if err != nil {
			t.Error(err)
			return
		}
		if err := other.CompareAndSetRoot(ctx, base, away); err != nil {
			t.Errorf("moving the root away: %v", err)
			return
		}
		if r, err := s.Root(ctx); err != nil || r != away { // the publishing store sees it away
			t.Errorf("fixture: the store sees %s (%v), want the root away", r.Short(), err)
			return
		}
		if err := other.CompareAndSetRoot(ctx, away, base); err != nil {
			t.Errorf("moving the root back: %v", err)
			return
		}
		sawAway = true
	}
	mine, err := s.Put(ctx, []byte("the publish over it"))
	if err != nil {
		t.Fatal(err)
	}
	bs.armed.Store(true)
	if err := s.CompareAndSetRoot(ctx, base, mine); err != nil {
		t.Fatalf("a publish over a root that moved away and back while it uploaded = %v: the root is where the caller expected it, so it must land", err)
	}
	if !sawAway {
		t.Fatal("fixture: the root never moved away and back mid-upload")
	}
	if r, err := open(t, bs, kr).Root(ctx); err != nil || r != mine {
		t.Fatalf("a fresh store opens at %s (%v), want the publish's root %s", r.Short(), err, mine.Short())
	}
}
