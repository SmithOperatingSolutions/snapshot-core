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
