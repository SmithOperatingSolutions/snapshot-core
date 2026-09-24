package vcs_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/memstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/object"
	"github.com/SmithOperatingSolutions/snapshot-core/core/vcs"
)

// countingStore counts the root swaps: each is a publish, the unit a
// commit's cost is measured in.
type countingStore struct {
	chunk.Store
	swaps atomic.Int64
}

func (c *countingStore) CompareAndSetRoot(ctx context.Context, expected, next hash.Hash) error {
	c.swaps.Add(1)
	return c.Store.CompareAndSetRoot(ctx, expected, next)
}

// #10: a host that edits a namespace and commits it means one operation;
// Commit does it in one publish, where UpdateWorkingSet then
// CommitWorkingSet took two. The commit is the same: the namespace, the
// parent, the height, and a clean working set at it. A stale working set
// is a conflict, and a commit that lands is still the branch's only new one.
func TestACommitIsOnePublish(t *testing.T) {
	cs := &countingStore{Store: memstore.New()}
	f := newFixtureOn(t, cs)
	ws, err := f.r.WorkingSet(ctx, alice, vcs.MainBranch)
	if err != nil {
		t.Fatal(err)
	}
	n, err := f.r.Namespace(ctx, ws.Working)
	if err != nil {
		t.Fatal(err)
	}
	e := n.Editor()
	if err := e.Put("a", f.obj(7, "a")); err != nil {
		t.Fatal(err)
	}
	if n, err = e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	before := f.head(vcs.MainBranch)
	swaps := cs.swaps.Load()
	c, err := f.r.Commit(ctx, alice, vcs.MainBranch, ws, n.Root(), "one publish")
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if got := cs.swaps.Load() - swaps; got != 1 {
		t.Fatalf("committing an edited namespace swapped the root %d times, want once", got)
	}
	head := f.head(vcs.MainBranch)
	if head.Hash != c.Hash || head.Namespace != n.Root() || len(head.Parents) != 1 || head.Parents[0] != before.Hash || head.Height != before.Height+1 || head.Message != "one publish" {
		t.Fatalf("the head is %+v, want the commit of %s onto %s", head, n.Root().Short(), before.Hash.Short())
	}
	after, err := f.r.WorkingSet(ctx, alice, vcs.MainBranch)
	if err != nil {
		t.Fatal(err)
	}
	if after.Working != n.Root() || after.Staged != n.Root() || after.Merge != nil {
		t.Fatalf("after the commit the working set is %+v, want clean at %s", after, n.Root().Short())
	}
	if ref, ok := f.get(head.Namespace, "a"); !ok || ref.Model != 7 {
		t.Fatalf("the committed namespace holds a = %+v, %v", ref, ok)
	}
	// Stale: the working set moved on since prev was read.
	if _, err := f.r.Commit(ctx, alice, vcs.MainBranch, ws, n.Root(), "again"); !errors.Is(err, vcs.ErrConflict) {
		t.Fatalf("Commit with a stale working set = %v, want ErrConflict", err)
	}
	if got := f.head(vcs.MainBranch); got.Hash != c.Hash {
		t.Fatalf("a conflicting Commit moved the head to %s", got.Hash.Short())
	}
	_ = object.Ref{}
}
