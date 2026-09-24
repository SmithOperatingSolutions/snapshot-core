package vcs_test

import (
	"context"
	"errors"
	"strings"
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

// Commit refuses what CommitWorkingSet and UpdateWorkingSet refuse: a
// message over the bound or not UTF-8, a merge state that is not the
// stored one, and a merge with a conflict standing; and it commits a merge
// once the conflict is resolved, with both parents.
func TestCommitRefusesWhatTheTwoStepsRefuse(t *testing.T) {
	f := newFixture(t)
	ws, err := f.r.WorkingSet(ctx, alice, vcs.MainBranch)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.r.Commit(ctx, alice, vcs.MainBranch, ws, ws.Working, strings.Repeat("x", vcs.MaxMessageLen+1)); err == nil {
		t.Fatal("a message over the bound was committed")
	}
	if _, err := f.r.Commit(ctx, alice, vcs.MainBranch, ws, ws.Working, "bad \xff utf-8"); err == nil {
		t.Fatal("a message that is not UTF-8 was committed")
	}
	notStored := ws
	notStored.Merge = &vcs.MergeState{}
	if _, err := f.r.Commit(ctx, alice, vcs.MainBranch, notStored, ws.Working, "m"); !errors.Is(err, vcs.ErrMergeState) {
		t.Fatalf("Commit with a merge state that is not the stored one = %v, want ErrMergeState", err)
	}
	// A merge with a conflict standing, as TestConflictsBlockTheCommitUntilResolved sets up.
	f.put(vcs.MainBranch, "doc", f.obj(8, "base"))
	f.commit(vcs.MainBranch, "base")
	f.branchFrom("dev")
	f.put("dev", "doc", f.obj(8, "dev"))
	theirs := f.commit("dev", "dev work")
	f.put(vcs.MainBranch, "doc", f.obj(8, "main"))
	ours := f.commit(vcs.MainBranch, "main work")
	if r, err := f.r.Merge(ctx, alice, vcs.MainBranch, theirs.Hash); err != nil || len(r.Conflicts) != 1 {
		t.Fatalf("positive control: the merge found %+v (%v), want one conflict", r.Conflicts, err)
	}
	merging, err := f.r.WorkingSet(ctx, alice, vcs.MainBranch)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.r.Commit(ctx, alice, vcs.MainBranch, merging, merging.Working, "too soon"); !errors.Is(err, vcs.ErrUnresolvedConflicts) {
		t.Fatalf("Commit with a conflict standing = %v, want ErrUnresolvedConflicts", err)
	}
	resolved := f.obj(8, "resolved")
	if err := f.r.ResolveConflict(ctx, alice, vcs.MainBranch, "doc", &resolved); err != nil {
		t.Fatal(err)
	}
	merging, err = f.r.WorkingSet(ctx, alice, vcs.MainBranch)
	if err != nil {
		t.Fatal(err)
	}
	c, err := f.r.Commit(ctx, alice, vcs.MainBranch, merging, merging.Working, "merge dev")
	if err != nil {
		t.Fatalf("Commit of a resolved merge: %v", err)
	}
	if len(c.Parents) != 2 || c.Parents[0] != ours.Hash || c.Parents[1] != theirs.Hash || c.Height != max(ours.Height, theirs.Height)+1 {
		t.Fatalf("the merge commit is %+v; want parents [ours theirs]", c)
	}
	if got, _ := f.get(c.Namespace, "doc"); got != resolved {
		t.Fatal("the merge commit does not hold the resolution")
	}
	if after, _ := f.r.WorkingSet(ctx, alice, vcs.MainBranch); after.Merge != nil {
		t.Fatal("the merge state stands after the commit")
	}
}
