package vcs_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/auth"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/object"
	"github.com/SmithOperatingSolutions/snapshot-core/core/vcs"
)

// granted allows alice everything. Bob may read everything and write every
// path; on one branch he may do only the actions listed, on the others
// anything.
type granted struct {
	branch  string
	actions []auth.Action
}

func (g granted) Authorize(_ context.Context, p auth.Principal, a auth.Action, res string) error {
	if p.ID != bob.ID || a == auth.Read || strings.HasPrefix(res, "path:") || res != "branch:"+g.branch {
		return nil
	}
	for _, ok := range g.actions {
		if a == ok {
			return nil
		}
	}
	return errors.New("not granted")
}

// as reopens f's repository under g, for bob to act in.
func (f *fixture) as(g granted) *vcs.Repo {
	f.t.Helper()
	o := f.o
	o.Authorizer = g
	r, err := vcs.Open(ctx, f.s, o)
	if err != nil {
		f.t.Fatal(err)
	}
	return r
}

// withDoc is ws's working namespace with "doc" set to ref.
func (f *fixture) withDoc(ws vcs.WorkingSet, ref object.Ref) hash.Hash {
	f.t.Helper()
	n, err := f.r.Namespace(ctx, ws.Working)
	if err != nil {
		f.t.Fatal(err)
	}
	e := n.Editor()
	if err := e.Put("doc", ref); err != nil {
		f.t.Fatal(err)
	}
	if n, err = e.Flush(ctx); err != nil {
		f.t.Fatal(err)
	}
	return n.Root()
}

// Committing is its own action: bob with write and not commit cannot
// commit what is staged, nor replace and commit in one publish; with commit
// and not write he commits what is staged but cannot publish a namespace of
// his own; with both, he can. Each refusal is ErrDenied and leaves the head.
func TestCommittingIsItsOwnAction(t *testing.T) {
	f := newFixture(t)
	main := vcs.MainBranch
	f.put(main, "doc", f.obj(8, "base"))
	f.commit(main, "base")
	f.put(main, "doc", f.obj(8, "staged by alice"))
	head := f.head(main)

	writeOnly := f.as(granted{branch: main, actions: []auth.Action{auth.Write}})
	if _, err := writeOnly.CommitWorkingSet(ctx, bob, main, "c"); !errors.Is(err, auth.ErrDenied) {
		t.Errorf("CommitWorkingSet with write and not commit = %v, want ErrDenied", err)
	}
	ws, err := f.r.WorkingSet(ctx, alice, main)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writeOnly.Commit(ctx, bob, main, ws, f.withDoc(ws, f.obj(8, "bob's")), "c"); !errors.Is(err, auth.ErrDenied) {
		t.Errorf("Commit with write and not commit = %v, want ErrDenied", err)
	}
	commitOnly := f.as(granted{branch: main, actions: []auth.Action{auth.Commit}})
	if _, err := commitOnly.Commit(ctx, bob, main, ws, f.withDoc(ws, f.obj(8, "bob's")), "c"); !errors.Is(err, auth.ErrDenied) {
		t.Errorf("Commit with commit and not write = %v, want ErrDenied: it replaces the working set too", err)
	}
	if f.head(main).Hash != head.Hash {
		t.Fatal("a refused commit moved the head")
	}
	c, err := commitOnly.CommitWorkingSet(ctx, bob, main, "what alice staged")
	if err != nil {
		t.Fatalf("CommitWorkingSet with commit and not write: %v", err)
	}
	if c.Author != bob.ID || len(c.Parents) != 1 || c.Parents[0] != head.Hash {
		t.Errorf("bob's commit is %+v, want his, on the head", c)
	}
	both := f.as(granted{branch: main, actions: []auth.Action{auth.Write, auth.Commit}})
	ws, err = f.r.WorkingSet(ctx, alice, main)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := both.Commit(ctx, bob, main, ws, f.withDoc(ws, f.obj(8, "bob's")), "c"); err != nil {
		t.Errorf("Commit with write and commit: %v", err)
	}
}

// Merging is its own action: bob with write and commit and not merge
// cannot merge, resolve a conflict or abandon a merge; with merge and not
// write he can do all three, and commits the merge once granted commit.
// Each refusal is ErrDenied and leaves the working set.
func TestMergingIsItsOwnAction(t *testing.T) {
	f := newFixture(t)
	main := vcs.MainBranch
	f.put(main, "doc", f.obj(8, "base"))
	f.commit(main, "base")
	f.branchFrom("dev")
	f.put("dev", "extra", f.obj(8, "dev"))
	clean := f.commit("dev", "dev work").Hash

	noMerge := f.as(granted{branch: main, actions: []auth.Action{auth.Write, auth.Commit}})
	before, err := f.r.WorkingSet(ctx, alice, main)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := noMerge.Merge(ctx, bob, main, clean); !errors.Is(err, auth.ErrDenied) {
		t.Errorf("Merge with write and commit and not merge = %v, want ErrDenied", err)
	}
	if now, _ := f.r.WorkingSet(ctx, alice, main); now.Hash != before.Hash {
		t.Fatal("a refused merge changed the working set")
	}
	mergeOnly := f.as(granted{branch: main, actions: []auth.Action{auth.Merge}})
	if res, err := mergeOnly.Merge(ctx, bob, main, clean); err != nil || len(res.Conflicts) != 0 {
		t.Fatalf("Merge with merge and not write: %d conflicts, %v; want a clean merge", len(res.Conflicts), err)
	}
	if _, err := mergeOnly.CommitWorkingSet(ctx, bob, main, "merge dev"); !errors.Is(err, auth.ErrDenied) {
		t.Errorf("committing the merge with merge and not commit = %v, want ErrDenied", err)
	}
	mergeAndCommit := f.as(granted{branch: main, actions: []auth.Action{auth.Merge, auth.Commit}})
	c, err := mergeAndCommit.CommitWorkingSet(ctx, bob, main, "merge dev")
	if err != nil || len(c.Parents) != 2 {
		t.Fatalf("committing the merge with merge and commit: %+v, %v; want a commit with two parents", c, err)
	}

	f.put(main, "doc", f.obj(8, "main"))
	f.commit(main, "main work")
	f.put("dev", "doc", f.obj(8, "dev"))
	conflicting := f.commit("dev", "dev on doc").Hash
	if res, err := f.r.Merge(ctx, alice, main, conflicting); err != nil || len(res.Conflicts) != 1 {
		t.Fatalf("fixture: alice's merge found %d conflicts (%v), want 1", len(res.Conflicts), err)
	}
	resolved := f.obj(8, "resolved")
	if err := noMerge.ResolveConflict(ctx, bob, main, "doc", &resolved); !errors.Is(err, auth.ErrDenied) {
		t.Errorf("ResolveConflict with write and not merge = %v, want ErrDenied", err)
	}
	if err := noMerge.AbortMerge(ctx, bob, main); !errors.Is(err, auth.ErrDenied) {
		t.Errorf("AbortMerge with write and not merge = %v, want ErrDenied", err)
	}
	if err := mergeOnly.ResolveConflict(ctx, bob, main, "doc", &resolved); err != nil {
		t.Errorf("ResolveConflict with merge and not write: %v", err)
	}
	if err := mergeOnly.AbortMerge(ctx, bob, main); err != nil {
		t.Errorf("AbortMerge with merge and not write: %v", err)
	}
}

// A protected branch is an authorizer's policy: on main bob may merge and
// commit but not write. A direct write to main is refused (and a one-publish
// commit of his own namespace), the working set unchanged; he writes on a
// feature branch, merges it into main and commits the merge, and main holds
// his change under a merge commit.
func TestAProtectedBranchTakesMergesNotDirectWrites(t *testing.T) {
	f := newFixture(t)
	main := vcs.MainBranch
	f.put(main, "doc", f.obj(8, "base"))
	f.commit(main, "base")
	f.branchFrom("feature")
	f.r = f.as(granted{branch: main, actions: []auth.Action{auth.Merge, auth.Commit}})

	ws, err := f.r.WorkingSet(ctx, bob, main)
	if err != nil {
		t.Fatalf("positive control: bob reads main: %v", err)
	}
	next := ws
	next.Working = f.withDoc(ws, f.obj(8, "straight to main"))
	next.Staged = next.Working
	if _, err := f.r.UpdateWorkingSet(ctx, bob, main, ws, next); !errors.Is(err, auth.ErrDenied) {
		t.Errorf("a direct write to the protected main = %v, want ErrDenied", err)
	}
	if _, err := f.r.Commit(ctx, bob, main, ws, next.Working, "straight to main"); !errors.Is(err, auth.ErrDenied) {
		t.Errorf("a one-publish commit to the protected main = %v, want ErrDenied", err)
	}
	if now, _ := f.r.WorkingSet(ctx, bob, main); now.Hash != ws.Hash {
		t.Fatal("a refused write changed main's working set")
	}

	change := f.obj(8, "through a merge")
	f.edit(bob, "feature", map[string]*object.Ref{"doc": &change})
	feature, err := f.r.CommitWorkingSet(ctx, bob, "feature", "on feature")
	if err != nil {
		t.Fatalf("bob commits on his feature branch: %v", err)
	}
	if res, err := f.r.Merge(ctx, bob, main, feature.Hash); err != nil || len(res.Conflicts) != 0 {
		t.Fatalf("bob merges feature into the protected main: %d conflicts, %v", len(res.Conflicts), err)
	}
	c, err := f.r.CommitWorkingSet(ctx, bob, main, "merge feature")
	if err != nil {
		t.Fatalf("bob commits the merge on the protected main: %v", err)
	}
	if len(c.Parents) != 2 || c.Parents[1] != feature.Hash {
		t.Errorf("main's head is %+v, want a merge commit with feature as its second parent", c)
	}
	if got, ok := f.get(c.Namespace, "doc"); !ok || got != change {
		t.Error("main does not hold bob's change after the merge")
	}
}
