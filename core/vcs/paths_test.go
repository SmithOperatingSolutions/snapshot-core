package vcs_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/auth"
	"github.com/SmithOperatingSolutions/snapshot-core/core/object"
	"github.com/SmithOperatingSolutions/snapshot-core/core/vcs"
)

// fenced allows everything except, for one principal, writing under one
// path prefix of one branch.
type fenced struct {
	who, prefix string
}

func (f fenced) Authorize(_ context.Context, p auth.Principal, a auth.Action, res string) error {
	if p.ID == f.who && a == auth.Write && strings.HasPrefix(res, f.prefix) {
		return errors.New("not under " + f.prefix)
	}
	return nil
}

// The Storage Core Spec: the Authorizer is asked "per branch and per path
// prefix". A write asks for write on every path it changes
// ("path:<branch>:<path>"): bob, kept out of main's secret/, cannot change
// a path there by updating the working set (not by a prev that misstates
// what is stored, either), cannot commit a change there alice staged,
// cannot merge one in, cannot resolve a conflict there, and cannot abandon
// a merge whose abandoning would change one; each refusal is ErrDenied and
// changes nothing. His changes elsewhere go through, and
// alice's go through everywhere.
func TestWritesAreAuthorizedPerPath(t *testing.T) {
	f := newFixture(t)
	o := f.o
	o.Authorizer = fenced{who: bob.ID, prefix: "path:main:secret/"}
	r, err := vcs.Open(ctx, f.s, o)
	if err != nil {
		t.Fatal(err)
	}
	f.r = r
	main := vcs.MainBranch
	f.put(main, "secret/plan", f.obj(8, "base"))
	f.put(main, "public/notes", f.obj(8, "base"))
	f.commit(main, "base")

	edit := func(p auth.Principal, prev vcs.WorkingSet, path string, ref object.Ref) error {
		n, err := f.r.Namespace(ctx, prev.Working)
		if err != nil {
			t.Fatal(err)
		}
		e := n.Editor()
		if err := e.Put(path, ref); err != nil {
			t.Fatal(err)
		}
		if n, err = e.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		next := prev
		next.Working, next.Staged = n.Root(), n.Root()
		_, err = f.r.UpdateWorkingSet(ctx, p, main, prev, next)
		return err
	}
	ws, err := f.r.WorkingSet(ctx, alice, main)
	if err != nil {
		t.Fatal(err)
	}
	if err := edit(bob, ws, "secret/plan", f.obj(8, "bob")); !errors.Is(err, auth.ErrDenied) {
		t.Fatalf("bob changing secret/plan = %v, want ErrDenied", err)
	}
	forged := ws
	if n, err := f.r.Namespace(ctx, ws.Working); err == nil {
		e := n.Editor()
		if err := e.Put("secret/plan", f.obj(8, "bob")); err != nil {
			t.Fatal(err)
		}
		if n, err = e.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		forged.Working, forged.Staged = n.Root(), n.Root() // prev says the change is already there
	}
	if _, err := f.r.UpdateWorkingSet(ctx, bob, main, forged, forged); !errors.Is(err, auth.ErrDenied) {
		t.Fatalf("bob changing secret/plan with a prev that misstates the stored working set = %v, want ErrDenied", err)
	}
	if now, _ := f.r.WorkingSet(ctx, alice, main); now.Hash != ws.Hash {
		t.Fatal("a refused update changed the working set")
	}
	if err := edit(bob, ws, "public/notes", f.obj(8, "bob")); err != nil {
		t.Fatalf("positive control: bob changing public/notes: %v", err)
	}

	ws, _ = f.r.WorkingSet(ctx, alice, main)
	if err := edit(alice, ws, "secret/plan", f.obj(8, "alice")); err != nil {
		t.Fatalf("positive control: alice changing secret/plan: %v", err)
	}
	head := f.head(main)
	if _, err := f.r.CommitWorkingSet(ctx, bob, main, "bob commits alice's change"); !errors.Is(err, auth.ErrDenied) {
		t.Fatalf("bob committing a change to secret/plan alice staged = %v, want ErrDenied", err)
	}
	if got := f.head(main); got.Hash != head.Hash {
		t.Fatal("a refused commit moved the head")
	}
	f.commit(main, "alice commits her change")

	f.branchFrom("dev")
	f.put("dev", "secret/plan", f.obj(8, "dev"))
	theirs := f.commit("dev", "dev work")
	before, _ := f.r.WorkingSet(ctx, alice, main)
	if _, err := f.r.Merge(ctx, bob, main, theirs.Hash); !errors.Is(err, auth.ErrDenied) {
		t.Fatalf("bob merging a change to secret/plan into main = %v, want ErrDenied", err)
	}
	if now, _ := f.r.WorkingSet(ctx, alice, main); now.Hash != before.Hash {
		t.Fatal("a refused merge changed the working set")
	}

	f.put(main, "secret/plan", f.obj(8, "main"))
	f.commit(main, "main work")
	pre, _ := f.r.WorkingSet(ctx, alice, main)
	if res, err := f.r.Merge(ctx, alice, main, theirs.Hash); err != nil || len(res.Conflicts) != 1 {
		t.Fatalf("fixture: alice's merge found %d conflicts (%v), want 1 at secret/plan", len(res.Conflicts), err)
	}
	resolved := f.obj(8, "resolved")
	if err := f.r.ResolveConflict(ctx, bob, main, "secret/plan", &resolved); !errors.Is(err, auth.ErrDenied) {
		t.Fatalf("bob resolving the conflict at secret/plan = %v, want ErrDenied", err)
	}
	if err := f.r.ResolveConflict(ctx, alice, main, "secret/plan", &resolved); err != nil {
		t.Fatalf("positive control: alice resolving it: %v", err)
	}
	merging, _ := f.r.WorkingSet(ctx, alice, main)
	if err := f.r.AbortMerge(ctx, bob, main); !errors.Is(err, auth.ErrDenied) {
		t.Fatalf("bob abandoning a merge, which would change secret/plan back = %v, want ErrDenied", err)
	}
	if now, _ := f.r.WorkingSet(ctx, alice, main); now.Hash != merging.Hash {
		t.Fatal("a refused abort changed the working set")
	}
	if err := f.r.AbortMerge(ctx, alice, main); err != nil {
		t.Fatalf("positive control: alice abandoning it: %v", err)
	}
	if now, _ := f.r.WorkingSet(ctx, alice, main); now.Merge != nil || now.Working != pre.Working {
		t.Fatal("alice's abort did not put back the working set from before the merge")
	}
}

// pathsOnly allows reading anything and writing any path, and nothing more.
type pathsOnly struct{}

func (pathsOnly) Authorize(_ context.Context, _ auth.Principal, a auth.Action, res string) error {
	if a == auth.Read || strings.HasPrefix(res, "path:") {
		return nil
	}
	return errors.New("paths only")
}

// Permission on paths is not permission on the branch: with every path
// granted and the branch not, every write is ErrDenied: an update, a
// commit, a merge, a resolution, and an abort.
func TestAWriteNeedsTheBranchAsWellAsItsPaths(t *testing.T) {
	f := newFixture(t)
	main := vcs.MainBranch
	f.put(main, "doc", f.obj(8, "base"))
	f.commit(main, "base")
	f.branchFrom("dev")
	f.put("dev", "doc", f.obj(8, "dev"))
	theirs := f.commit("dev", "dev work")
	f.put(main, "doc", f.obj(8, "main"))
	f.commit(main, "main work")
	if res, err := f.r.Merge(ctx, alice, main, theirs.Hash); err != nil || len(res.Conflicts) != 1 {
		t.Fatalf("fixture: the merge found %d conflicts (%v), want 1", len(res.Conflicts), err)
	}
	o := f.o
	o.Authorizer = pathsOnly{}
	r, err := vcs.Open(ctx, f.s, o)
	if err != nil {
		t.Fatal(err)
	}
	ws, err := r.WorkingSet(ctx, bob, main)
	if err != nil {
		t.Fatalf("positive control: reading: %v", err)
	}
	resolved := f.obj(8, "resolved")
	for _, c := range []struct {
		name string
		call func() error
	}{
		{"UpdateWorkingSet", func() error { _, err := r.UpdateWorkingSet(ctx, bob, main, ws, ws); return err }},
		{"CommitWorkingSet", func() error { _, err := r.CommitWorkingSet(ctx, bob, "dev", "c"); return err }},
		{"Merge", func() error { _, err := r.Merge(ctx, bob, "dev", theirs.Hash); return err }},
		{"ResolveConflict", func() error { return r.ResolveConflict(ctx, bob, main, "doc", &resolved) }},
		{"AbortMerge", func() error { return r.AbortMerge(ctx, bob, main) }},
	} {
		if err := c.call(); !errors.Is(err, auth.ErrDenied) {
			t.Errorf("%s with the paths granted and the branch not = %v, want ErrDenied", c.name, err)
		}
	}
	if now, _ := r.WorkingSet(ctx, bob, main); now.Hash != ws.Hash {
		t.Fatal("a refused write changed main's working set")
	}
}
