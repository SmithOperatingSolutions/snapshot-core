package vcs_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
	"github.com/SmithOperatingSolutions/snapshot-core/core/vcs"
)

// A working set names namespaces that are stored, and a branch or a tag
// names a stored commit. Anything else is refused, and nothing changes.
func TestRefsNameOnlyStoredObjects(t *testing.T) {
	f := newFixture(t)
	main := vcs.MainBranch
	head := f.head(main)
	ws, err := f.r.WorkingSet(ctx, alice, main)
	if err != nil {
		t.Fatal(err)
	}
	missing := hash.Sum([]byte("never stored"))
	for _, c := range []struct {
		name string
		next vcs.WorkingSet
	}{
		{"working", vcs.WorkingSet{Working: missing, Staged: ws.Staged}},
		{"staged", vcs.WorkingSet{Working: ws.Working, Staged: missing}},
	} {
		if _, err := f.r.UpdateWorkingSet(ctx, alice, main, ws, c.next); err == nil {
			t.Errorf("a working set whose %s namespace is not stored was accepted", c.name)
		}
	}
	if now, _ := f.r.WorkingSet(ctx, alice, main); now.Hash != ws.Hash {
		t.Fatal("a refused working set changed the branch's working set")
	}
	for _, c := range []struct {
		name string
		at   hash.Hash
		want error
	}{
		{"a hash never stored", missing, chunk.ErrNotFound},
		{"a working set", ws.Hash, chunk.ErrCorrupt},
	} {
		if err := f.r.CreateBranch(ctx, alice, "dev", c.at); !errors.Is(err, c.want) {
			t.Errorf("a branch at %s = %v, want %v", c.name, err, c.want)
		}
		if _, err := f.r.CreateTag(ctx, alice, "v1", c.at, "release"); !errors.Is(err, c.want) {
			t.Errorf("a tag of %s = %v, want %v", c.name, err, c.want)
		}
	}
	if bs, err := f.r.Branches(ctx, alice); err != nil || len(bs) != 1 {
		t.Fatalf("after the refused branches, Branches = %v (%v), want [main]", bs, err)
	}
	if err := f.r.CreateBranch(ctx, alice, "dev", head.Hash); err != nil {
		t.Fatalf("positive control: a branch at the head: %v", err)
	}
	if _, err := f.r.CreateTag(ctx, alice, "v1", head.Hash, "release"); err != nil {
		t.Fatalf("positive control: a tag of the head: %v", err)
	}
}

// Commit and tag messages are UTF-8 of at most 64 KiB. Others are refused,
// and nothing is committed or tagged.
func TestMessagesAreBoundedUTF8(t *testing.T) {
	f := newFixture(t)
	main := vcs.MainBranch
	head := f.head(main)
	for _, c := range []struct{ name, msg string }{
		{"over 64 KiB", strings.Repeat("m", vcs.MaxMessageLen+1)},
		{"not UTF-8", "caf\xe9"},
	} {
		if _, err := f.r.CommitWorkingSet(ctx, alice, main, c.msg); err == nil {
			t.Errorf("a commit message %s was accepted", c.name)
		}
		if _, err := f.r.CreateTag(ctx, alice, "v1", head.Hash, c.msg); err == nil {
			t.Errorf("a tag message %s was accepted", c.name)
		}
	}
	if got := f.head(main); got.Hash != head.Hash {
		t.Fatal("a refused commit moved the head")
	}
	longest := strings.Repeat("é", vcs.MaxMessageLen/2)
	if c, err := f.r.CommitWorkingSet(ctx, alice, main, longest); err != nil || c.Message != longest {
		t.Fatalf("positive control: a commit message of exactly 64 KiB: %v", err)
	}
	if back, err := f.r.Log(ctx, alice, f.head(main).Hash, 1); err != nil || back[0].Message != longest {
		t.Fatalf("positive control: reading back the 64 KiB message: %v", err)
	}
	if _, err := f.r.CreateTag(ctx, alice, "v1", head.Hash, longest); err != nil {
		t.Fatalf("positive control: a tag message of exactly 64 KiB: %v", err)
	}
}

// A branch that does not exist is ErrBranchNotFound to every call that
// names one, and none of them creates it.
func TestMissingBranchesAreNotFound(t *testing.T) {
	f := newFixture(t)
	head := f.head(vcs.MainBranch)
	ws, err := f.r.WorkingSet(ctx, alice, vcs.MainBranch)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name string
		call func() error
	}{
		{"DeleteBranch", func() error { return f.r.DeleteBranch(ctx, alice, "nope") }},
		{"Checkout", func() error {
			sess, err := f.r.Checkout(ctx, alice, "nope")
			if err == nil {
				sess.Close()
			}
			return err
		}},
		{"CommitWorkingSet", func() error { _, err := f.r.CommitWorkingSet(ctx, alice, "nope", "m"); return err }},
		{"UpdateWorkingSet", func() error { _, err := f.r.UpdateWorkingSet(ctx, alice, "nope", ws, ws); return err }},
		{"Merge", func() error { _, err := f.r.Merge(ctx, alice, "nope", head.Hash); return err }},
		{"Conflicts", func() error { _, err := f.r.Conflicts(ctx, alice, "nope"); return err }},
		{"ResolveConflict", func() error { return f.r.ResolveConflict(ctx, alice, "nope", "doc", nil) }},
	} {
		if err := c.call(); !errors.Is(err, vcs.ErrBranchNotFound) {
			t.Errorf("%s of a missing branch = %v, want ErrBranchNotFound", c.name, err)
		}
	}
	if bs, err := f.r.Branches(ctx, alice); err != nil || len(bs) != 1 || bs[0] != vcs.MainBranch {
		t.Fatalf("Branches = %v (%v), want [main]", bs, err)
	}
}

// Merges out of turn are refused and change nothing: a commit that shares
// no history with the branch, a second merge while one is in progress,
// and resolving where no merge is in progress or at a path with no
// conflict. With no merge in progress there are no conflicts to list.
// Resolving by deleting removes the path from both namespaces, and only
// that path.
func TestMergesOutOfTurnAreRefused(t *testing.T) {
	f := newFixture(t)
	main := vcs.MainBranch
	stranger := writeCommit(t, f.s, nil, 0, "a root of its own")
	if _, err := f.r.MergeBase(ctx, alice, f.head(main).Hash, stranger); err == nil {
		t.Error("MergeBase of two unrelated histories succeeded")
	}
	before, err := f.r.WorkingSet(ctx, alice, main)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.r.Merge(ctx, alice, main, stranger); err == nil {
		t.Error("merging an unrelated history succeeded")
	}
	if cs, err := f.r.Conflicts(ctx, alice, main); err != nil || len(cs) != 0 {
		t.Errorf("Conflicts with no merge in progress = %v, %v; want none", cs, err)
	}
	if err := f.r.ResolveConflict(ctx, alice, main, "doc", nil); err == nil {
		t.Error("resolving with no merge in progress succeeded")
	}
	if now, _ := f.r.WorkingSet(ctx, alice, main); now.Hash != before.Hash {
		t.Fatal("a refused merge or resolution changed the working set")
	}

	f.put(main, "doc", f.obj(8, "base"))
	f.put(main, "keep", f.obj(7, "k"))
	f.commit(main, "base")
	f.branchFrom("dev")
	f.branchFrom("other")
	f.put("dev", "doc", f.obj(8, "dev"))
	theirs := f.commit("dev", "dev work")
	f.put("other", "more", f.obj(7, "o"))
	other := f.commit("other", "other work")
	f.put(main, "doc", f.obj(8, "main"))
	f.commit(main, "main work")
	if r, err := f.r.Merge(ctx, alice, main, theirs.Hash); err != nil || len(r.Conflicts) != 1 {
		t.Fatalf("fixture: the merge found %d conflicts (%v), want 1", len(r.Conflicts), err)
	}
	during, err := f.r.WorkingSet(ctx, alice, main)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.r.Merge(ctx, alice, main, other.Hash); err == nil {
		t.Error("a second merge while one is in progress succeeded")
	}
	if err := f.r.ResolveConflict(ctx, alice, main, "keep", nil); err == nil {
		t.Error("resolving a path with no conflict succeeded")
	}
	if now, _ := f.r.WorkingSet(ctx, alice, main); now.Hash != during.Hash {
		t.Fatal("a refused merge or resolution changed the working set")
	}

	if err := f.r.ResolveConflict(ctx, alice, main, "doc", nil); err != nil {
		t.Fatal(err)
	}
	ws, err := f.r.WorkingSet(ctx, alice, main)
	if err != nil {
		t.Fatal(err)
	}
	for _, ns := range []struct {
		name string
		root hash.Hash
	}{{"working", ws.Working}, {"staged", ws.Staged}} {
		if _, ok := f.get(ns.root, "doc"); ok {
			t.Errorf("resolved by deleting, doc is still in the %s namespace", ns.name)
		}
		if _, ok := f.get(ns.root, "keep"); !ok {
			t.Errorf("resolving doc lost keep from the %s namespace", ns.name)
		}
	}
	if cs, err := f.r.Conflicts(ctx, alice, main); err != nil || len(cs) != 0 {
		t.Fatalf("after resolving the only conflict, Conflicts = %v, %v; want none", cs, err)
	}
}

// A tag ref names a tag chunk by its 32-byte hash. One that names a commit,
// or holds 33 bytes, reads as corrupt, never as a tag (issue #2).
func TestForgedTagRefsAreCorrupt(t *testing.T) {
	for _, c := range []struct {
		name  string
		value func(head, tag hash.Hash) []byte
	}{
		{"a tag naming a commit", func(head, _ hash.Hash) []byte { return head[:] }},
		{"a tag of 33 bytes", func(_, tag hash.Hash) []byte { return append(tag[:], 0) }},
	} {
		f := newFixture(t)
		head := f.head(vcs.MainBranch).Hash
		tag, err := f.r.CreateTag(ctx, alice, "v1", head, "t")
		if err != nil {
			t.Fatal(err)
		}
		if got, err := f.r.Tag(ctx, alice, "v1"); err != nil || got.Hash != tag.Hash {
			t.Fatalf("%s: positive control: Tag = %+v, %v", c.name, got, err)
		}
		f.forge("tags/v1", c.value(head, tag.Hash))
		if got, err := f.r.Tag(ctx, alice, "v1"); !errors.Is(err, chunk.ErrCorrupt) {
			t.Errorf("%s: Tag = %+v, %v; want ErrCorrupt", c.name, got, err)
		}
	}
}

// forge sets a ref to raw bytes behind the version graph's back.
func (f *fixture) forge(key string, value []byte) {
	f.t.Helper()
	root, err := f.s.Root(ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	m, err := prolly.Open(ctx, f.s, f.o.Config, root)
	if err != nil {
		f.t.Fatal(err)
	}
	e := m.Editor()
	if err := e.Put([]byte(key), value); err != nil {
		f.t.Fatal(err)
	}
	forged, err := e.Flush(ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	if err := f.s.CompareAndSetRoot(ctx, root, forged.Root()); err != nil {
		f.t.Fatal(err)
	}
}
