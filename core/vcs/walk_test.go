package vcs_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/memstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/object"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
	"github.com/SmithOperatingSolutions/snapshot-core/core/vcs"
)

// mark walks the repository at s's root as GC marks: everything named is
// kept, and a chunk that reaches others is gone into once.
func mark(s chunk.Store, o vcs.Options) (map[hash.Hash]bool, error) {
	root, err := s.Root(ctx)
	if err != nil {
		return nil, err
	}
	named, gone := map[hash.Hash]bool{}, map[hash.Hash]bool{}
	err = vcs.Walk(ctx, s, o, root, func(h hash.Hash, leaf bool) (bool, error) {
		named[h] = true
		if leaf || gone[h] {
			return false, nil
		}
		gone[h] = true
		return true, nil
	})
	return named, err
}

// keep copies the named chunks, and only those, into an empty store with
// the same root: what GC would leave.
func keep(t *testing.T, s chunk.Store, named map[hash.Hash]bool) *memstore.Store {
	t.Helper()
	k := memstore.New()
	for h := range named {
		b, err := s.Get(ctx, h)
		if err != nil {
			t.Fatalf("Walk named %s, which the store does not hold: %v", h.Short(), err)
		}
		if _, err := k.Put(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	root, err := s.Root(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := k.CompareAndSetRoot(ctx, hash.Hash{}, root); err != nil {
		t.Fatalf("the refs map's root is not among what Walk named: %v", err)
	}
	return k
}

// readEverything reads all a repository holds through its API: every
// branch's whole history and each commit's namespace, every working set and
// its namespaces, a merge's base, theirs and conflicts, and the tags, down
// to each object's content.
func readEverything(t *testing.T, s chunk.Store, o vcs.Options, tags []vcs.Tag) {
	t.Helper()
	r, err := vcs.Open(ctx, s, o)
	if err != nil {
		t.Fatalf("the kept repository does not open: %v", err)
	}
	none, err := object.New(ctx, memstore.New(), o.Config, o.Registry)
	if err != nil {
		t.Fatal(err)
	}
	content := func(what string, h hash.Hash) {
		if _, err := s.Get(ctx, h); err != nil {
			t.Fatalf("%s: an object's content does not read: %v", what, err)
		}
	}
	namespace := func(what string, root hash.Hash) {
		n, err := r.Namespace(ctx, root)
		if err != nil {
			t.Fatalf("%s: the namespace does not open: %v", what, err)
		}
		d, err := objectDiff(none, n)
		if err != nil {
			t.Fatalf("%s: the namespace does not read: %v", what, err)
		}
		for _, c := range d {
			content(what+" "+c.Path, c.To.Root.Hash)
		}
	}
	read := map[hash.Hash]bool{}
	history := func(what string, from hash.Hash) {
		log, err := r.Log(ctx, alice, from, vcs.MaxLog)
		if err != nil {
			t.Fatalf("%s: the history does not read: %v", what, err)
		}
		for _, c := range log {
			if !read[c.Hash] {
				read[c.Hash] = true
				namespace(fmt.Sprintf("%s: commit %q", what, c.Message), c.Namespace)
			}
		}
	}
	branches, err := r.Branches(ctx, alice)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range branches {
		head, err := r.Head(ctx, alice, b)
		if err != nil {
			t.Fatalf("branch %s: %v", b, err)
		}
		history("branch "+b, head.Hash)
		ws, err := r.WorkingSet(ctx, alice, b)
		if err != nil {
			t.Fatalf("branch %s's working set: %v", b, err)
		}
		namespace("branch "+b+" working", ws.Working)
		namespace("branch "+b+" staged", ws.Staged)
		if ws.Merge == nil {
			continue
		}
		history("branch "+b+" merge base", ws.Merge.Base)
		history("branch "+b+" merging", ws.Merge.Theirs)
		cs, err := r.Conflicts(ctx, alice, b)
		if err != nil {
			t.Fatalf("branch %s's conflicts: %v", b, err)
		}
		for _, c := range cs {
			for _, side := range []object.Ref{c.Base, c.Ours, c.Theirs} {
				if side != (object.Ref{}) {
					content("conflict "+c.Path, side.Root.Hash)
				}
			}
		}
	}
	for _, tag := range tags {
		if _, err := s.Get(ctx, tag.Hash); err != nil {
			t.Fatalf("tag %q does not read: %v", tag.Message, err)
		}
		history("tag "+tag.Message, tag.Target)
	}
}

func objectDiff(from, to *object.Namespace) ([]object.Change, error) {
	d, err := object.Diff(ctx, from, to)
	if err != nil {
		return nil, err
	}
	var out []object.Change
	for {
		c, ok, err := d.Next()
		if err != nil || !ok {
			return out, err
		}
		out = append(out, c)
	}
}

// GC keeps what the version graph's Walk names (DESIGN §9), so it must name
// all a repository holds: kept alone, those chunks read back as the whole
// repository, every branch's history, working sets, a merge in progress
// (whose theirs only the merge holds, its branch deleted, and whose ours
// only the conflict holds, rewritten since) with an edit made during it,
// and a tag of an old commit. And it must not name what nothing holds any
// longer: a working set replaced, a deleted branch's commit and object, an
// old refs root.
func TestAWalkNamesAllTheRepositoryHolds(t *testing.T) {
	f := newFixture(t)
	main := vcs.MainBranch
	ws0, err := f.r.WorkingSet(ctx, alice, main)
	if err != nil {
		t.Fatal(err)
	}
	f.put(main, "doc", f.obj(8, "base"))
	f.put(main, "notes", f.obj(7, "a"))
	base := f.commit(main, "base")
	for i := range 5 {
		f.put(main, fmt.Sprintf("more/%d", i), f.obj(7, fmt.Sprint("more ", i)))
		f.commit(main, fmt.Sprint("more ", i))
	}
	f.branchFrom("dev")
	f.put("dev", "doc", f.obj(8, "dev"))
	theirs := f.commit("dev", "dev work")
	tag, err := f.r.CreateTag(ctx, alice, "v1", base.Hash, "the base")
	if err != nil {
		t.Fatal(err)
	}
	f.put(main, "doc", f.obj(8, "main"))
	f.commit(main, "main work")
	f.put(main, "doc", f.obj(8, "main, not committed")) // the merge's ours
	if r, err := f.r.Merge(ctx, alice, main, theirs.Hash); err != nil || len(r.Conflicts) != 1 {
		t.Fatalf("fixture: the merge found %d conflicts (%v), want 1", len(r.Conflicts), err)
	}
	f.put(main, "doc", f.obj(8, "rewritten during the merge")) // now only the conflict holds ours
	f.put(main, "during", f.obj(7, "written during the merge"))
	if err := f.r.DeleteBranch(ctx, alice, "dev"); err != nil { // now only the merge holds theirs
		t.Fatal(err)
	}
	f.branchFrom("gone")
	goneObj := f.obj(7, "only on gone")
	f.put("gone", "only-here", goneObj)
	gone := f.commit("gone", "gone")
	before, err := f.s.Root(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.r.DeleteBranch(ctx, alice, "gone"); err != nil {
		t.Fatal(err)
	}

	named, err := mark(f.s, f.o)
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	readEverything(t, keep(t, f.s, named), f.o, []vcs.Tag{tag})
	for what, h := range map[string]hash.Hash{
		"the working set its first edit replaced": ws0.Hash,
		"the deleted branch's commit":             gone.Hash,
		"the object only the deleted branch held": goneObj.Root.Hash,
		"the refs root before the deletion":       before,
	} {
		if named[h] {
			t.Errorf("Walk named %s, which nothing holds any longer", what)
		}
	}
}

// Walk refuses, rather than name too little: a ref the version graph does
// not write, a ref that is not a hash, a head that is no commit, a tag that
// is no tag, a working set that is none, and a namespace holding an object
// whose model cannot walk.
func TestWalkRefusesWhatItCannotName(t *testing.T) {
	main := vcs.MainBranch
	for _, c := range []struct {
		name string
		edit func(f *fixture, e *prolly.Editor, head, ws hash.Hash) error
	}{
		{"a ref of another kind", func(_ *fixture, e *prolly.Editor, head, _ hash.Hash) error { return e.Put([]byte("other/x"), head[:]) }},
		{"a head of 33 bytes", func(_ *fixture, e *prolly.Editor, head, _ hash.Hash) error {
			return e.Put([]byte("heads/"+main), append(head[:], 0))
		}},
		{"a head naming a working set", func(_ *fixture, e *prolly.Editor, _, ws hash.Hash) error { return e.Put([]byte("heads/"+main), ws[:]) }},
		{"a tag naming a commit", func(_ *fixture, e *prolly.Editor, head, _ hash.Hash) error { return e.Put([]byte("tags/v1"), head[:]) }},
		{"a working set naming a commit", func(_ *fixture, e *prolly.Editor, head, _ hash.Hash) error {
			return e.Put([]byte("work/"+main), head[:])
		}},
	} {
		f := newFixture(t)
		if _, err := mark(f.s, f.o); err != nil {
			t.Fatalf("%s: positive control: %v", c.name, err)
		}
		ws, err := f.r.WorkingSet(ctx, alice, main)
		if err != nil {
			t.Fatal(err)
		}
		root, err := f.s.Root(ctx)
		if err != nil {
			t.Fatal(err)
		}
		m, err := prolly.Open(ctx, f.s, f.o.Config, root)
		if err != nil {
			t.Fatal(err)
		}
		e := m.Editor()
		if err := c.edit(f, e, f.head(main).Hash, ws.Hash); err != nil {
			t.Fatal(err)
		}
		forged, err := e.Flush(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.s.CompareAndSetRoot(ctx, root, forged.Root()); err != nil {
			t.Fatal(err)
		}
		if _, err := mark(f.s, f.o); !errors.Is(err, chunk.ErrCorrupt) {
			t.Errorf("%s: Walk = %v, want ErrCorrupt", c.name, err)
		}
	}
	f := newFixture(t)
	f.put(main, "x", f.obj(1, "an object of a model that cannot walk"))
	f.commit(main, "unwalkable")
	if _, err := mark(f.s, f.o); !errors.Is(err, object.ErrNotWalkable) {
		t.Errorf("a repository holding an object whose model cannot walk: Walk = %v, want ErrNotWalkable", err)
	}
}

// A failed read is never less to keep: every read Walk makes, failed in
// turn, is its error.
func TestWalkSurfacesStoreErrors(t *testing.T) {
	s := &faulty{Store: memstore.New()}
	f := newFixtureOn(t, s)
	f.put(vcs.MainBranch, "doc", f.obj(8, "base"))
	f.commit(vcs.MainBranch, "base")
	f.branchFrom("dev")
	f.put("dev", "doc", f.obj(8, "dev"))
	theirs := f.commit("dev", "dev")
	f.put(vcs.MainBranch, "doc", f.obj(8, "main"))
	f.commit(vcs.MainBranch, "main")
	if _, err := f.r.CreateTag(ctx, alice, "v1", theirs.Hash, "t"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.r.Merge(ctx, alice, vcs.MainBranch, theirs.Hash); err != nil {
		t.Fatal(err)
	}
	s.arm(0, 0)
	if _, err := mark(s, f.o); err != nil {
		t.Fatal(err)
	}
	reads := s.calls[callGet].Load()
	if reads < 10 {
		t.Fatalf("fixture: Walk read %d chunks, want many", reads)
	}
	for n := int64(1); n <= reads; n++ {
		s.arm(callGet, n)
		_, err := mark(s, f.o)
		s.arm(0, 0)
		if !errors.Is(err, errInjected) {
			t.Fatalf("with read %d of %d failing, Walk = %v, want the store's error", n, reads, err)
		}
	}
}
