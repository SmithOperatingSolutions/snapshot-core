package gc_test

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/SmithOperatingSolutions/snapshot-core/core/object"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
	"github.com/SmithOperatingSolutions/snapshot-core/core/vcs"
)

// history is a repository driven the way a host following the contract
// drives one (DESIGN §9): every operation reads afresh and publishes at
// once, and on ErrConflict reads again and does it again.
type history struct {
	w        *world
	branches []string
	tags     []vcs.Tag
	names    int
	deleted  int
	merges   int // merges in progress read back after a collection
}

var (
	paths    = []string{"p0", "p1", "p2", "p3", "d/p4", "d/p5"}
	contents = []string{"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7"} // few, so bytes come back after GC condemns them
)

// retry runs op until it is not a conflict.
func (h *history) retry(what string, op func() error) error {
	for range 10 {
		if err := op(); !errors.Is(err, vcs.ErrConflict) {
			return err
		}
	}
	h.w.t.Fatalf("%s kept conflicting", what)
	return nil
}

func (h *history) edit(branch string, f func(e *object.Editor) error) {
	err := h.retry("an edit", func() error {
		ws, err := h.w.r.WorkingSet(ctx, alice, branch)
		if err != nil {
			return err
		}
		n, err := h.w.r.Namespace(ctx, ws.Working)
		if err != nil {
			return err
		}
		e := n.Editor()
		if err := f(e); err != nil {
			return err
		}
		if n, err = e.Flush(ctx); err != nil {
			return err
		}
		next := ws
		next.Working, next.Staged = n.Root(), n.Root()
		_, err = h.w.r.UpdateWorkingSet(ctx, alice, branch, ws, next)
		return err
	})
	if err != nil {
		h.w.t.Fatalf("editing %s: %v", branch, err)
	}
}

func (h *history) head(branch string) vcs.Commit {
	c, err := h.w.r.Head(ctx, alice, branch)
	if err != nil {
		h.w.t.Fatal(err)
	}
	return c
}

func (h *history) step(rt *rapid.T) {
	b := rapid.SampledFrom(h.branches).Draw(rt, "branch")
	switch rapid.IntRange(0, 12).Draw(rt, "op") {
	case 0, 1, 2:
		path, content := rapid.SampledFrom(paths).Draw(rt, "path"), rapid.SampledFrom(contents).Draw(rt, "content")
		h.edit(b, func(e *object.Editor) error { return e.Put(path, h.w.note(7, content)) })
	case 3:
		path := rapid.SampledFrom(paths).Draw(rt, "path")
		h.edit(b, func(e *object.Editor) error { return e.Delete(path) })
	case 4:
		err := h.retry("a commit", func() error { _, err := h.w.r.CommitWorkingSet(ctx, alice, b, "c"); return err })
		if err != nil && !errors.Is(err, vcs.ErrUnresolvedConflicts) {
			rt.Fatalf("committing %s: %v", b, err)
		}
	case 5:
		h.names++
		name := fmt.Sprintf("b%d", h.names)
		at := h.head(b).Hash
		if err := h.retry("a branch", func() error { return h.w.r.CreateBranch(ctx, alice, name, at) }); err != nil {
			rt.Fatal(err)
		}
		h.branches = append(h.branches, name)
	case 6:
		if b == vcs.MainBranch {
			return
		}
		if err := h.retry("a branch deletion", func() error { return h.w.r.DeleteBranch(ctx, alice, b) }); err != nil {
			rt.Fatal(err)
		}
		for i, x := range h.branches {
			if x == b {
				h.branches = append(h.branches[:i], h.branches[i+1:]...)
				break
			}
		}
	case 7:
		src := b
		if others := h.others(b); len(others) > 0 {
			src = rapid.SampledFrom(others).Draw(rt, "merged")
		}
		theirs := h.head(src).Hash
		// A second merge while one is in progress is refused; that is fine here.
		_ = h.retry("a merge", func() error { _, err := h.w.r.Merge(ctx, alice, b, theirs); return err })
	case 8:
		cs, err := h.w.r.Conflicts(ctx, alice, b)
		if err != nil {
			rt.Fatal(err)
		}
		for _, c := range cs {
			var to *object.Ref
			if c.Ours != (object.Ref{}) {
				ours := c.Ours
				to = &ours
			}
			if err := h.retry("a resolution", func() error { return h.w.r.ResolveConflict(ctx, alice, b, c.Path, to) }); err != nil {
				rt.Fatal(err)
			}
		}
	case 9:
		h.names++
		name, at := fmt.Sprintf("t%d", h.names), h.head(b).Hash
		var tag vcs.Tag
		err := h.retry("a tag", func() error {
			var err error
			tag, err = h.w.r.CreateTag(ctx, alice, name, at, "t")
			return err
		})
		if err != nil {
			rt.Fatal(err)
		}
		h.tags = append(h.tags, tag)
	case 10:
		h.w.jump += rapid.SampledFrom([]time.Duration{0, grace / 2, grace + time.Minute}).Draw(rt, "wait")
		h.collect()
	case 12:
		h.diverge(rt, b)
	case 11:
		// An edit that spans two collections a grace window apart: what its
		// put counted on may be condemned and expire before it publishes.
		path, content := rapid.SampledFrom(paths).Draw(rt, "path"), rapid.SampledFrom(contents).Draw(rt, "content")
		first := true
		h.edit(b, func(e *object.Editor) error {
			ref := h.w.note(7, content)
			if first {
				first = false
				for range 2 {
					h.w.jump += grace + time.Minute
					h.collect()
				}
			}
			return e.Put(path, ref)
		})
	}
}

// diverge branches off b, puts different contents at one path on each
// side, commits both, and merges the new branch into b: a merge left in
// conflict, unless b has one in progress already.
func (h *history) diverge(rt *rapid.T, b string) {
	path := rapid.SampledFrom(paths).Draw(rt, "path")
	ours := rapid.SampledFrom(contents).Draw(rt, "ours")
	theirs := rapid.SampledFrom(contents).Filter(func(c string) bool { return c != ours }).Draw(rt, "theirs")
	h.names++
	side := fmt.Sprintf("b%d", h.names)
	at := h.head(b).Hash
	if err := h.retry("a branch", func() error { return h.w.r.CreateBranch(ctx, alice, side, at) }); err != nil {
		rt.Fatal(err)
	}
	h.branches = append(h.branches, side)
	for _, s := range []struct{ branch, content string }{{side, theirs}, {b, ours}} {
		h.edit(s.branch, func(e *object.Editor) error { return e.Put(path, h.w.note(7, s.content)) })
		err := h.retry("a commit", func() error { _, err := h.w.r.CommitWorkingSet(ctx, alice, s.branch, "c"); return err })
		if errors.Is(err, vcs.ErrUnresolvedConflicts) {
			return
		}
		if err != nil {
			rt.Fatal(err)
		}
	}
	merged := h.head(side).Hash
	_ = h.retry("a merge", func() error { _, err := h.w.r.Merge(ctx, alice, b, merged); return err })
}

func (h *history) others(b string) []string {
	var out []string
	for _, x := range h.branches {
		if x != b {
			out = append(out, x)
		}
	}
	return out
}

// collect runs GC and checks the whole repository still reads.
func (h *history) collect() gcReport {
	rep, err := h.w.gc()
	if err != nil {
		h.w.t.Fatalf("GC: %v", err)
	}
	h.deleted += len(rep.Deleted)
	h.readsWhole()
	return gcReport{condemned: rep.Condemned, deleted: len(rep.Deleted)}
}

type gcReport struct{ condemned, deleted int }

// readsWhole reads, from a fresh store, everything the repository holds
// through its API: every branch's history and namespaces, working sets,
// merges in progress with their base, theirs and conflicts, and tags,
// down to every object's content.
func (h *history) readsWhole() {
	w := h.w
	s := w.store()
	r, err := vcs.Open(ctx, s, w.vcs())
	if err != nil {
		w.t.Fatalf("after GC the repository does not open: %v", err)
	}
	none, err := object.New(ctx, w.scratch(), prolly.DefaultConfig(), w.reg)
	if err != nil {
		w.t.Fatal(err)
	}
	content := func(what string, ref object.Ref) {
		if _, err := s.Get(ctx, ref.Root.Hash); err != nil {
			w.t.Fatalf("after GC %s does not read: %v", what, err)
		}
	}
	namespace := func(what string, root [32]byte) {
		n, err := r.Namespace(ctx, root)
		if err != nil {
			w.t.Fatalf("after GC %s does not open: %v", what, err)
		}
		d, err := object.Diff(ctx, none, n)
		if err != nil {
			w.t.Fatal(err)
		}
		for {
			c, ok, err := d.Next()
			if err != nil {
				w.t.Fatalf("after GC %s does not read: %v", what, err)
			}
			if !ok {
				return
			}
			content(what+" "+c.Path, c.To)
		}
	}
	read := map[[32]byte]bool{}
	history := func(what string, from [32]byte) {
		log, err := r.Log(ctx, alice, from, vcs.MaxLog)
		if err != nil {
			w.t.Fatalf("after GC %s's history does not read: %v", what, err)
		}
		for _, c := range log {
			if !read[c.Hash] {
				read[c.Hash] = true
				namespace(what+" commit", c.Namespace)
			}
		}
	}
	for _, b := range h.branches {
		head, err := r.Head(ctx, alice, b)
		if err != nil {
			w.t.Fatalf("after GC branch %s: %v", b, err)
		}
		history("branch "+b, head.Hash)
		ws, err := r.WorkingSet(ctx, alice, b)
		if err != nil {
			w.t.Fatalf("after GC branch %s's working set: %v", b, err)
		}
		namespace("branch "+b+" working", ws.Working)
		namespace("branch "+b+" staged", ws.Staged)
		if ws.Merge == nil {
			continue
		}
		h.merges++
		history("branch "+b+"'s merge base", ws.Merge.Base)
		history("branch "+b+"'s merge", ws.Merge.Theirs)
		cs, err := r.Conflicts(ctx, alice, b)
		if err != nil {
			w.t.Fatalf("after GC branch %s's conflicts: %v", b, err)
		}
		for _, c := range cs {
			for _, side := range []object.Ref{c.Base, c.Ours, c.Theirs} {
				if side != (object.Ref{}) {
					content("conflict "+c.Path, side)
				}
			}
		}
	}
	for _, t := range h.tags {
		if _, err := s.Get(ctx, t.Hash); err != nil {
			w.t.Fatalf("after GC tag %s does not read: %v", t.Hash.Short(), err)
		}
		history("tag", t.Target)
	}
}

// The Storage Core Spec's GC property, C4's exit criterion: over random
// histories (edits whose bytes come back after GC condemned them, edits
// that span collections and are fenced, commits, branches made and
// deleted, merges left in conflict and resolved, tags) with GC run between
// them as the clock moves on by nothing, half a grace window or more than
// one, GC never deletes a chunk any ref or working set reaches: after every
// run the whole repository reads. And it does delete what nothing reaches:
// once grace windows pass with no writer, a run finds nothing left to
// condemn and nothing to delete. The run proves its reach: GC deleted
// something in most histories, and merges in progress were read back.
func TestGCSafetyProperty(t *testing.T) {
	var cases, collected, merges atomic.Int64
	rapid.Check(t, func(rt *rapid.T) {
		cases.Add(1)
		w := worldFor(rt)
		defer w.close()
		h := &history{w: w, branches: []string{vcs.MainBranch}}
		for range rapid.IntRange(5, 40).Draw(rt, "steps") {
			h.step(rt)
		}
		for range 3 {
			w.jump += grace + time.Minute
			h.collect()
		}
		w.jump += grace + time.Minute
		if last := h.collect(); last.condemned != 0 || last.deleted != 0 {
			rt.Fatalf("grace windows after the last write, GC still condemned %d packs and deleted %d objects", last.condemned, last.deleted)
		}
		if h.deleted > 0 {
			collected.Add(1)
		}
		merges.Add(int64(h.merges))
	})
	if n := cases.Load(); collected.Load()*2 < n {
		t.Fatalf("GC deleted something in %d of %d histories: the property did not reach collection", collected.Load(), n)
	}
	if merges.Load() == 0 {
		t.Fatal("no merge in progress was ever read back after a collection: the property did not reach merges")
	}
}
