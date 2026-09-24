package gc_test

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/gc"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/object"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
	"github.com/SmithOperatingSolutions/snapshot-core/core/vcs"
)

// history is a repository driven the way a host following the contract
// drives one (DESIGN §9): every operation reads afresh and publishes at
// once, on ErrConflict reads again and does it again, and when its session
// is lost (ErrSessionLost) reopens the repository and does it again.
type history struct {
	w        *world
	branches []string
	tags     []vcs.Tag
	names    int
	deleted  int
	merges   int      // merges in progress read back after a collection
	lost     int      // sessions lost to GC and reopened
	slow     int      // slow writers so far
	steps    int      // steps taken
	collects int      // collections run
	log      []string // what each collection reported
	repacked int      // packs repacked by collections
}

var (
	paths    = []string{"p0", "p1", "p2", "p3", "d/p4", "d/p5"}
	contents = []string{"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7"} // few, so bytes come back after GC condemns them
)

// retry runs op until it is neither a conflict nor a lost session.
func (h *history) retry(what string, op func() error) error {
	for range 10 {
		err := op()
		if errors.Is(err, vcs.ErrSessionLost) {
			h.lost++
			h.w.reopen()
			continue
		}
		if !errors.Is(err, vcs.ErrConflict) {
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
	op := rapid.IntRange(0, 13).Draw(rt, "op")
	h.steps++
	h.w.clock.during(fmt.Sprintf("step %d, op %d on %s", h.steps, op, b))
	switch op {
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
		h.w.addJump(rapid.SampledFrom([]time.Duration{0, grace / 2, grace + time.Minute}).Draw(rt, "wait"))
		h.collect()
	case 12:
		h.diverge(rt, b)
	case 13:
		// A slow writer: the pack it uploads sits unpublished past the grace
		// window, GC deletes it as an orphan, and the publish loses the
		// session; the host reopens and edits again.
		path := rapid.SampledFrom(paths).Draw(rt, "path")
		h.slow++
		note := incompressible(int64(h.slow), 1500)
		first := true
		h.edit(b, func(e *object.Editor) error {
			packs := count(rt, h.w.blobs, "packs/")
			ref := h.w.note(7, note)
			for i := range 2 { // the second fills the pack holding the note: uploaded, not published
				h.w.note(7, incompressible(int64(h.slow)<<8|int64(i), 1500))
			}
			if first {
				first = false
				waitForPacks(rt, h.w.blobs, packs+1) // the upload lands beside the writer
				h.w.addJump(grace + time.Minute)
				h.collect()
			}
			return e.Put(path, ref)
		})
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
					h.w.addJump(grace + time.Minute)
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
	h.collects++
	h.w.clock.during(fmt.Sprintf("collection %d", h.collects))
	rep, err := h.w.gc()
	if err != nil {
		h.w.t.Fatalf("GC: %v", err)
	}
	h.deleted += len(rep.Deleted)
	h.repacked += rep.Repacked
	h.log = append(h.log, fmt.Sprintf("collection %d: %d rounds, %d live, %d condemned, %d reprieved, %d repacked, deleted %v", h.collects, rep.Rounds, rep.Live, rep.Condemned, rep.Reprieved, rep.Repacked, rep.Deleted))
	h.w.clock.during(fmt.Sprintf("reading everything after collection %d", h.collects))
	h.readsWhole()
	written := make([]string, 0, len(rep.Deleted))
	for _, name := range rep.Deleted {
		written = append(written, h.w.clock.written(name))
	}
	return gcReport{condemned: rep.Condemned, reprieved: rep.Reprieved, repacked: rep.Repacked, deleted: written}
}

type gcReport struct {
	condemned, reprieved, repacked int
	deleted                        []string
}

// quiet is a collection that found nothing to do: nothing to condemn,
// reprieve, repack or delete, so nothing for the next window to finish.
func (r gcReport) quiet() bool {
	return r.condemned == 0 && r.reprieved == 0 && r.repacked == 0 && len(r.deleted) == 0
}

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
// once grace windows pass with no writer (at most maxQuietWindows), a run
// finds nothing left to condemn and nothing to delete, so does the next,
// and what the store holds has converged on
// what is reachable: every pack at least half live, so the packs' bytes are
// at most twice the live bytes and a pack's overhead each (#1). The run
// proves its reach: GC deleted something in most histories, merges in
// progress were read back, slow writers (and fenced edits) lost their
// sessions and did their work again on a reopened repository, and packs
// were repacked.
// maxQuietWindows bounds the grace windows GC gets, with no writer, to
// find nothing left to condemn or delete: a repack's chain of expiries is
// four windows long, and no history should start two.
const maxQuietWindows = 8

func TestGCSafetyProperty(t *testing.T) {
	var cases, collected, merges, lost, repacked atomic.Int64
	rapid.Check(t, func(rt *rapid.T) {
		cases.Add(1)
		w := worldFor(rt)
		defer w.close()
		h := &history{w: w, branches: []string{vcs.MainBranch}}
		for range rapid.IntRange(5, 40).Draw(rt, "steps") {
			h.step(rt)
		}
		// With no writer, each grace window lets GC finish what the last one
		// started: a condemned pack expires, and the index objects rewritten
		// over it expire a window later; a repack may make a pack holding the
		// same chunks as the fresh one dead, and that pack takes the same
		// road. Two chains of that length, and GC has run out of things to
		// do: a collection then finds nothing, and so does the next.
		quiet := 0
		for ; quiet < maxQuietWindows; quiet++ {
			w.addJump(grace + time.Minute)
			if h.collect().quiet() {
				break
			}
		}
		if quiet == maxQuietWindows {
			rt.Fatalf("%d grace windows after the last write, every collection still condemned, reprieved, repacked or deleted something: GC does not converge\n%s", maxQuietWindows, strings.Join(h.log[max(0, len(h.log)-maxQuietWindows):], "\n"))
		}
		w.addJump(grace + time.Minute)
		if last := h.collect(); !last.quiet() {
			rt.Fatalf("a collection that found nothing to do was followed by one that condemned %d packs, reprieved %d, repacked %d and deleted %d objects: %v\n%s", last.condemned, last.reprieved, last.repacked, len(last.deleted), last.deleted, strings.Join(h.log[max(0, len(h.log)-6):], "\n"))
		}
		h.converged(rt)
		if h.deleted > 0 {
			collected.Add(1)
		}
		merges.Add(int64(h.merges))
		lost.Add(int64(h.lost))
		repacked.Add(int64(h.repacked))
	})
	if n := cases.Load(); collected.Load()*2 < n {
		t.Fatalf("GC deleted something in %d of %d histories: the property did not reach collection", collected.Load(), n)
	}
	if merges.Load() == 0 {
		t.Fatal("no merge in progress was ever read back after a collection: the property did not reach merges")
	}
	if lost.Load() == 0 {
		t.Fatal("no history lost a session to GC and reopened: the property did not reach a lost session")
	}
	if repacked.Load() == 0 {
		t.Fatal("no history repacked a pack: the property did not reach repacking")
	}
}

// converged checks that, with nothing left to collect, the packs' bytes are
// at most twice what the repository reaches plus a pack's overhead each:
// no kept pack is less than half live.
func (h *history) converged(rt *rapid.T) {
	w := h.w
	s := w.store()
	root, err := s.Root(ctx)
	if err != nil {
		rt.Fatal(err)
	}
	live, closeLive, err := gc.Mark(ctx, s, gc.Options{Blobs: w.blobs, Keys: w.keys, Repo: repo, Config: prolly.DefaultConfig(), Registry: w.reg}, root)
	if err != nil {
		rt.Fatal(err)
	}
	defer func() { _ = closeLive() }()
	var liveBytes int64
	err = live.Each(func(hh hash.Hash) error {
		_, _, n, ok := s.Location(hh)
		if !ok {
			return fmt.Errorf("live chunk %s is not located", hh.Short())
		}
		liveBytes += n
		return nil
	})
	if err != nil {
		rt.Fatal(err)
	}
	var packBytes, packs int64
	after := ""
	for {
		page, err := w.blobs.List(ctx, "packs/", after, blob.MaxListPage)
		if err != nil {
			rt.Fatal(err)
		}
		for _, info := range page {
			packBytes += info.Size
			packs++
		}
		if len(page) < blob.MaxListPage {
			break
		}
		after = page[len(page)-1].Name
	}
	if slack := 512 * packs; packBytes > 2*liveBytes+slack {
		rt.Fatalf("with nothing left to collect the store holds %d bytes of packs for %d live bytes in %d packs: over twice the live bytes and %d of overhead, so a pack that is mostly dead was kept", packBytes, liveBytes, packs, slack)
	}
}
