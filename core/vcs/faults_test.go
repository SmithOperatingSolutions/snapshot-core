package vcs_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/auth"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/memstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/object"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
	"github.com/SmithOperatingSolutions/snapshot-core/core/vcs"
)

var errInjected = errors.New("injected store failure")

// The store calls faulty can fail.
const (
	callGet = iota
	callPut
	callHas
	callRoot
	callSwap
	callKinds
)

var callNames = [callKinds]string{"read", "write", "has", "root read", "root swap"}

// faulty fails exactly one store call, the at-th of one kind, and passes
// everything else through. Failing only one means an operation that
// swallowed the error would go on to succeed, visibly.
type faulty struct {
	*memstore.Store
	kind  int
	at    int64
	calls [callKinds]atomic.Int64
}

func (s *faulty) fails(kind int) bool { return s.calls[kind].Add(1) == s.at && s.kind == kind }

func (s *faulty) Get(ctx context.Context, h hash.Hash) ([]byte, error) {
	if s.fails(callGet) {
		return nil, errInjected
	}
	return s.Store.Get(ctx, h)
}

func (s *faulty) Put(ctx context.Context, b []byte) (hash.Hash, error) {
	if s.fails(callPut) {
		return hash.Hash{}, errInjected
	}
	return s.Store.Put(ctx, b)
}

func (s *faulty) Has(ctx context.Context, hs []hash.Hash) (map[hash.Hash]bool, error) {
	if s.fails(callHas) {
		return nil, errInjected
	}
	return s.Store.Has(ctx, hs)
}

func (s *faulty) Root(ctx context.Context) (hash.Hash, error) {
	if s.fails(callRoot) {
		return hash.Hash{}, errInjected
	}
	return s.Store.Root(ctx)
}

func (s *faulty) CompareAndSetRoot(ctx context.Context, expected, next hash.Hash) error {
	if s.fails(callSwap) {
		return errInjected
	}
	return s.Store.CompareAndSetRoot(ctx, expected, next)
}

// arm makes the at-th call of kind fail (at 0: none) and restarts the count.
func (s *faulty) arm(kind int, at int64) {
	s.kind, s.at = kind, at
	for i := range s.calls {
		s.calls[i].Store(0)
	}
}

func (s *faulty) root(t *testing.T) hash.Hash {
	t.Helper()
	h, err := s.Store.Root(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// restore puts the root back to before. A store with no root yet is put
// back by starting again on an empty one.
func (s *faulty) restore(t *testing.T, before hash.Hash) {
	t.Helper()
	if before.IsZero() {
		s.Store = memstore.New()
		return
	}
	if err := s.Store.CompareAndSetRoot(ctx, s.root(t), before); err != nil {
		t.Fatal(err)
	}
}

// everyFailurePoint runs op once with nothing failing, to count the store
// calls it makes, then once for each of them with that call failing. Every
// failing run must return the store's error and leave the root where it
// was. Between runs the root is put back, so every run starts from one
// repository.
func everyFailurePoint(t *testing.T, s *faulty, name string, op func() error) {
	t.Helper()
	before := s.root(t)
	s.arm(0, 0)
	if err := op(); err != nil {
		t.Fatalf("%s: positive control: %v", name, err)
	}
	var counts [callKinds]int64
	for k := range counts {
		counts[k] = s.calls[k].Load()
	}
	s.restore(t, before)
	failed := 0
	for k, n := range counts {
		for at := int64(1); at <= n; at++ {
			s.arm(k, at)
			err := op()
			s.arm(0, 0)
			if !errors.Is(err, errInjected) {
				t.Fatalf("%s: with %s %d of %d failing, err = %v; want the store's error", name, callNames[k], at, n, err)
			}
			if now := s.root(t); now != before {
				t.Fatalf("%s: with %s %d of %d failing, the root moved from %s to %s", name, callNames[k], at, n, before, now)
			}
			failed++
		}
	}
	if failed == 0 {
		t.Fatalf("%s: fixture: the operation made no store calls", name)
	}
}

// On any error, nothing partial reaches the root (CONTRIBUTING.md): every
// store call that every operation makes, failed in turn, is that
// operation's error, and leaves the refs as they were. A read that failed
// is never a missing branch or an empty history.
func TestStoreErrorsSurfaceAndLeaveTheRefs(t *testing.T) {
	s := &faulty{Store: memstore.New()}
	o := options(t, auth.AllowAll{}, &clock{})
	everyFailurePoint(t, s, "Init", func() error { _, err := vcs.Init(ctx, s, alice, o); return err })

	f := newFixtureOn(t, s)
	main := vcs.MainBranch
	f.put(main, "doc", f.obj(8, "base"))
	f.put(main, "notes", f.obj(7, "a"))
	f.commit(main, "base")
	f.branchFrom("dev")
	f.edit(alice, "dev", map[string]*object.Ref{"doc": ptr(f.obj(8, "dev")), "dev-only": ptr(f.obj(7, "d"))})
	theirs := f.commit("dev", "dev work")
	f.edit(alice, main, map[string]*object.Ref{"doc": ptr(f.obj(8, "main")), "main-only": ptr(f.obj(7, "m"))})
	ours := f.commit(main, "main work")

	ws, err := f.r.WorkingSet(ctx, alice, main)
	if err != nil {
		t.Fatal(err)
	}
	n, err := f.r.Namespace(ctx, ws.Working)
	if err != nil {
		t.Fatal(err)
	}
	e := n.Editor()
	if err := e.Put("staged", f.obj(7, "s")); err != nil {
		t.Fatal(err)
	}
	if n, err = e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	next := ws
	next.Working, next.Staged = n.Root(), n.Root()

	for _, c := range []struct {
		name string
		op   func() error
	}{
		{"Open", func() error { _, err := vcs.Open(ctx, s, f.o); return err }},
		{"Head", func() error { _, err := f.r.Head(ctx, alice, main); return err }},
		{"Branches", func() error { _, err := f.r.Branches(ctx, alice); return err }},
		{"ReadCommit", func() error { _, err := f.r.ReadCommit(ctx, alice, ours.Hash); return err }},
		{"WorkingSet", func() error { _, err := f.r.WorkingSet(ctx, alice, main); return err }},
		{"UpdateWorkingSet", func() error { _, err := f.r.UpdateWorkingSet(ctx, alice, main, ws, next); return err }},
		{"CommitWorkingSet", func() error { _, err := f.r.CommitWorkingSet(ctx, alice, main, "again"); return err }},
		{"CreateBranch", func() error { return f.r.CreateBranch(ctx, alice, "feature", ours.Hash) }},
		{"DeleteBranch", func() error { return f.r.DeleteBranch(ctx, alice, "dev") }},
		{"Checkout", func() error {
			sess, err := f.r.Checkout(ctx, alice, main)
			if err == nil {
				sess.Close()
			}
			return err
		}},
		{"CreateTag", func() error { _, err := f.r.CreateTag(ctx, alice, "v1", ours.Hash, "release"); return err }},
		{"Log", func() error { _, err := f.r.Log(ctx, alice, ours.Hash, 10); return err }},
		{"MergeBase", func() error { _, err := f.r.MergeBase(ctx, alice, ours.Hash, theirs.Hash); return err }},
		{"Merge", func() error { _, err := f.r.Merge(ctx, alice, main, theirs.Hash); return err }},
	} {
		everyFailurePoint(t, s, c.name, c.op)
	}

	if r, err := f.r.Merge(ctx, alice, main, theirs.Hash); err != nil || len(r.Conflicts) != 1 {
		t.Fatalf("fixture: the merge found %d conflicts (%v), want 1", len(r.Conflicts), err)
	}
	resolved := f.obj(8, "resolved")
	everyFailurePoint(t, s, "Conflicts", func() error { _, err := f.r.Conflicts(ctx, alice, main); return err })
	everyFailurePoint(t, s, "ResolveConflict", func() error { return f.r.ResolveConflict(ctx, alice, main, "doc", &resolved) })
	everyFailurePoint(t, s, "ResolveConflict by deleting", func() error { return f.r.ResolveConflict(ctx, alice, main, "doc", nil) })

	if err := f.r.ResolveConflict(ctx, alice, main, "doc", &resolved); err != nil {
		t.Fatal(err)
	}
	everyFailurePoint(t, s, "CommitWorkingSet of a merge", func() error {
		_, err := f.r.CommitWorkingSet(ctx, alice, main, "merge dev")
		return err
	})
	merged := f.commit(main, "merge dev")
	everyFailurePoint(t, s, "Log through a merge", func() error { _, err := f.r.Log(ctx, alice, merged.Hash, 10); return err })

	// A conflict list that spans nodes, so listing it reads the store.
	many := map[string]*object.Ref{}
	side := func(branch, content string) vcs.Commit {
		for p := range many {
			many[p] = ptr(f.obj(8, content))
		}
		f.edit(alice, branch, many)
		return f.commit(branch, content)
	}
	for i := range 200 {
		many[fmt.Sprintf("many/%03d", i)] = nil
	}
	side(main, "many")
	f.branchFrom("left")
	left := side("left", "left")
	side(main, "right")
	if r, err := f.r.Merge(ctx, alice, main, left.Hash); err != nil || len(r.Conflicts) != len(many) {
		t.Fatalf("fixture: the merge found %d conflicts (%v), want %d", len(r.Conflicts), err, len(many))
	}
	if ws, err = f.r.WorkingSet(ctx, alice, main); err != nil {
		t.Fatal(err)
	}
	if cm, err := prolly.Open(ctx, s, f.o.Config, ws.Merge.Conflicts); err != nil || cm.Height() == 0 {
		t.Fatalf("fixture: the conflicts map fits in one node (%v)", err)
	}
	everyFailurePoint(t, s, "Conflicts spanning nodes", func() error { _, err := f.r.Conflicts(ctx, alice, main); return err })
}

// racing runs first once, just before the first root swap, as another
// writer getting in between a read of the root and the swap would; with
// lose set, every swap loses, as if another writer always got in first.
type racing struct {
	*memstore.Store
	first func()
	lose  bool
	swaps atomic.Int64
}

func (s *racing) CompareAndSetRoot(ctx context.Context, expected, next hash.Hash) error {
	s.swaps.Add(1)
	if f := s.first; f != nil {
		s.first = nil
		f()
	}
	if s.lose {
		return chunk.ErrRootConflict
	}
	return s.Store.CompareAndSetRoot(ctx, expected, next)
}

// Two Inits race for one store: the one whose swap loses is ErrExists, not
// a root conflict, and the repository is the winner's.
func TestAnInitThatLosesTheRaceIsErrExists(t *testing.T) {
	s := &racing{Store: memstore.New()}
	o := options(t, auth.AllowAll{}, &clock{})
	s.first = func() {
		if _, err := vcs.Init(ctx, s.Store, bob, o); err != nil {
			t.Errorf("the Init that won: %v", err)
		}
	}
	if _, err := vcs.Init(ctx, s, alice, o); !errors.Is(err, vcs.ErrExists) {
		t.Fatalf("the Init that lost the race = %v, want ErrExists", err)
	}
	r, err := vcs.Open(ctx, s.Store, o)
	if err != nil {
		t.Fatal(err)
	}
	if c, err := r.Head(ctx, alice, vcs.MainBranch); err != nil || c.Author != bob.ID {
		t.Fatalf("main's first commit is by %q (%v), want the winner, %s", c.Author, err, bob.ID)
	}
}

// A writer whose every root swap loses re-reads and re-applies, but not
// forever: it gives up with an error of its own, and nothing it did
// reaches the root.
func TestAWriterThatKeepsLosingGivesUp(t *testing.T) {
	s := &racing{Store: memstore.New()}
	f := newFixtureOn(t, s)
	vcs.SetBackoff(f.r, func(ctx context.Context, _ time.Duration) error { return ctx.Err() }, noJitter) // 1000 pauses, taken at once
	f.put(vcs.MainBranch, "a", f.obj(7, "a"))
	before := f.head(vcs.MainBranch)
	s.lose = true
	s.swaps.Store(0)
	_, err := f.r.CommitWorkingSet(ctx, alice, vcs.MainBranch, "never lands")
	if err == nil || errors.Is(err, chunk.ErrRootConflict) {
		t.Fatalf("a commit that lost every swap = %v, want the version graph to give up", err)
	}
	if n := s.swaps.Load(); n < 2 || n > 1000 {
		t.Fatalf("the commit tried %d swaps, want it to retry, a bounded number of times", n)
	}
	if got := f.head(vcs.MainBranch); got.Hash != before.Hash {
		t.Fatal("a commit that gave up moved the head")
	}
}

// A repository needs a model registry to read what it stores: without one
// Init and Open are refused, and Init writes no root. A nil clock is the
// wall clock.
func TestARepositoryNeedsARegistryAndReadsTheWallClock(t *testing.T) {
	f := newFixture(t)
	o := f.o
	o.Registry = nil
	if _, err := vcs.Open(ctx, f.s, o); err == nil {
		t.Error("Open without a model registry succeeded")
	}
	empty := memstore.New()
	if _, err := vcs.Init(ctx, empty, alice, o); err == nil {
		t.Error("Init without a model registry succeeded")
	}
	if root, _ := empty.Root(ctx); !root.IsZero() {
		t.Fatal("a refused Init wrote a root")
	}
	o = f.o
	o.Clock = nil
	start := time.Now()
	r, err := vcs.Init(ctx, memstore.New(), alice, o)
	if err != nil {
		t.Fatal(err)
	}
	c, err := r.Head(ctx, alice, vcs.MainBranch)
	if err != nil || c.Time.Before(start.Add(-time.Minute)) || c.Time.After(time.Now().Add(time.Minute)) {
		t.Fatalf("with a nil clock the first commit is at %v (%v), want about %v", c.Time, err, start)
	}
}

// The refs map is data like any other: a ref that is not a hash, a branch
// with no working set, and a ref naming the wrong kind of chunk are
// ErrCorrupt, not a missing branch, a zero hash or a truncated one.
func TestForgedRefsAreCorrupt(t *testing.T) {
	main := vcs.MainBranch
	head, work := []byte("heads/"+main), []byte("work/"+main)
	for _, c := range []struct {
		name string
		edit func(e *prolly.Editor, head, ws hash.Hash) error
		read func(f *fixture) error
	}{
		{"a head of 33 bytes", func(e *prolly.Editor, h, _ hash.Hash) error { return e.Put(head, append(h[:], 0)) }, readHead},
		{"a head of 5 bytes", func(e *prolly.Editor, _, _ hash.Hash) error { return e.Put(head, []byte("short")) }, readHead},
		{"a head naming a working set", func(e *prolly.Editor, _, ws hash.Hash) error { return e.Put(head, ws[:]) }, readHead},
		{"a branch with no working set", func(e *prolly.Editor, _, _ hash.Hash) error { return e.Delete(work) }, readWorkingSet},
		{"a working set naming a commit", func(e *prolly.Editor, h, _ hash.Hash) error { return e.Put(work, h[:]) }, readWorkingSet},
	} {
		f := newFixture(t)
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
		if err := c.edit(e, f.head(main).Hash, ws.Hash); err != nil {
			t.Fatal(err)
		}
		forged, err := e.Flush(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.s.CompareAndSetRoot(ctx, root, forged.Root()); err != nil {
			t.Fatal(err)
		}
		if err := c.read(f); !errors.Is(err, chunk.ErrCorrupt) {
			t.Errorf("%s: reading it = %v, want ErrCorrupt", c.name, err)
		}
	}
}

func readHead(f *fixture) error { _, err := f.r.Head(ctx, alice, vcs.MainBranch); return err }

func readWorkingSet(f *fixture) error {
	_, err := f.r.WorkingSet(ctx, alice, vcs.MainBranch)
	return err
}

// lostOnce is a store whose next root swap fails as the swap of a store
// whose session GC lost does (DESIGN §9).
type lostOnce struct {
	*memstore.Store
	armed bool
}

func (s *lostOnce) CompareAndSetRoot(ctx context.Context, expected, next hash.Hash) error {
	if s.armed {
		s.armed = false
		return fmt.Errorf("%w: a chunk this write counted on", chunk.ErrSessionLost)
	}
	return s.Store.CompareAndSetRoot(ctx, expected, next)
}

// A write whose store lost its session to GC is ErrSessionLost to the host,
// not a conflict: reading again and writing again on the same store cannot
// help, so the host reopens the repository instead. Nothing changed.
func TestALostSessionIsNoConflict(t *testing.T) {
	s := &lostOnce{Store: memstore.New()}
	f := newFixtureOn(t, s)
	main := vcs.MainBranch
	f.put(main, "a", f.obj(7, "a"))
	before := f.head(main)
	s.armed = true
	_, err := f.r.CommitWorkingSet(ctx, alice, main, "lost")
	if !errors.Is(err, vcs.ErrSessionLost) || errors.Is(err, vcs.ErrConflict) {
		t.Fatalf("a commit whose session GC lost = %v, want ErrSessionLost and no conflict", err)
	}
	if got := f.head(main); got.Hash != before.Hash {
		t.Fatal("the lost commit moved the head")
	}
	ws, err := f.r.WorkingSet(ctx, alice, main)
	if err != nil {
		t.Fatal(err)
	}
	s.armed = true
	if _, err := f.r.UpdateWorkingSet(ctx, alice, main, ws, ws); !errors.Is(err, vcs.ErrSessionLost) || errors.Is(err, vcs.ErrConflict) {
		t.Fatalf("a working-set update whose session GC lost = %v, want ErrSessionLost and no conflict", err)
	}
}
