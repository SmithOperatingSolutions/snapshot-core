package vcs_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/memstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/vcs"
)

// pauses records the pauses a repository takes between lost swaps.
type pauses struct {
	mu sync.Mutex
	d  []time.Duration
}

func (p *pauses) sleep(ctx context.Context, d time.Duration) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.d = append(p.d, d)
	return ctx.Err()
}

func (p *pauses) taken() []time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.d)
}

func mostJitter(n int64) int64 { return n - 1 }
func noJitter(int64) int64     { return 0 }

// losing loses its next left root swaps, as if another writer always got
// in first, and runs at once, just before the at'th swap.
type losing struct {
	*memstore.Store
	left, swaps atomic.Int64
	at          int64
	fn          func()
}

func (s *losing) CompareAndSetRoot(ctx context.Context, expected, next hash.Hash) error {
	if n := s.swaps.Add(1); n == s.at && s.fn != nil {
		s.fn()
	}
	if s.left.Add(-1) >= 0 {
		return chunk.ErrRootConflict
	}
	return s.Store.CompareAndSetRoot(ctx, expected, next)
}

// The pause after each lost swap in a row starts under a millisecond and
// doubles to a cap of tens of milliseconds, where it stays however long
// the losing goes on; jitter spreads each pause over the upper half of its
// span, so writers that lost together do not retry together.
func TestTheBackoffDoublesFromUnderAMillisecondToItsCap(t *testing.T) {
	want := []time.Duration{200 * time.Microsecond, 400 * time.Microsecond, 800 * time.Microsecond,
		1600 * time.Microsecond, 3200 * time.Microsecond, 6400 * time.Microsecond, 12800 * time.Microsecond,
		20 * time.Millisecond, 20 * time.Millisecond}
	for i, w := range want {
		if got := vcs.Backoff(i, mostJitter); got != w {
			t.Errorf("the pause after lost swap %d, jittered longest, is %v, want %v", i+1, got, w)
		}
		if got := vcs.Backoff(i, noJitter); got != w/2 {
			t.Errorf("the pause after lost swap %d, jittered shortest, is %v, want %v", i+1, got, w/2)
		}
	}
	for _, i := range []int{63, 64, 999, 1 << 30} {
		if got := vcs.Backoff(i, mostJitter); got != 20*time.Millisecond {
			t.Errorf("the pause after lost swap %d is %v, want the 20ms cap", i+1, got)
		}
	}
}

// A writer that loses root swaps pauses after each loss, longer each time,
// and not before its first attempt or after the swap that lands.
func TestLostSwapsBackOffBetweenAttempts(t *testing.T) {
	s := &losing{Store: memstore.New()}
	f := newFixtureOn(t, s)
	p := &pauses{}
	vcs.SetBackoff(f.r, p.sleep, mostJitter)
	f.put(vcs.MainBranch, "a", f.obj(7, "a"))
	if got := p.taken(); len(got) != 0 {
		t.Fatalf("positive control: writes that lost nothing paused %v", got)
	}
	s.swaps.Store(0)
	s.left.Store(3)
	if _, err := f.r.CommitWorkingSet(ctx, alice, vcs.MainBranch, "after three losses"); err != nil {
		t.Fatalf("a commit whose first 3 swaps lost: %v", err)
	}
	if n := s.swaps.Load(); n != 4 {
		t.Fatalf("fixture: the commit made %d swaps, want 4 (3 lost, then one that lands)", n)
	}
	want := []time.Duration{vcs.Backoff(0, mostJitter), vcs.Backoff(1, mostJitter), vcs.Backoff(2, mostJitter)}
	if got := p.taken(); !slices.Equal(got, want) {
		t.Fatalf("after 3 lost swaps the writer paused %v, want %v: writers that lose must back off, or they retry in a storm", got, want)
	}
}

// Resolving a conflict re-reads and re-applies when the working set changed
// under it, and pauses before it does, as a lost swap does.
func TestAResolveThatLostItsWorkingSetBacksOff(t *testing.T) {
	s := &losing{Store: memstore.New()}
	f := newFixtureOn(t, s)
	main := vcs.MainBranch
	f.put(main, "doc", f.obj(8, "base"))
	f.commit(main, "base")
	f.branchFrom("dev")
	f.put("dev", "doc", f.obj(8, "dev"))
	theirs := f.commit("dev", "dev work")
	f.put(main, "doc", f.obj(8, "main"))
	f.commit(main, "main work")
	if r, err := f.r.Merge(ctx, alice, main, theirs.Hash); err != nil || len(r.Conflicts) != 1 {
		t.Fatalf("fixture: the merge found %d conflicts (%v), want 1", len(r.Conflicts), err)
	}
	p := &pauses{}
	vcs.SetBackoff(f.r, p.sleep, mostJitter)
	s.swaps.Store(0)
	s.at, s.fn = 1, func() { f.putWorking(main, "notes", f.obj(7, "another writer's")) } // before the resolve's swap
	resolved := f.obj(8, "resolved")
	if err := f.r.ResolveConflict(ctx, alice, main, "doc", &resolved); err != nil {
		t.Fatalf("a resolve whose working set changed under it: %v", err)
	}
	if cs, err := f.r.Conflicts(ctx, alice, main); err != nil || len(cs) != 0 {
		t.Fatalf("after the resolve %d conflicts remain (%v), want none", len(cs), err)
	}
	b0 := vcs.Backoff(0, mostJitter)
	if got := p.taken(); !slices.Equal(got, []time.Duration{b0, b0}) {
		t.Fatalf("the resolve paused %v, want %v: once for the lost swap, once for the working set it re-read", got, []time.Duration{b0, b0})
	}
}

// A writer that keeps losing stops when its context ends, during the
// pause, rather than retrying on to the attempt bound.
func TestAWriterThatKeepsLosingStopsWithItsContext(t *testing.T) {
	s := &losing{Store: memstore.New()}
	f := newFixtureOn(t, s)
	f.put(vcs.MainBranch, "a", f.obj(7, "a"))
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.swaps.Store(0)
	s.left.Store(1 << 40)
	s.at, s.fn = 3, cancel
	start := time.Now()
	_, err := f.r.CommitWorkingSet(cctx, alice, vcs.MainBranch, "never lands")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("a commit that kept losing after its context ended = %v, want context.Canceled", err)
	}
	if n := s.swaps.Load(); n != 3 {
		t.Fatalf("the commit made %d swaps, want 3: it must stop at the pause after its context ends", n)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("the commit took %v to stop", d)
	}
}

// The default pause ends when its context does, not when its time is up.
func TestAPauseEndsWithItsContext(t *testing.T) {
	if err := vcs.Sleep(ctx, time.Millisecond); err != nil {
		t.Fatalf("positive control: a pause whose context lives = %v, want it to end on time", err)
	}
	cctx, cancel := context.WithCancel(ctx)
	time.AfterFunc(10*time.Millisecond, cancel)
	start := time.Now()
	if err := vcs.Sleep(cctx, 10*time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("a pause whose context ended = %v, want context.Canceled", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("a pause whose context ended at 10ms lasted %v: a canceled writer waits out its backoff", d)
	}
}
