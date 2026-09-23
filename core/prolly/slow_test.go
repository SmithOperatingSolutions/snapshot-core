//go:build slow

package prolly_test

import (
	"math/rand/v2"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
)

// The C2 exit criterion: determinism and bounded-diff properties hold on 1M
// entries (Engine Spec L1 at full scale; the normal suite runs them smaller).

const million = 1_000_000

// Sorted in one batch, or shuffled across ten: the same root.
func TestSlowDeterminismOn1MEntries(t *testing.T) {
	s := newStore()
	sorted := build(t, s, million, "a")
	e := empty(t, s, prolly.DefaultConfig()).Editor()
	var m *prolly.Map
	for i, j := range rand.New(rand.NewPCG(7, 7)).Perm(million) {
		must(t, e.Put(key(j), val(j, "a")))
		if (i+1)%(million/10) == 0 {
			m = flush(t, e)
		}
	}
	if m.Root() != sorted.Root() || m.Count() != million {
		t.Fatalf("1M entries shuffled across 10 flushes made root %s (count %d), sorted in one %s", m.Root(), m.Count(), sorted.Root())
	}
}

// "Changing one value in a 1M-entry map rewrites at most tree-height + 1 nodes."
func TestSlowOneValueChangeOn1MEntries(t *testing.T) {
	s := newStore()
	m := build(t, s, million, "a")
	s.added.Store(0)
	e := m.Editor()
	must(t, e.Put(key(654321), val(654321, "b")))
	flush(t, e)
	if n := s.added.Load(); n != int64(m.Height()+1) {
		t.Fatalf("changing one value of 1M stored %d new nodes, want height+1 = %d", n, m.Height()+1)
	}
}

// "Diff of two maps differing by n entries returns exactly those n, and reads
// O(n log N) nodes."
func TestSlowDiffOn1MEntriesReadsLittle(t *testing.T) {
	s := newStore()
	from := build(t, s, million, "a")
	e := from.Editor()
	rng := rand.New(rand.NewPCG(9, 9))
	changed := map[int]bool{}
	for len(changed) < 100 {
		changed[rng.IntN(million)] = true
	}
	for i := range changed {
		must(t, e.Put(key(i), val(i, "b")))
	}
	to := flush(t, e)
	s.gets.Store(0)
	got := diff(t, from, to)
	if len(got) != len(changed) {
		t.Fatalf("a diff of 100 changes in 1M returned %d", len(got))
	}
	if reads, limit := s.gets.Load(), int64(4*len(changed)*(from.Height()+1)); reads > limit {
		t.Fatalf("a diff of 100 changes in 1M read %d nodes, want at most %d", reads, limit)
	}
}

// "Node sizes stay within 512 B – 16 KiB across 1M random entries."
func TestSlowNodeSizesOn1MRandomEntries(t *testing.T) {
	s := newStore()
	checkNodeSizes(t, s, randomMap(t, s, million, 3))
}
