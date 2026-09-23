package prolly_test

import (
	"bytes"
	"math/rand/v2"
	"sort"
	"sync/atomic"
	"testing"

	"pgregory.net/rapid"

	"github.com/SmithOperatingSolutions/snapshot-core/core/boundary"
	"github.com/SmithOperatingSolutions/snapshot-core/core/cdc"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
)

// tiny draws small nodes and keeps values over 72 bytes (one in ten) as
// streams, so a thousand entries make a tree several levels deep with both
// kinds of value. (Each stream write costs a 128 KiB chunker, which is why
// streams are the exception here, as they are in a real map.)
func tiny() prolly.Config {
	c := prolly.DefaultConfig()
	c.Nodes = boundary.Geometry{Min: 64, Target: 1024, Max: 4096}
	c.InlineLimit = 72
	c.Stream.CDC = cdc.Geometry{Min: 256, Max: 4 << 10, Mask: 0x3FF}
	return c
}

var (
	keyGen   = rapid.Map(rapid.SliceOfN(rapid.Byte(), 0, 16), func(b []byte) string { return string(b) })
	valueGen = rapid.SliceOfN(rapid.Byte(), 0, 80)
)

// drawEntries draws entry sets of three sizes: small and medium ones from
// rapid (which shrink to the smallest failure), and seeded large ones, which
// rapid alone would rarely reach, that make trees of height 2 and more.
func drawEntries(t *rapid.T, label string) map[string][]byte {
	switch rapid.IntRange(0, 2).Draw(t, label+" size class") {
	case 0:
		return rapid.MapOfN(keyGen, valueGen, 0, 12).Draw(t, label)
	case 1:
		return rapid.MapOfN(keyGen, valueGen, 0, 400).Draw(t, label)
	}
	return seeded(rapid.IntRange(700, 1500).Draw(t, label+" n"), rapid.Uint64().Draw(t, label+" seed"), nil)
}

// seeded returns n random entries (keys 1..16 bytes, values 0..80), adding
// to into when it is not nil.
func seeded(n int, seed uint64, into map[string][]byte) map[string][]byte {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	if into == nil {
		into = make(map[string][]byte, n)
	}
	for target := len(into) + n; len(into) < target; {
		k, v := make([]byte, 1+rng.IntN(16)), make([]byte, rng.IntN(81))
		for i := range k {
			k[i] = byte(rng.Uint32())
		}
		for i := range v {
			v[i] = byte(rng.Uint32())
		}
		into[string(k)] = v
	}
	return into
}

// deepEnough fails a property whose cases rarely built a tree of height 2
// or more: its incremental paths would have gone untested.
func deepEnough(t *testing.T, deep *atomic.Int64) {
	t.Helper()
	if n := deep.Load(); n < 10 {
		t.Fatalf("fixture: only %d cases built a tree of height 2 or more, want at least 10", n)
	}
}

// bulk builds the map of entries in one Flush of sorted puts.
func bulk(t tb, s *counting, c prolly.Config, entries map[string][]byte) *prolly.Map {
	t.Helper()
	e := empty(t, s, c).Editor()
	for _, k := range sortedKeysOf(entries) {
		must(t, e.Put([]byte(k), entries[k]))
	}
	return flush(t, e)
}

func sortedKeysOf(m map[string][]byte) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

// matches checks a map's contents against a reference, entry by entry.
func matches(t tb, m *prolly.Map, ref map[string][]byte) {
	t.Helper()
	got := all(t, m, nil, nil)
	keys := sortedKeysOf(ref)
	if len(got) != len(keys) || m.Count() != uint64(len(keys)) {
		t.Fatalf("the map has %d entries (Count %d), want %d", len(got), m.Count(), len(keys))
	}
	for i, k := range keys {
		if got[i].k != k || !bytes.Equal([]byte(got[i].v), ref[k]) {
			t.Fatalf("entry %d is %q (%d bytes), want %q (%d bytes)", i, got[i].k, len(got[i].v), k, len(ref[k]))
		}
	}
}

// Engine Spec L1 "Determinism (property): for random entry sets, inserting
// in any shuffled order yields the same root hash" — in any batches too.
func TestDeterminismProperty(t *testing.T) {
	var deep atomic.Int64
	rapid.Check(t, func(rt *rapid.T) {
		entries := drawEntries(rt, "entries")
		keys := sortedKeysOf(entries)
		s := newStore()
		want := bulk(rt, s, tiny(), entries)
		if want.Height() >= 2 {
			deep.Add(1)
		}
		for _, name := range []string{"first", "second"} {
			order := rapid.Permutation(keys).Draw(rt, name+" order")
			batch := rapid.IntRange(1, 200).Draw(rt, name+" batch")
			e := empty(rt, s, tiny()).Editor()
			for i, k := range order {
				must(rt, e.Put([]byte(k), entries[k]))
				if (i+1)%batch == 0 {
					flush(rt, e) // an intermediate tree, edited in place by the next batch
				}
			}
			if m := flush(rt, e); m.Root() != want.Root() {
				rt.Fatalf("%s order in batches of %d made root %s, sorted in one batch %s", name, batch, m.Root(), want.Root())
			}
		}
		matches(rt, want, entries)
	})
	deepEnough(t, &deep)
}

// Engine Spec L1 "History independence (property): insert then delete key k
// yields the same root as never inserting k".
func TestHistoryIndependenceProperty(t *testing.T) {
	var deep atomic.Int64
	rapid.Check(t, func(rt *rapid.T) {
		entries := drawEntries(rt, "entries")
		k := keyGen.Filter(func(k string) bool { _, ok := entries[k]; return !ok }).Draw(rt, "k")
		v := valueGen.Draw(rt, "v")
		s := newStore()
		never := bulk(rt, s, tiny(), entries)
		if never.Height() >= 2 {
			deep.Add(1)
		}
		e := never.Editor()
		must(rt, e.Put([]byte(k), v))
		with := flush(rt, e)
		if with.Root() == never.Root() {
			rt.Fatalf("inserting %q did not change the root", k)
		}
		must(rt, e.Delete([]byte(k)))
		if back := flush(rt, e); back.Root() != never.Root() {
			rt.Fatalf("inserting then deleting %q left root %s, never inserting it %s", k, back.Root(), never.Root())
		}
	})
	deepEnough(t, &deep)
}

// Flush edits a tree in place, re-chunking only around the edits; after
// every batch of random puts and deletes the result is exactly the tree a
// bulk build of the same entries draws, and holds exactly those entries.
func TestIncrementalFlushEqualsBulkBuild(t *testing.T) {
	var deep atomic.Int64
	rapid.Check(t, func(rt *rapid.T) {
		ref := drawEntries(rt, "initial")
		s := newStore()
		m := bulk(rt, s, tiny(), ref)
		batches := rapid.IntRange(1, 4).Draw(rt, "batches")
		for b := 0; b < batches; b++ {
			if m.Height() >= 2 {
				deep.Add(1)
			}
			e := m.Editor()
			ops := rapid.IntRange(1, 120).Draw(rt, "ops")
			existing := sortedKeysOf(ref)
			for i := 0; i < ops; i++ {
				switch op := rapid.IntRange(0, 5).Draw(rt, "op"); {
				case op == 4: // grow: many new keys at once
					for k, v := range seeded(rapid.IntRange(1, 800).Draw(rt, "grow"), rapid.Uint64().Draw(rt, "grow seed"), nil) {
						must(rt, e.Put([]byte(k), v))
						ref[k] = v
					}
				case op == 5: // collapse: delete nine keys in ten at once
					seed := rapid.Uint64().Draw(rt, "collapse seed")
					rng := rand.New(rand.NewPCG(seed, seed))
					for _, k := range existing {
						if rng.IntN(10) > 0 {
							must(rt, e.Delete([]byte(k)))
							delete(ref, k)
						}
					}
				case op == 0 || len(existing) == 0: // put a new or random key
					k, v := keyGen.Draw(rt, "key"), valueGen.Draw(rt, "value")
					must(rt, e.Put([]byte(k), v))
					ref[k] = v
				case op == 1: // overwrite an existing key
					k := rapid.SampledFrom(existing).Draw(rt, "overwrite")
					v := valueGen.Draw(rt, "value")
					must(rt, e.Put([]byte(k), v))
					ref[k] = v
				case op == 2: // delete an existing key
					k := rapid.SampledFrom(existing).Draw(rt, "delete")
					must(rt, e.Delete([]byte(k)))
					delete(ref, k)
				default: // delete a key that may not exist
					k := keyGen.Draw(rt, "delete maybe")
					must(rt, e.Delete([]byte(k)))
					delete(ref, k)
				}
			}
			m = flush(rt, e)
			if want := bulk(rt, newStore(), tiny(), ref); m.Root() != want.Root() {
				rt.Fatalf("after batch %d the edited tree's root is %s, a bulk build's %s", b, m.Root(), want.Root())
			}
			matches(rt, m, ref)
		}
	})
	deepEnough(t, &deep)
}
