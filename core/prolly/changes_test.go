package prolly_test

import (
	"bytes"
	"math/rand/v2"
	"sync/atomic"
	"testing"

	"pgregory.net/rapid"

	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
)

// A flush knows which keys it changed (#43): exactly the keys a Diff of the
// map it edited against the map it made reports, in key order, and the root
// it edited. A put of the value already stored, a delete of a key the map
// lacks, and a new key put then deleted in one batch change nothing and are
// not among them. The authority is Diff, which walks both trees; a host
// that authorizes per changed path may use the flush's list in its place
// only because the two agree.
func TestAFlushKnowsWhatItChangedProperty(t *testing.T) {
	var deep atomic.Int64
	rapid.Check(t, func(rt *rapid.T) {
		ref := drawEntries(rt, "initial")
		s := newStore()
		m := bulk(rt, s, tiny(), ref)
		e := m.Editor()
		for b := range rapid.IntRange(1, 3).Draw(rt, "batches") {
			if m.Height() >= 2 {
				deep.Add(1)
			}
			existing := sortedKeysOf(ref)
			for range rapid.IntRange(1, 60).Draw(rt, "ops") {
				switch op := rapid.IntRange(0, 6).Draw(rt, "op"); {
				case op == 0 || len(existing) == 0:
					k, v := keyGen.Draw(rt, "key"), valueGen.Draw(rt, "value")
					must(rt, e.Put([]byte(k), v))
				case op == 1:
					must(rt, e.Put([]byte(rapid.SampledFrom(existing).Draw(rt, "overwrite")), valueGen.Draw(rt, "value")))
				case op == 2: // the value already stored
					k := rapid.SampledFrom(existing).Draw(rt, "same")
					must(rt, e.Put([]byte(k), ref[k]))
				case op == 3:
					must(rt, e.Delete([]byte(rapid.SampledFrom(existing).Draw(rt, "delete"))))
				case op == 4: // most likely a key the map lacks
					must(rt, e.Delete([]byte(keyGen.Draw(rt, "delete maybe"))))
				case op == 5: // a new key, put then deleted
					k := keyGen.Draw(rt, "put then delete")
					must(rt, e.Put([]byte(k), valueGen.Draw(rt, "value")))
					must(rt, e.Delete([]byte(k)))
				default: // many at once, so boundaries move at every level
					seed := rapid.Uint64().Draw(rt, "seed")
					rng := rand.New(rand.NewPCG(seed, seed))
					for _, k := range existing {
						if rng.IntN(4) == 0 {
							must(rt, e.Delete([]byte(k)))
						}
					}
					for k, v := range seeded(rapid.IntRange(1, 300).Draw(rt, "grow"), seed, nil) {
						must(rt, e.Put([]byte(k), v))
					}
				}
			}
			next := flush(rt, e)
			base, keys, ok := next.Changes()
			if !ok {
				rt.Fatalf("batch %d: a flushed map does not know what its flush changed", b)
			}
			if base != m.Root() {
				rt.Fatalf("batch %d: the flush says it edited root %s, it edited %s", b, base, m.Root())
			}
			want := diff(rt, m, next)
			if len(keys) != len(want) {
				rt.Fatalf("batch %d: the flush lists %d changed keys, a diff finds %d: a host authorizing by the list would ask about the wrong paths", b, len(keys), len(want))
			}
			for i, c := range want {
				if !bytes.Equal(keys[i], c.Key) {
					rt.Fatalf("batch %d: changed key %d is %q in the flush's list, %q in the diff", b, i, keys[i], c.Key)
				}
			}
			ref = map[string][]byte{}
			for _, kv := range all(rt, next, nil, nil) {
				ref[kv.k] = []byte(kv.v)
			}
			m = next
		}
	})
	deepEnough(t, &deep)
}

// Only a flush knows what it changed: a map opened by its root, or the
// empty map, says so, and a caller must diff.
func TestAnOpenedMapDoesNotClaimChanges(t *testing.T) {
	s := newStore()
	m := build(t, s, 50, "a")
	if _, _, ok := m.Changes(); !ok {
		t.Fatal("positive control: a flushed map does not know what its flush changed")
	}
	opened, err := prolly.Open(ctx, s, prolly.DefaultConfig(), m.Root())
	must(t, err)
	if _, keys, ok := opened.Changes(); ok {
		t.Fatalf("a map opened by its root claims its opening changed %d keys", len(keys))
	}
	if _, _, ok := empty(t, s, prolly.DefaultConfig()).Changes(); ok {
		t.Fatal("the empty map claims changes")
	}
}
