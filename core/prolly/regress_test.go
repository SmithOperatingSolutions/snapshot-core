package prolly_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"runtime"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
)

// allocated is how many bytes f allocated, by the runtime's own count.
func allocated(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// limited is a map configuration whose values go to streams past 1 KiB and
// whose reads hold at most max bytes of one.
func limited(maxValue int) prolly.Config {
	c := prolly.DefaultConfig()
	c.InlineLimit, c.MaxValue = 1<<10, maxValue
	return c
}

// valueReads is every way a map hands a value over, each reading the map
// whole: Get of one key, an iteration, a diff from the empty map, and a
// walk that hands values over.
func valueReads(m *prolly.Map, s chunk.Reader, c prolly.Config, k []byte) map[string]func() ([]byte, error) {
	firstChange := func(d *prolly.DiffIter, err error) ([]byte, error) {
		if err != nil {
			return nil, err
		}
		var got []byte
		for {
			ch, ok, err := d.Next()
			if err != nil || !ok {
				return got, err
			}
			got = ch.To
		}
	}
	return map[string]func() ([]byte, error){
		"Get": func() ([]byte, error) {
			v, _, err := m.Get(ctx, k)
			return v, err
		},
		"iteration": func() ([]byte, error) {
			it, err := m.IterRange(ctx, nil, nil)
			if err != nil {
				return nil, err
			}
			var got []byte
			for {
				_, v, ok, err := it.Next()
				if err != nil || !ok {
					return got, err
				}
				got = v
			}
		},
		"Diff": func() ([]byte, error) {
			e, err := prolly.Empty(ctx, newStore(), c)
			if err != nil {
				return nil, err
			}
			return firstChange(prolly.Diff(ctx, e, m))
		},
		"Walk": func() ([]byte, error) {
			var got []byte
			err := prolly.Walk(ctx, s, c, m.Root(), func(hash.Hash, bool) (bool, error) { return true, nil },
				func(_, v []byte) error { got = bytes.Clone(v); return nil })
			return got, err
		},
	}
}

// #23: a map hands a long value over whole, reading its stream, and a
// stream's length is a claim that can be thousands of times what it
// stores. A map opened to hold at most 64 KiB of a value must refuse a
// 4 MiB one before reading it, however it is asked for; and hand a value
// of exactly 64 KiB over byte for byte.
func TestRegression_SC23_AMapReadsNoValueOverItsLimit(t *testing.T) {
	const limit = 64 << 10
	for name, tc := range map[string]struct {
		size   int
		refuse bool
	}{"at the limit": {limit, false}, "over it": {4 << 20, true}} {
		s := newStore()
		k, v := []byte("k"), random("sc23 "+name, tc.size)
		e := empty(t, s, limited(8<<20)).Editor() // written by a map that takes it
		must(t, e.Put(k, v))
		root := flush(t, e).Root()
		m, err := prolly.Open(ctx, s, limited(limit), root)
		if err != nil {
			t.Fatalf("%s: Open: %v", name, err)
		}
		for how, read := range valueReads(m, s, limited(limit), k) {
			var got []byte
			used := allocated(func() { got, err = read() })
			switch {
			case !tc.refuse && (err != nil || !bytes.Equal(got, v)):
				t.Errorf("positive control: a %d-byte value under a %d-byte limit read by %s as %d bytes, %v", tc.size, limit, how, len(got), err)
			case tc.refuse && (!errors.Is(err, prolly.ErrValueTooLarge) || used > 512<<10):
				t.Errorf("a map holding at most %d bytes of a value read a %d-byte one by %s: %d bytes, %v, after allocating %d bytes; "+
					"want ErrValueTooLarge within 512 KiB", limit, tc.size, how, len(got), err, used)
			}
		}
	}
}

// #23: what a map will not read back, its editor does not take. A value
// one byte over the limit is refused and the edit not applied; one of
// exactly the limit is taken and reads back.
func TestRegression_SC23_AnEditorTakesNoValueOverItsLimit(t *testing.T) {
	const limit = 64 << 10
	s := newStore()
	c := limited(limit)
	m := empty(t, s, c)
	at, over := random("sc23 at", limit), random("sc23 over", limit+1)
	e := m.Editor()
	if err := e.Put([]byte("over"), over); !errors.Is(err, prolly.ErrValueTooLarge) {
		t.Errorf("Put of a %d-byte value into a map whose limit is %d = %v, want ErrValueTooLarge", len(over), limit, err)
	}
	must(t, e.Put([]byte("at"), at)) // positive control: exactly the limit
	got := flush(t, e)
	if v, ok := get(t, got, []byte("at")); !ok || !bytes.Equal(v, at) {
		t.Fatalf("positive control: the %d-byte value read back as %d bytes (found %v)", limit, len(v), ok)
	}
	if got.Count() != 1 {
		t.Fatalf("a refused Put was applied: the map holds %d entries, want the 1 taken", got.Count())
	}
}

// #24: a node's header claims how many entries follow, and the claim is
// checked only as they decode. A 64 KiB node claiming 21,845 entries (as
// many 3-byte leaf entries as fit) whose first entry does not decode must
// be refused without first allocating room for 21,845 entries.
func TestRegression_SC24_ANodeIsNotSizedByItsClaimedCount(t *testing.T) {
	s := newStore()
	honest := build(t, s, 50, "sc24").Root()
	const budget = 256 << 10
	var err error
	if used := allocated(func() { _, err = prolly.Open(ctx, s, prolly.DefaultConfig(), honest) }); err != nil || used > budget {
		t.Fatalf("positive control: opening a 50-entry map allocated %d bytes (%v), want it open within %d", used, err, budget)
	}
	forged := binary.AppendUvarint([]byte{0x01, 0x00}, 21845)
	forged = append(forged, bytes.Repeat([]byte{0xFF}, 64<<10)...) // a key length that never ends
	root, err := s.Put(ctx, forged)
	must(t, err)
	used := allocated(func() { _, err = prolly.Open(ctx, s, prolly.DefaultConfig(), root) })
	if !errors.Is(err, chunk.ErrCorrupt) || used > budget {
		t.Fatalf("opening a 64 KiB node that claims 21,845 entries and holds none: %v, after allocating %d bytes; want ErrCorrupt within %d", err, used, budget)
	}
}
