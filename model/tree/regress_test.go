package tree_test

import (
	"encoding/binary"
	"errors"
	"runtime"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/memstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
	"github.com/SmithOperatingSolutions/snapshot-core/model/tree"
)

// allocated is how many bytes f allocated, by the runtime's own count.
func allocated(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// The prolly node format of DESIGN §7, written here independently:
// 0x01 · level u8 · count uvarint · entry × count, a key being length
// uvarint · bytes.
func leafNode(kvs ...[]byte) []byte {
	b := binary.AppendUvarint([]byte{0x01, 0}, uint64(len(kvs)/2))
	for i := 0; i < len(kvs); i += 2 {
		b = binary.AppendUvarint(b, uint64(len(kvs[i])))
		b = append(b, kvs[i]...)
		b = append(b, 0x00) // inline value
		b = binary.AppendUvarint(b, uint64(len(kvs[i+1])))
		b = append(b, kvs[i+1]...)
	}
	return b
}

type child struct {
	key   string
	h     hash.Hash
	count uint64
}

func internalNode(level int, cs ...child) []byte {
	b := binary.AppendUvarint([]byte{0x01, byte(level)}, uint64(len(cs)))
	for _, c := range cs {
		b = binary.AppendUvarint(b, uint64(len(c.key)))
		b = append(b, c.key...)
		b = append(b, c.h[:]...)
		b = binary.AppendUvarint(b, c.count)
	}
	return b
}

// #24: a tree's root claims how many entries it holds, and the claim is
// only checked as iteration reaches each child. Reading a two-entry tree
// whose root claims 65,537 (its second child 65,536) must refuse it as corrupt without first
// allocating room for 65,537 entries; the honest tree reads within the
// same budget.
func TestRegression_SC24_ReadingATreeDoesNotSizeByItsClaimedCount(t *testing.T) {
	s := memstore.New()
	put := func(b []byte) hash.Hash {
		h, err := s.Put(ctx, b)
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	a, b := put(leafNode([]byte("a"), file("a").Encode())), put(leafNode([]byte("b"), file("b").Encode()))
	const budget = 512 << 10
	for name, claim := range map[string]uint64{"honest": 1, "claiming 65,536": 1 << 16} {
		root := model.Root{Hash: put(internalNode(1, child{"a", a, 1}, child{"b", b, claim})), Size: claim + 1, Format: tree.Format}
		var got map[string]tree.Entry
		var err error
		used := allocated(func() { got, err = tree.Read(ctx, s, cfg(), root) })
		switch {
		case claim == 1 && (err != nil || len(got) != 2 || got["a"] != file("a") || got["b"] != file("b")):
			t.Fatalf("positive control: the honest two-entry tree read as %d entries, %v", len(got), err)
		case claim > 1 && !errors.Is(err, chunk.ErrCorrupt):
			t.Fatalf("a two-entry tree whose root claims %d entries read as %d entries, %v; want ErrCorrupt", claim+1, len(got), err)
		case used > budget:
			t.Fatalf("reading a two-entry tree (%s) allocated %d bytes, want at most %d: room for the entries a root claims is not room it has earned",
				name, used, budget)
		}
	}
}

// #23: a tree's record is exactly tree.EntrySize bytes, so a stream value
// is never one. Reading, validating, walking or diffing a tree whose
// record is a 4 MiB stream must refuse it without reading the stream.
func TestRegression_SC23_ATreeReadsNoRecordLongerThanAnEntry(t *testing.T) {
	s := memstore.New()
	w := cfg()
	w.MaxValue = 8 << 20
	m, err := prolly.Empty(ctx, s, w)
	if err != nil {
		t.Fatal(err)
	}
	e := m.Editor()
	for _, p := range []string{"a", "b"} {
		if err := e.Put([]byte(p), file(p).Encode()); err != nil {
			t.Fatal(err)
		}
	}
	good, err := e.Flush(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Put([]byte("big"), noise("sc23 record", 4<<20)); err != nil {
		t.Fatal(err)
	}
	bad, err := e.Flush(ctx)
	if err != nil {
		t.Fatal(err)
	}
	goodRoot := model.Root{Hash: good.Root(), Size: good.Count(), Format: tree.Format}
	badRoot := model.Root{Hash: bad.Root(), Size: bad.Count(), Format: tree.Format}
	mdl := tree.Model{Config: cfg()}
	reads := map[string]func(model.Root) error{
		"Read":     func(r model.Root) error { _, err := tree.Read(ctx, s, cfg(), r); return err },
		"Validate": func(r model.Root) error { return mdl.Validate(ctx, r, s) },
		"Walk": func(r model.Root) error {
			return mdl.Walk(ctx, r, s, func(hash.Hash, bool) (bool, error) { return true, nil })
		},
		"Diff": func(r model.Root) error {
			d, err := mdl.Diff(ctx, goodRoot, r, s)
			for err == nil {
				var ok bool
				if _, ok, err = d.Next(ctx); !ok {
					break
				}
			}
			return err
		},
	}
	for how, read := range reads {
		if err := read(goodRoot); err != nil { // its files are one chunk each: named, never read
			t.Fatalf("positive control: %s of the honest tree: %v", how, err)
		}
		used := allocated(func() { err = read(badRoot) })
		if err == nil || used > 512<<10 {
			t.Errorf("%s of a tree with a 4 MiB record allocated %d bytes (%v); want it refused within 512 KiB", how, used, err)
		}
	}
}
