package prolly_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
	"github.com/SmithOperatingSolutions/snapshot-core/core/stream"
)

// The node format of DESIGN §7, written here independently of the package:
//
//	node            0x01 · level u8 · count uvarint · entry × count
//	leaf entry      key · value
//	internal entry  key · child [32] · entries uvarint
//	key             length uvarint · bytes
//	value           0x00 · length uvarint · bytes | 0x01 · root [32] · size uvarint · depth u8
type tvalue struct {
	isRef  bool
	inline []byte
	ref    stream.Ref
}

type tnode struct {
	level  int
	keys   [][]byte
	vals   []tvalue    // level 0
	kids   []hash.Hash // level ≥ 1
	counts []uint64    // level ≥ 1
}

func encodeNode(n tnode) []byte {
	b := []byte{0x01, byte(n.level)}
	b = binary.AppendUvarint(b, uint64(len(n.keys)))
	for i, k := range n.keys {
		b = binary.AppendUvarint(b, uint64(len(k)))
		b = append(b, k...)
		if n.level == 0 {
			v := n.vals[i]
			if v.isRef {
				b = append(b, 0x01)
				b = append(b, v.ref.Root[:]...)
				b = binary.AppendUvarint(b, v.ref.Size)
				b = append(b, v.ref.Depth)
			} else {
				b = append(b, 0x00)
				b = binary.AppendUvarint(b, uint64(len(v.inline)))
				b = append(b, v.inline...)
			}
			continue
		}
		b = append(b, n.kids[i][:]...)
		b = binary.AppendUvarint(b, n.counts[i])
	}
	return b
}

type byteReader struct {
	t tb
	b []byte
}

func (r *byteReader) uvarint() uint64 {
	v, k := binary.Uvarint(r.b)
	if k <= 0 {
		r.t.Fatal("a node's uvarint does not decode")
	}
	r.b = r.b[k:]
	return v
}

func (r *byteReader) take(n uint64) []byte {
	if uint64(len(r.b)) < n {
		r.t.Fatalf("a node ends %d bytes early", n-uint64(len(r.b)))
	}
	out := r.b[:n]
	r.b = r.b[n:]
	return out
}

func decodeNode(t tb, b []byte) tnode {
	t.Helper()
	if len(b) < 3 || b[0] != 0x01 {
		t.Fatalf("a node does not start 0x01 (%d bytes)", len(b))
	}
	n := tnode{level: int(b[1])}
	r := &byteReader{t: t, b: b[2:]}
	count := r.uvarint()
	for i := uint64(0); i < count; i++ {
		n.keys = append(n.keys, r.take(r.uvarint()))
		if n.level == 0 {
			switch tag := r.take(1)[0]; tag {
			case 0x00:
				n.vals = append(n.vals, tvalue{inline: r.take(r.uvarint())})
			case 0x01:
				var v tvalue
				v.isRef = true
				copy(v.ref.Root[:], r.take(32))
				v.ref.Size = r.uvarint()
				v.ref.Depth = r.take(1)[0]
				n.vals = append(n.vals, v)
			default:
				t.Fatalf("a leaf value has tag %#x", tag)
			}
			continue
		}
		var h hash.Hash
		copy(h[:], r.take(32))
		n.kids = append(n.kids, h)
		n.counts = append(n.counts, r.uvarint())
	}
	if len(r.b) != 0 {
		t.Fatalf("a node has %d bytes past its entries", len(r.b))
	}
	return n
}

// walk reads a tree by the documented format alone, checking every
// invariant of DESIGN §7 on the way, and returns its entry count and its
// first and last keys; sizes collects each level's node sizes in key order.
func walk(t tb, s *counting, h hash.Hash, level int, prev []byte, sizes map[int][]int) (uint64, []byte, []byte) {
	t.Helper()
	b, err := s.Store.Get(ctx, h) // not counted
	if err != nil {
		t.Fatal(err)
	}
	sizes[level] = append(sizes[level], len(b))
	n := decodeNode(t, b)
	if n.level != level {
		t.Fatalf("a node at level %d says level %d", level, n.level)
	}
	if len(n.keys) == 0 {
		t.Fatal("a node of a non-empty map has no entries")
	}
	for i := 1; i < len(n.keys); i++ {
		if bytes.Compare(n.keys[i-1], n.keys[i]) >= 0 {
			t.Fatalf("a level-%d node's keys are not strictly increasing at %d", level, i)
		}
	}
	if prev != nil && bytes.Compare(n.keys[0], prev) <= 0 {
		t.Fatalf("a level-%d node starts at or before the key its left neighbour ends with", level)
	}
	if level == 0 {
		return uint64(len(n.keys)), n.keys[0], n.keys[len(n.keys)-1]
	}
	var total uint64
	var first []byte
	left := prev
	for i := range n.keys {
		count, f, l := walk(t, s, n.kids[i], level-1, left, sizes)
		if !bytes.Equal(l, n.keys[i]) {
			t.Fatalf("a level-%d entry's key is %q, its child's last key %q", level, n.keys[i], l)
		}
		if count != n.counts[i] {
			t.Fatalf("a level-%d entry counts %d entries, its child holds %d", level, n.counts[i], count)
		}
		if i == 0 {
			first = f
		}
		total += count
		left = l
	}
	return total, first, n.keys[len(n.keys)-1]
}

func leafValues(t *testing.T, s *counting, root hash.Hash) map[string]tvalue {
	t.Helper()
	out := map[string]tvalue{}
	var visit func(h hash.Hash)
	visit = func(h hash.Hash) {
		b, err := s.Store.Get(ctx, h)
		if err != nil {
			t.Fatal(err)
		}
		n := decodeNode(t, b)
		for i, k := range n.keys {
			if n.level == 0 {
				out[string(k)] = n.vals[i]
			} else {
				visit(n.kids[i])
			}
		}
	}
	visit(root)
	return out
}

// What the package stores is the documented format, readable without it:
// levels, key order, internal keys and counts, and a multi-child top.
func TestNodesAreTheDocumentedFormat(t *testing.T) {
	s := newStore()
	m := build(t, s, 20000, "a")
	if m.Height() < 2 {
		t.Fatalf("fixture: height %d, want at least 2", m.Height())
	}
	count, first, last := walk(t, s, m.Root(), m.Height(), nil, map[int][]int{})
	if count != 20000 || m.Count() != 20000 || !bytes.Equal(first, key(0)) || !bytes.Equal(last, key(19999)) {
		t.Fatalf("the documented walk found %d entries %q..%q; Count says %d", count, first, last, m.Count())
	}
	top, _ := s.Store.Get(ctx, m.Root())
	if n := decodeNode(t, top); len(n.keys) < 2 {
		t.Fatalf("the root has %d entries, want at least 2 (no single-child root)", len(n.keys))
	}
}

func store(t *testing.T, s *counting, n tnode) hash.Hash {
	t.Helper()
	h, err := s.Store.Put(ctx, encodeNode(n))
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func leaf(kvs ...string) tnode {
	n := tnode{}
	for i := 0; i+1 < len(kvs); i += 2 {
		n.keys = append(n.keys, []byte(kvs[i]))
		n.vals = append(n.vals, tvalue{inline: []byte(kvs[i+1])})
	}
	return n
}

func internal(level int, entries ...any) tnode {
	n := tnode{level: level}
	for i := 0; i+2 < len(entries); i += 3 {
		n.keys = append(n.keys, []byte(entries[i].(string)))
		n.kids = append(n.kids, entries[i+1].(hash.Hash))
		n.counts = append(n.counts, uint64(entries[i+2].(int)))
	}
	return n
}

// A tree built by hand in the documented format opens and reads.
func TestTheReaderReadsTheDocumentedFormat(t *testing.T) {
	s := newStore()
	l1, l2 := store(t, s, leaf("a", "1", "b", "2")), store(t, s, leaf("c", "3"))
	root := store(t, s, internal(1, "b", l1, 2, "c", l2, 1))
	m, err := prolly.Open(ctx, s, prolly.DefaultConfig(), root)
	if err != nil {
		t.Fatal(err)
	}
	if m.Count() != 3 || m.Height() != 1 {
		t.Fatalf("a hand-built map has count %d, height %d; want 3, 1", m.Count(), m.Height())
	}
	if v, ok := get(t, m, []byte("c")); !ok || string(v) != "3" {
		t.Fatalf("Get(c) = %q, %v", v, ok)
	}
	if got := all(t, m, nil, nil); len(got) != 3 || got[0] != (kv{"a", "1"}) || got[2] != (kv{"c", "3"}) {
		t.Fatalf("iterating a hand-built map gave %v", got)
	}
}

// readAll opens a map and reads every entry every way; the first error.
func readAll(s *counting, c prolly.Config, root hash.Hash) error {
	m, err := prolly.Open(ctx, s, c, root)
	if err != nil {
		return err
	}
	it, err := m.IterRange(ctx, nil, nil)
	if err != nil {
		return err
	}
	for {
		k, _, ok, err := it.Next()
		if err != nil || !ok {
			return err
		}
		if _, _, err := m.Get(ctx, k); err != nil {
			return err
		}
	}
}

// Every rule the decoder and the tree walk enforce, one forgery each.
func TestForgedTreesAreCorrupt(t *testing.T) {
	s := newStore()
	c := prolly.DefaultConfig()
	c.InlineLimit = 100
	l1, l2 := store(t, s, leaf("a", "1", "b", "2")), store(t, s, leaf("c", "3"))
	if err := readAll(s, c, store(t, s, internal(1, "b", l1, 2, "c", l2, 1))); err != nil {
		t.Fatalf("positive control: %v", err)
	}
	ac, bd := store(t, s, leaf("a", "1", "c", "3")), store(t, s, leaf("b", "2", "d", "4"))
	emptyLeaf := store(t, s, leaf())
	raw := func(b []byte) hash.Hash {
		h, err := s.Store.Put(ctx, b)
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	longKey := string(bytes.Repeat([]byte("k"), prolly.MaxKeySize+1))
	shortRef := tnode{keys: [][]byte{[]byte("k")}, vals: []tvalue{{isRef: true, ref: stream.Ref{Size: 50}}}}
	for name, root := range map[string]hash.Hash{
		"leaf keys out of order":            store(t, s, leaf("b", "2", "a", "1")),
		"leaf keys repeat":                  store(t, s, leaf("a", "1", "a", "2")),
		"entry key is not the child's last": store(t, s, internal(1, "bb", l1, 2, "c", l2, 1)),
		"entry count disagrees":             store(t, s, internal(1, "b", l1, 3, "c", l2, 1)),
		"child level is not one below":      store(t, s, internal(2, "b", l1, 2, "c", l2, 1)),
		"children overlap":                  store(t, s, internal(1, "c", ac, 2, "d", bd, 2)),
		"a child with no entries":           store(t, s, internal(1, "b", l1, 2, "c", emptyLeaf, 0)),
		"inline value over the limit":       store(t, s, leaf("k", string(bytes.Repeat([]byte("v"), 101)))),
		"stream ref within the limit":       store(t, s, shortRef),
		"a key over 4 KiB":                  store(t, s, leaf(longKey, "v")),
		"an internal node with no entries":  store(t, s, internal(1)),
		"a single-child root":               store(t, s, internal(1, "b", l1, 2)),
		"bytes past the entries":            raw(append(encodeNode(leaf("a", "1")), 0)),
		"wrong kind byte":                   raw(append([]byte{0x02}, encodeNode(leaf("a", "1"))[1:]...)),
		"level over 63":                     raw([]byte{0x01, 64, 0x00}),
		"truncated":                         raw(encodeNode(leaf("a", "1"))[:5]),
	} {
		if err := readAll(s, c, root); !errors.Is(err, chunk.ErrCorrupt) {
			t.Errorf("%s: reading = %v, want ErrCorrupt", name, err)
		}
	}
}
