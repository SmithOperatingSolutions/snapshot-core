package stream_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"sync/atomic"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/boundary"
	"github.com/SmithOperatingSolutions/snapshot-core/core/cdc"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/memstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/stream"
)

var ctx = context.Background()

// small cuts little data into many chunks, so trees get deep quickly.
func small() stream.Config {
	return stream.Config{CDC: cdc.Geometry{Min: 256, Max: 8 << 10, Mask: 0x3FF}, Nodes: boundary.DefaultGeometry()}
}

func random(seed string, n int) []byte {
	out := make([]byte, 0, n+32)
	for i := 0; len(out) < n; i++ {
		h := sha256.Sum256([]byte(fmt.Sprintf("%s/%d", seed, i)))
		out = append(out, h[:]...)
	}
	return out[:n]
}

// counting is a memstore that counts the chunks a write adds.
type counting struct {
	*memstore.Store
	added atomic.Int64
}

func newStore() *counting { return &counting{Store: memstore.New()} }

func (c *counting) Put(ctx context.Context, data []byte) (hash.Hash, error) {
	h := hash.Sum(data)
	if have, err := c.Has(ctx, []hash.Hash{h}); err == nil && !have[h] {
		c.added.Add(1)
	}
	return c.Store.Put(ctx, data)
}

func write(t *testing.T, s chunk.Writer, data []byte, c stream.Config) stream.Ref {
	t.Helper()
	ref, err := stream.Write(ctx, s, bytes.NewReader(data), c)
	if err != nil {
		t.Fatalf("Write(%d bytes): %v", len(data), err)
	}
	return ref
}

// The index node format of DESIGN §7, written here independently of the
// package: 0x02 · level u8 · count uvarint · (child [32] · size uvarint) × count.
type ientry struct {
	child hash.Hash
	size  uint64
}

func encodeIndex(level int, es []ientry) []byte {
	b := []byte{0x02, byte(level)}
	b = binary.AppendUvarint(b, uint64(len(es)))
	for _, e := range es {
		b = append(b, e.child[:]...)
		b = binary.AppendUvarint(b, e.size)
	}
	return b
}

func decodeIndex(t *testing.T, b []byte) (int, []ientry) {
	t.Helper()
	if len(b) < 3 || b[0] != 0x02 {
		t.Fatalf("an index node does not start 0x02 (%d bytes)", len(b))
	}
	level := int(b[1])
	n, k := binary.Uvarint(b[2:])
	if k <= 0 {
		t.Fatal("an index node's count does not decode")
	}
	rest := b[2+k:]
	var es []ientry
	for i := uint64(0); i < n; i++ {
		if len(rest) < 32 {
			t.Fatal("an index node ends inside an entry")
		}
		var e ientry
		copy(e.child[:], rest[:32])
		size, k := binary.Uvarint(rest[32:])
		if k <= 0 {
			t.Fatal("an index entry's size does not decode")
		}
		e.size, rest = size, rest[32+k:]
		es = append(es, e)
	}
	if len(rest) != 0 {
		t.Fatalf("an index node has %d bytes past its entries", len(rest))
	}
	return level, es
}

// walk reads a stream by the documented format alone, checking levels and
// sizes on the way down, and returns its bytes and its index nodes' entry lengths per level.
func walk(t *testing.T, s chunk.Reader, h hash.Hash, depth int, want uint64, nodes map[int][]int) []byte {
	t.Helper()
	b, err := s.Get(ctx, h)
	if err != nil {
		t.Fatal(err)
	}
	if depth == 0 {
		if uint64(len(b)) != want {
			t.Fatalf("a data chunk is %d bytes, its parent says %d", len(b), want)
		}
		return b
	}
	level, es := decodeIndex(t, b)
	if level != depth {
		t.Fatalf("an index node at depth %d says level %d", depth, level)
	}
	nodes[level] = append(nodes[level], len(b)-2-len(binary.AppendUvarint(nil, uint64(len(es)))))
	var out []byte
	var sum uint64
	for _, e := range es {
		sum += e.size
		out = append(out, walk(t, s, e.child, depth-1, e.size, nodes)...)
	}
	if sum != want {
		t.Fatalf("a level-%d node's entries add to %d bytes, its parent says %d", level, sum, want)
	}
	return out
}

func TestRoundTripAtEverySize(t *testing.T) {
	for _, n := range []int{0, 1, 255, 256, 5000, 64 << 10, 1 << 20, 3 << 20} {
		s := newStore()
		data := random(fmt.Sprintf("size-%d", n), n)
		ref := write(t, s, data, small())
		if ref.Size != uint64(n) {
			t.Fatalf("a %d-byte stream has Ref.Size %d", n, ref.Size)
		}
		got, err := stream.ReadAll(ctx, s, ref)
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("a %d-byte stream read back as %d bytes (%v)", n, len(got), err)
		}
		r, err := stream.Open(ctx, s, ref)
		if err != nil {
			t.Fatal(err)
		}
		if got, err := io.ReadAll(r); err != nil || !bytes.Equal(got, data) || r.Size() != int64(n) {
			t.Fatalf("reading a %d-byte stream through Read gave %d bytes (%v), Size %d", n, len(got), err, r.Size())
		}
	}
}

// A one-piece stream is its own root, and the empty stream is the empty chunk.
func TestAOnePieceStreamIsItsOwnRoot(t *testing.T) {
	s := newStore()
	empty := write(t, s, nil, small())
	if empty != (stream.Ref{Root: sha256.Sum256(nil), Size: 0, Depth: 0}) {
		t.Fatalf("the empty stream is %+v, want the empty chunk at depth 0", empty)
	}
	if b, err := s.Get(ctx, empty.Root); err != nil || len(b) != 0 {
		t.Fatalf("the empty stream's chunk is not stored: %d bytes, %v", len(b), err)
	}
	data := random("one piece", 200) // under the minimum cut
	if ref := write(t, s, data, small()); ref != (stream.Ref{Root: sha256.Sum256(data), Size: 200, Depth: 0}) {
		t.Fatalf("a one-piece stream is %+v, want its data chunk at depth 0", ref)
	}
}

// What the writer stores is the documented format, readable without the
// package; index nodes obey the split rule's bounds; no top node has one child.
func TestWriterEmitsTheDocumentedIndexFormat(t *testing.T) {
	s := newStore()
	data := random("documented", 2<<20)
	ref := write(t, s, data, small())
	if ref.Depth < 2 {
		t.Fatalf("fixture: 2 MiB in small chunks made a stream of depth %d, want at least 2", ref.Depth)
	}
	nodes := map[int][]int{}
	if got := walk(t, s, ref.Root, int(ref.Depth), ref.Size, nodes); !bytes.Equal(got, data) {
		t.Fatal("the stream read by the documented format differs from what was written")
	}
	top, _ := s.Get(ctx, ref.Root)
	if _, es := decodeIndex(t, top); len(es) < 2 {
		t.Fatalf("the top index node has %d children, want at least 2", len(es))
	}
	for level, sizes := range nodes {
		for i, n := range sizes[:len(sizes)-1] { // the last node of a level may be short
			if n < 512 || n > 16<<10+64 {
				t.Errorf("level %d node %d has %d bytes of entries, outside 512 B .. 16 KiB", level, i, n)
			}
		}
	}
}

// A stream built by hand in the documented format reads back, whole and at an offset.
func TestReaderReadsTheDocumentedFormat(t *testing.T) {
	s := newStore()
	ref := handBuilt(t, s)
	if got, err := stream.ReadAll(ctx, s, ref); err != nil || string(got) != "alphabetagamma" {
		t.Fatalf("ReadAll of a hand-built stream = %q, %v", got, err)
	}
	r, err := stream.Open(ctx, s, ref)
	if err != nil {
		t.Fatal(err)
	}
	p := make([]byte, 5)
	if n, err := r.ReadAt(p, 3); n != 5 || err != nil || string(p) != "habet" {
		t.Fatalf("ReadAt(3, 5) = %q, %d, %v; want \"habet\"", p[:n], n, err)
	}
}

func put(t *testing.T, s chunk.Writer, b []byte) hash.Hash {
	t.Helper()
	h, err := s.Put(ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func handBuilt(t *testing.T, s chunk.Writer) stream.Ref {
	t.Helper()
	var es []ientry
	for _, w := range []string{"alpha", "beta", "gamma"} {
		es = append(es, ientry{child: put(t, s, []byte(w)), size: uint64(len(w))})
	}
	return stream.Ref{Root: put(t, s, encodeIndex(1, es)), Size: 14, Depth: 1}
}

// Every size and level is checked against what is actually there.
func TestForgedStreamsAreCorrupt(t *testing.T) {
	s := newStore()
	good := handBuilt(t, s)
	if got, err := stream.ReadAll(ctx, s, good); err != nil || string(got) != "alphabetagamma" {
		t.Fatalf("positive control: %q, %v", got, err)
	}
	a, b, c := put(t, s, []byte("alpha")), put(t, s, []byte("beta")), put(t, s, []byte("gamma"))
	node := func(level int, es ...ientry) hash.Hash { return put(t, s, encodeIndex(level, es)) }
	ok := []ientry{{a, 5}, {b, 4}, {c, 5}}
	inner := node(1, ok...)
	for name, ref := range map[string]stream.Ref{
		"a child longer than its entry":  {Root: node(1, ientry{a, 4}, ientry{b, 4}, ientry{c, 5}), Size: 13, Depth: 1},
		"a child shorter than its entry": {Root: node(1, ientry{a, 6}, ientry{b, 4}, ientry{c, 5}), Size: 15, Depth: 1},
		"ref size disagrees":             {Root: good.Root, Size: 15, Depth: 1},
		"ref depth too deep":             {Root: good.Root, Size: 14, Depth: 2},
		"ref depth too shallow":          {Root: good.Root, Size: 14, Depth: 0},
		"node claims level 2":            {Root: node(2, ok...), Size: 14, Depth: 2},
		"a single-child top":             {Root: node(2, ientry{inner, 14}), Size: 14, Depth: 2},
		"a node with no entries":         {Root: node(1), Size: 0, Depth: 1},
		"wrong kind byte":                {Root: put(t, s, append([]byte{0x01}, encodeIndex(1, ok)[1:]...)), Size: 14, Depth: 1},
		"a byte past the entries":        {Root: put(t, s, append(encodeIndex(1, ok), 0)), Size: 14, Depth: 1},
		"truncated":                      {Root: put(t, s, encodeIndex(1, ok)[:40]), Size: 14, Depth: 1},
	} {
		_, err := stream.ReadAll(ctx, s, ref)
		if !errors.Is(err, chunk.ErrCorrupt) {
			t.Errorf("%s: ReadAll = %v, want ErrCorrupt", name, err)
		}
	}
}

func TestRandomAccessMatchesTheSource(t *testing.T) {
	s := newStore()
	data := random("random access", 1<<20)
	ref := write(t, s, data, small())
	r, err := stream.Open(ctx, s, ref)
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewPCG(1, 2))
	for i := 0; i < 300; i++ {
		off := rng.IntN(len(data))
		p := make([]byte, 1+rng.IntN(20000))
		n, err := r.ReadAt(p, int64(off))
		want := data[off:min(off+len(p), len(data))]
		if n != len(want) || !bytes.Equal(p[:n], want) {
			t.Fatalf("ReadAt(%d, %d) returned %d bytes, want %d matching the source", off, len(p), n, len(want))
		}
		if short := n < len(p); short != errors.Is(err, io.EOF) {
			t.Fatalf("ReadAt(%d, %d) = %d bytes, %v: io.EOF exactly when short", off, len(p), n, err)
		}
	}
	if n, err := r.ReadAt(make([]byte, 1), int64(len(data))); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("ReadAt at the end = %d, %v; want 0, io.EOF", n, err)
	}
}

// Content-defined all the way up: one inserted byte rewrites a few data
// chunks and a node or two per level, not the stream.
func TestAnInsertedByteRewritesAFewChunks(t *testing.T) {
	s := newStore()
	data := random("insert", 2<<20)
	ref := write(t, s, data, small())
	if s.added.Load() < 100 || ref.Depth < 2 {
		t.Fatalf("fixture: the first write added %d chunks at depth %d, want hundreds and depth 2+", s.added.Load(), ref.Depth)
	}
	s.added.Store(0)
	edited := append(append(bytes.Clone(data[:100<<10]), 'x'), data[100<<10:]...)
	ref2 := write(t, s, edited, small())
	if got, _ := stream.ReadAll(ctx, s, ref2); !bytes.Equal(got, edited) {
		t.Fatal("the edited stream reads back wrong")
	}
	if n, limit := s.added.Load(), int64(3+2*int(ref2.Depth)); n > limit {
		t.Fatalf("inserting one byte added %d chunks, want at most %d (3 data + 2 per index level)", n, limit)
	}
}

func TestIdenticalContentIsTheSameStream(t *testing.T) {
	s := newStore()
	data := random("same", 300<<10)
	ref := write(t, s, data, small())
	if s.added.Load() == 0 || ref.Size != uint64(len(data)) {
		t.Fatalf("fixture: the first write added %d chunks, size %d", s.added.Load(), ref.Size)
	}
	s.added.Store(0)
	if again := write(t, s, bytes.Clone(data), small()); again != ref || s.added.Load() != 0 {
		t.Fatalf("the same bytes written again are %+v (was %+v) and added %d chunks", again, ref, s.added.Load())
	}
}

// Three forgeries that would read back without any error if their check
// were missing: a Ref claiming fewer bytes than its tree holds, an entry of
// zero bytes (a second encoding of the same stream), and data that is itself
// a well-formed index node, read one level too deep.
func TestForgeriesThatWouldReadCleanly(t *testing.T) {
	s := newStore()
	good := handBuilt(t, s)
	a, b, c := put(t, s, []byte("alpha")), put(t, s, []byte("beta")), put(t, s, []byte("gamma"))
	// An index node whose entries add up to its own length, stored as data.
	crafted := encodeIndex(1, []ientry{{put(t, s, random("x", 40)), 40}, {put(t, s, random("y", 29)), 29}})
	if len(crafted) != 69 {
		t.Fatalf("fixture: the crafted node is %d bytes, want 69", len(crafted))
	}
	d := put(t, s, crafted)
	twice := stream.Ref{Root: put(t, s, encodeIndex(1, []ientry{{d, 69}, {d, 69}})), Size: 138, Depth: 1}
	if got, err := stream.ReadAll(ctx, s, twice); err != nil || !bytes.Equal(got, append(bytes.Clone(crafted), crafted...)) {
		t.Fatalf("positive control: a stream whose data looks like an index node read as %d bytes (%v)", len(got), err)
	}
	for name, ref := range map[string]stream.Ref{
		"ref size smaller than the tree": {Root: good.Root, Size: 13, Depth: 1},
		"a zero-length entry":            {Root: put(t, s, encodeIndex(1, []ientry{{a, 5}, {b, 0}, {b, 4}, {c, 5}})), Size: 14, Depth: 1},
		"data read as an index node":     {Root: twice.Root, Size: 138, Depth: 2},
	} {
		if got, err := stream.ReadAll(ctx, s, ref); !errors.Is(err, chunk.ErrCorrupt) {
			t.Errorf("%s: ReadAll = %d bytes, %v; want ErrCorrupt", name, len(got), err)
		}
	}
}
