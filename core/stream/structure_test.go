package stream_test

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/memstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/stream"
)

// shape is a stream tree built from fuzz bytes (#23): a few data chunks and
// index nodes whose entries may name one child many times, a Ref (honest
// or not) and the most a reader will hold. Single-chunk fuzz targets
// cannot reach what repetition does to a reader; this one builds trees.
type shape struct {
	s       *memstore.Store
	data    map[hash.Hash][]byte
	nodes   map[hash.Hash]snode
	entries int // over every distinct index node stored
	ref     stream.Ref
	limit   uint64
}

type snode struct {
	level int
	es    []ientry
	enc   []byte
}

// source hands out fuzz bytes, zeros once they run out.
type source struct{ b []byte }

func (s *source) u8() byte {
	if len(s.b) == 0 {
		return 0
	}
	c := s.b[0]
	s.b = s.b[1:]
	return c
}

// buildShape reads: data chunks (count, then each length); index nodes
// (count; each a level and groups of child, repeats and size, where size 0
// is the child's own); flags (bit 0: the Ref's size is the next two bytes,
// bit 1: its depth is the next byte mod 4); the limit (two bytes, shifted
// by a third mod 8, at most 1 MiB).
func buildShape(t *testing.T, in []byte) shape {
	t.Helper()
	src := &source{b: in}
	sh := shape{s: memstore.New(), data: map[hash.Hash][]byte{}, nodes: map[hash.Hash]snode{}}
	store := func(b []byte) hash.Hash { return put(t, sh.s, b) }
	type built struct {
		h    hash.Hash
		size uint64
	}
	var data, level1 []built
	for i := range 1 + int(src.u8()%4) {
		n := int(src.u8())
		d := bytes.Repeat([]byte{0xAA, byte(i)}, n)[:n] // never 0x02 first: data is not an index node
		h := store(d)
		sh.data[h] = d
		data = append(data, built{h, uint64(n)})
	}
	root := data[0]
	depth := 0
	for range int(src.u8() % 5) {
		level := 1 + int(src.u8()%2)
		var es []ientry
		for range 1 + int(src.u8()%8) {
			sel, reps, size := int(src.u8()), 1+int(src.u8()), uint64(src.u8())
			from := data
			if level == 2 && len(level1) > 0 {
				from = level1
			}
			c := from[sel%len(from)]
			if size == 0 {
				size = c.size
			}
			for range reps {
				es = append(es, ientry{child: c.h, size: size})
			}
		}
		var sum uint64
		for _, e := range es {
			sum += e.size
		}
		enc := encodeIndex(level, es)
		h := store(enc)
		if _, ok := sh.nodes[h]; !ok {
			sh.entries += len(es)
		}
		sh.nodes[h] = snode{level: level, es: es, enc: enc}
		root, depth = built{h, sum}, level
		if level == 1 {
			level1 = append(level1, root)
		}
	}
	sh.ref = stream.Ref{Root: root.h, Size: root.size, Depth: uint8(depth)}
	flags := src.u8()
	if flags&1 != 0 {
		sh.ref.Size = uint64(src.u8())<<8 | uint64(src.u8())
	}
	if flags&2 != 0 {
		sh.ref.Depth = src.u8() % 4
	}
	sh.limit = min((uint64(src.u8())<<8|uint64(src.u8()))<<(src.u8()%8), 1<<20)
	return sh
}

// expand is the stream's bytes by the documented rules alone (DESIGN §7),
// or false where a reader must refuse it. It adds to *cost the bytes of
// every chunk it looks at, once for each place the chunk stands, up to the
// first it refuses: what a reader that reads each node once per place, and
// in order, must read.
func (sh shape) expand(h hash.Hash, depth int, want uint64, top bool, cost *uint64) ([]byte, bool) {
	d, isData := sh.data[h]
	n, isNode := sh.nodes[h]
	switch {
	case isData:
		*cost += uint64(len(d))
	case isNode:
		*cost += uint64(len(n.enc))
		d = n.enc // read as data, a node is its bytes
	}
	if depth == 0 {
		return d, (isData || isNode) && uint64(len(d)) == want
	}
	if !isNode || n.level != depth || (top && len(n.es) < 2) {
		return nil, false
	}
	var sum uint64
	for _, e := range n.es {
		if e.size == 0 {
			return nil, false
		}
		sum += e.size
	}
	if sum != want {
		return nil, false
	}
	var out []byte
	for _, e := range n.es {
		b, ok := sh.expand(e.child, depth-1, e.size, false, cost)
		if !ok {
			return nil, false
		}
		out = append(out, b...)
	}
	return out, true
}

// What any tree of chunks does to a reader: a stream over the limit is
// refused before it is read; one inside it reads back exactly as the
// documented rules expand it, or is corrupt; reading allocates in
// proportion to what is read; and a walk that goes into each index node
// once calls visit once per entry of the nodes it goes into, however often
// the tree names them.
func FuzzStreamStructure(f *testing.F) {
	// Three chunks under one node, read at a generous limit.
	f.Add([]byte{2, 10, 20, 30, 1, 0, 2, 0, 0, 0, 1, 0, 0, 2, 0, 0, 0, 0x10, 0, 0})
	// One chunk under one node 64 times: 16,320 bytes, at exactly that limit.
	f.Add([]byte{0, 255, 1, 0, 0, 0, 63, 0, 0, 0x3F, 0xC0, 0})
	// That node 16 times under another: 261,120 bytes, over a 4 KiB limit.
	f.Add([]byte{0, 255, 2, 0, 0, 0, 63, 0, 1, 0, 0, 15, 0, 0, 0x10, 0x00, 0})
	// A Ref claiming more than its tree, and one a level too deep.
	f.Add([]byte{1, 5, 9, 1, 0, 0, 0, 1, 0, 1, 0, 0xFF, 0x10, 0, 0})
	f.Add([]byte{1, 5, 9, 1, 0, 0, 0, 1, 0, 2, 2, 0x10, 0, 0})
	f.Fuzz(func(t *testing.T, in []byte) {
		sh := buildShape(t, in)
		var got []byte
		var err error
		used := allocated(func() { got, err = stream.ReadAll(ctx, sh.s, sh.ref, sh.limit) })
		want, valid, cost := []byte(nil), false, uint64(0)
		if sh.ref.Size <= sh.limit {
			if sh.ref.Size == 0 {
				d, ok := sh.data[sh.ref.Root]
				valid = ok && len(d) == 0 && sh.ref.Depth == 0
			} else {
				want, valid = sh.expand(sh.ref.Root, int(sh.ref.Depth), sh.ref.Size, true, &cost)
			}
		}
		switch {
		case sh.ref.Size > sh.limit:
			if !errors.Is(err, stream.ErrTooLarge) || used > 64<<10 {
				t.Fatalf("a %d-byte stream against a %d-byte limit: ReadAll = %d bytes, %v, after allocating %d bytes; want ErrTooLarge, unread",
					sh.ref.Size, sh.limit, len(got), err, used)
			}
		case valid:
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("a valid %d-byte stream read back as %d bytes (%v); the documented rules give %d (%x)", sh.ref.Size, len(got), err, len(want), sha256.Sum256(want))
			}
		default:
			if !errors.Is(err, chunk.ErrCorrupt) {
				t.Fatalf("a stream the documented rules refuse (Ref %+v): ReadAll = %d bytes, %v; want ErrCorrupt", sh.ref, len(got), err)
			}
		}
		// A read allocates what it reads, each chunk once per place it
		// stands (the store's copy, a node's decoded entries), and a buffer
		// growing to what it hands back (under four times that, summed over
		// its doublings), whatever the tree repeats.
		if bound := 4*uint64(len(got)) + 3*cost + 256<<10; used > bound {
			t.Fatalf("reading a %d-byte stream (Ref %+v, stored in %d chunks) allocated %d bytes; "+
				"its chunks, once per place they stand, are %d bytes; want at most %d", len(got), sh.ref, len(sh.data)+len(sh.nodes), used, cost, bound)
		}
		seen := map[hash.Hash]bool{}
		calls := 0
		_ = stream.Walk(ctx, sh.s, sh.ref, func(h hash.Hash, leaf bool) (bool, error) {
			calls++
			if leaf || seen[h] {
				return false, nil
			}
			seen[h] = true
			return true, nil
		})
		if calls > 1+sh.entries {
			t.Fatalf("a walk going into each of %d index nodes once called visit %d times; they hold %d entries", len(sh.nodes), calls, sh.entries)
		}
	})
}
