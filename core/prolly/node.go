package prolly

import (
	"bytes"
	"fmt"
	"math/bits"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/stream"
	"github.com/SmithOperatingSolutions/snapshot-core/core/wire"
)

// Node: 0x01 · level u8 · count uvarint · entry × count (docs/DESIGN.md §7).
const (
	kindNode  = 0x01
	maxLevel  = 63
	valInline = 0x00
	valStream = 0x01
)

// value is a leaf entry's value as stored: inline bytes, or a stream for a
// value over the inline limit.
type value struct {
	inline []byte
	ref    *stream.Ref
}

func (v value) equal(o value) bool {
	if (v.ref == nil) != (o.ref == nil) {
		return false
	}
	if v.ref != nil {
		return *v.ref == *o.ref
	}
	return bytes.Equal(v.inline, o.inline)
}

// entry is one entry of a node: a key and value at level 0; above, the
// last key of a child, its hash, and the leaf entries under it.
type entry struct {
	key   []byte
	val   value
	child hash.Hash
	count uint64
}

type node struct {
	level   int
	entries []entry
}

// total is the number of leaf entries under n.
func (n *node) total() uint64 {
	if n.level == 0 {
		return uint64(len(n.entries))
	}
	var t uint64
	for _, e := range n.entries {
		t += e.count
	}
	return t
}

func (n *node) lastKey() []byte { return n.entries[len(n.entries)-1].key }

func (n *node) encode() []byte {
	var w wire.Writer
	w.U8(kindNode)
	w.U8(uint8(n.level))
	w.Uvarint(uint64(len(n.entries)))
	for _, e := range n.entries {
		w.LenBytes(e.key)
		switch {
		case n.level > 0:
			w.Raw(e.child[:])
			w.Uvarint(e.count)
		case e.val.ref != nil:
			w.U8(valStream)
			w.Raw(e.val.ref.Root[:])
			w.Uvarint(e.val.ref.Size)
			w.U8(e.val.ref.Depth)
		default:
			w.U8(valInline)
			w.LenBytes(e.val.inline)
		}
	}
	return w.Bytes()
}

// entrySize is e's encoded length at level: the size the split rule counts.
func entrySize(level int, e entry) int {
	n := uvarintLen(uint64(len(e.key))) + len(e.key)
	switch {
	case level > 0:
		return n + hash.Size + uvarintLen(e.count)
	case e.val.ref != nil:
		return n + 1 + hash.Size + uvarintLen(e.val.ref.Size) + 1
	default:
		return n + 1 + uvarintLen(uint64(len(e.val.inline))) + len(e.val.inline)
	}
}

func uvarintLen(v uint64) int { return (bits.Len64(v|1) + 6) / 7 }

func corrupt(format string, args ...any) error {
	return fmt.Errorf("%w: prolly: %s", chunk.ErrCorrupt, fmt.Sprintf(format, args...))
}

// decodeNode parses a node and enforces its canonical forms: keys strictly
// increasing and at most MaxKeySize; a value at or under the inline limit
// inline and a longer one a stream; internal entries counting at least one
// leaf entry, their sum inside 64 bits; only a leaf may be empty; nothing
// after the last entry. Keys and values alias b.
func decodeNode(b []byte, inlineLimit int) (*node, error) {
	r := wire.NewReader(b)
	kind, level, count := r.U8(), int(r.U8()), r.Uvarint()
	if r.Err() != nil || kind != kindNode || level > maxLevel || count > uint64(len(b)) || (level > 0 && count == 0) {
		return nil, corrupt("node header")
	}
	n := &node{level: level, entries: make([]entry, 0, count)}
	var sum, carry uint64
	for i := uint64(0); i < count; i++ {
		var e entry
		e.key = r.LenBytes(MaxKeySize)
		switch {
		case level > 0:
			copy(e.child[:], r.Fixed(hash.Size))
			e.count = r.Uvarint()
			sum, carry = bits.Add64(sum, e.count, 0)
			if e.count == 0 || carry != 0 {
				return nil, corrupt("entry %d counts %d leaf entries", i, e.count)
			}
		default:
			switch tag := r.U8(); tag {
			case valInline:
				e.val.inline = r.LenBytes(inlineLimit)
			case valStream:
				var ref stream.Ref
				copy(ref.Root[:], r.Fixed(hash.Size))
				ref.Size, ref.Depth = r.Uvarint(), r.U8()
				if r.Err() == nil && (ref.Size <= uint64(inlineLimit) || ref.Depth > maxLevel) {
					return nil, corrupt("entry %d: a %d-byte stream value (inline limit %d)", i, ref.Size, inlineLimit)
				}
				e.val.ref = &ref
			default:
				return nil, corrupt("entry %d: value tag %#x", i, tag)
			}
		}
		if r.Err() != nil {
			return nil, corrupt("entry %d: %v", i, r.Err())
		}
		if i > 0 && bytes.Compare(n.entries[i-1].key, e.key) >= 0 {
			return nil, corrupt("keys not strictly increasing at entry %d", i)
		}
		n.entries = append(n.entries, e)
	}
	if err := r.Done(); err != nil {
		return nil, corrupt("node: %v", err)
	}
	return n, nil
}
