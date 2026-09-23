// Package prolly is the ordered key-value map of Engine Spec L1: an
// immutable tree whose shape depends only on its contents (docs/DESIGN.md
// §7). The same entries make the same root, whatever order they were
// written in, so equal maps are equal hashes and a diff can skip every
// subtree the two sides share.
package prolly

import (
	"context"
	"errors"

	"github.com/SmithOperatingSolutions/snapshot-core/core/boundary"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/stream"
)

// MaxKeySize is the longest key (Engine Spec L1).
const MaxKeySize = 4 << 10

// ErrKeyTooLarge is returned for a key over MaxKeySize; the edit is not applied.
var ErrKeyTooLarge = errors.New("prolly: key over 4 KiB")

// Config is the repo geometry a map is written with.
type Config struct {
	Nodes       boundary.Geometry // how nodes are split
	InlineLimit int               // longer values are stored as streams
	Stream      stream.Config     // how those streams are written
}

// DefaultConfig is the default repo geometry: 512 B / 4 KiB / 16 KiB nodes,
// values inline up to 256 KiB.
func DefaultConfig() Config { return Config{} }

// Map is an immutable ordered map over a chunk store.
type Map struct{}

// Empty returns the empty map, storing its one node.
func Empty(ctx context.Context, s chunk.ReadWriter, c Config) (*Map, error) { return &Map{}, nil }

// Open returns the map whose root node is root.
func Open(ctx context.Context, s chunk.ReadWriter, c Config, root hash.Hash) (*Map, error) {
	return &Map{}, nil
}

// Root is the hash of the root node.
func (m *Map) Root() hash.Hash { return hash.Hash{} }

// Count is the number of entries.
func (m *Map) Count() uint64 { return 0 }

// Height is the root node's level: 0 for a map that fits in one leaf.
func (m *Map) Height() int { return 0 }

// Get returns key's value.
func (m *Map) Get(ctx context.Context, key []byte) ([]byte, bool, error) { return nil, false, nil }

// Iter walks entries in key order.
type Iter struct{}

// IterRange walks the keys in [lo, hi); nil is unbounded.
func (m *Map) IterRange(ctx context.Context, lo, hi []byte) (*Iter, error) { return &Iter{}, nil }

// Next returns the next entry; ok is false at the end.
func (it *Iter) Next() (key, val []byte, ok bool, err error) { return nil, nil, false, nil }

// Editor collects puts and deletes against a map.
type Editor struct{}

// Editor starts editing m.
func (m *Map) Editor() *Editor { return &Editor{} }

// Put sets key to val.
func (e *Editor) Put(key, val []byte) error { return nil }

// Delete removes key.
func (e *Editor) Delete(key []byte) error { return nil }

// Flush writes the edits and returns the new map; the editor then edits it.
func (e *Editor) Flush(ctx context.Context) (*Map, error) { return &Map{}, nil }

// ChangeKind says how an entry differs between two maps.
type ChangeKind uint8

// Change kinds.
const (
	Added ChangeKind = iota + 1
	Removed
	Modified
)

// Change is one entry that differs.
type Change struct {
	Kind     ChangeKind
	Key      []byte
	From, To []byte // the values on each side; nil where absent
}

// DiffIter walks the changes from one map to another in key order.
type DiffIter struct{}

// Diff compares from with to.
func Diff(ctx context.Context, from, to *Map) (*DiffIter, error) { return &DiffIter{}, nil }

// Next returns the next change; ok is false at the end.
func (d *DiffIter) Next() (Change, bool, error) { return Change{}, false, nil }

type node struct{}

func decodeNode(b []byte, inlineLimit int) (*node, error) { return nil, errors.New("prolly: not yet") }

func (n *node) encode() []byte { return nil }
