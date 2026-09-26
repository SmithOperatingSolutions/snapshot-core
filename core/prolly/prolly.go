// Package prolly is the ordered key-value map of Engine Spec L1: an
// immutable tree whose shape depends only on its contents (docs/DESIGN.md
// §7). The same entries make the same root, whatever order they were
// written in, so equal maps are equal hashes and a diff can skip every
// subtree the two sides share.
//
// Reads verify the tree as they descend: every node by SHA-256 (the chunk
// store) and its canonical encoding (decodeNode), and every child against
// its parent's entry: one level down, not empty, ending at the entry's key,
// holding the entry's count, and starting after the previous entry's key.
package prolly

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/SmithOperatingSolutions/snapshot-core/core/boundary"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/stream"
)

// MaxKeySize is the longest key (Engine Spec L1).
const MaxKeySize = 4 << 10

// maxInlineLimit keeps a leaf of one inline value and one key inside a chunk.
const maxInlineLimit = 512 << 10

// DefaultMaxValue is the longest value a map whose Config names no limit
// holds.
const DefaultMaxValue = 64 << 20

// maxValue is the longest value the map holds.
func (c Config) maxValue() int {
	if c.MaxValue == 0 {
		return DefaultMaxValue
	}
	return c.MaxValue
}

// ErrValueTooLarge is returned for a value over the map's MaxValue.
var ErrValueTooLarge = errors.New("prolly: value over the map's limit")

// ErrKeyTooLarge is returned for a key over MaxKeySize; the edit is not applied.
var ErrKeyTooLarge = errors.New("prolly: key over 4 KiB")

// Config is the repo geometry a map is written with.
type Config struct {
	Nodes       boundary.Geometry // how nodes are split
	InlineLimit int               // longer values are stored as streams
	Stream      stream.Config     // how those streams are written
	// MaxValue is the longest value the map holds: its editor refuses a
	// longer one, and its reads (Get, iteration, Diff, Walk's values)
	// refuse a longer stream before reading it, since a stream's length
	// can be many times what it stores (#23). 0 is DefaultMaxValue. It is
	// not repo geometry: nothing stored depends on it.
	MaxValue int
}

// DefaultConfig is the default repo geometry: 512 B / 4 KiB / 16 KiB nodes,
// values inline up to 256 KiB.
func DefaultConfig() Config {
	return Config{Nodes: boundary.DefaultGeometry(), InlineLimit: 256 << 10, Stream: stream.DefaultConfig()}
}

func (c Config) rule() (boundary.Rule, error) {
	if c.InlineLimit < 0 || c.InlineLimit > maxInlineLimit {
		return boundary.Rule{}, fmt.Errorf("prolly: inline limit %d outside 0..%d", c.InlineLimit, maxInlineLimit)
	}
	if c.MaxValue < 0 {
		return boundary.Rule{}, fmt.Errorf("prolly: value limit %d is negative", c.MaxValue)
	}
	return boundary.New(c.Nodes)
}

// Map is an immutable ordered map over a chunk store.
type Map struct {
	s      chunk.ReadWriter
	cfg    Config
	rule   boundary.Rule
	root   hash.Hash
	count  uint64
	height int
	// top is the root node, decoded and checked when the map was made:
	// the map is immutable, so every operation starts from it rather than
	// reading the root again.
	top *node
	// flushed is set on a map a flush made: the root it edited and the
	// keys it changed, in key order.
	flushed bool
	base    hash.Hash
	changed [][]byte
}

// Empty returns the empty map, storing its one node.
func Empty(ctx context.Context, s chunk.ReadWriter, c Config) (*Map, error) {
	rule, err := c.rule()
	if err != nil {
		return nil, err
	}
	h, err := s.Put(ctx, (&node{}).encode())
	if err != nil {
		return nil, err
	}
	return &Map{s: s, cfg: c, rule: rule, root: h, top: &node{}}, nil
}

// Open returns the map whose root node is root.
func Open(ctx context.Context, s chunk.ReadWriter, c Config, root hash.Hash) (*Map, error) {
	rule, err := c.rule()
	if err != nil {
		return nil, err
	}
	m := &Map{s: s, cfg: c, rule: rule, root: root}
	n, err := m.read(ctx, root)
	if err != nil {
		return nil, err
	}
	if n.level > 0 && len(n.entries) < 2 {
		return nil, corrupt("the root has one child")
	}
	m.count, m.height, m.top = n.total(), n.level, n
	return m, nil
}

// Root is the hash of the root node.
func (m *Map) Root() hash.Hash { return m.root }

// Count is the number of entries.
func (m *Map) Count() uint64 { return m.count }

// Height is the root node's level: 0 for a map that fits in one leaf.
func (m *Map) Height() int { return m.height }

func (m *Map) read(ctx context.Context, h hash.Hash) (*node, error) {
	if h == m.root && m.top != nil {
		return m.top, nil
	}
	b, err := m.s.Get(ctx, h)
	if err != nil {
		return nil, err
	}
	return decodeNode(b, m.cfg.InlineLimit)
}

// child reads the child of p's entry i and checks it against the entry.
func (m *Map) child(ctx context.Context, p *node, i int) (*node, error) {
	c, err := m.read(ctx, p.entries[i].child)
	if err != nil {
		return nil, err
	}
	if err := checkChild(p, i, c); err != nil {
		return nil, err
	}
	return c, nil
}

// checkChild checks c, the child of p's entry i, against the entry.
func checkChild(p *node, i int, c *node) error {
	e := p.entries[i]
	switch {
	case c.level != p.level-1:
		return corrupt("a level-%d node's child is level %d", p.level, c.level)
	case len(c.entries) == 0:
		return corrupt("a level-%d node's child is empty", p.level)
	case !bytes.Equal(c.lastKey(), e.key):
		return corrupt("a level-%d entry's key is not its child's last key", p.level)
	case c.total() != e.count:
		return corrupt("a level-%d entry counts %d entries, its child holds %d", p.level, e.count, c.total())
	case i > 0 && bytes.Compare(c.entries[0].key, p.entries[i-1].key) <= 0:
		return corrupt("a level-%d node's children overlap", p.level)
	}
	return nil
}

// materialize returns a value's bytes, reading a stream for a long one.
func (m *Map) materialize(ctx context.Context, v value) ([]byte, error) {
	if v.ref == nil {
		return bytes.Clone(v.inline), nil
	}
	return readValue(ctx, m.s, *v.ref, m.cfg.maxValue())
}

// readValue reads a long value's stream, refusing one over limit unread.
func readValue(ctx context.Context, rd chunk.Reader, ref stream.Ref, limit int) ([]byte, error) {
	b, err := stream.ReadAll(ctx, rd, ref, uint64(limit))
	if errors.Is(err, stream.ErrTooLarge) {
		return nil, fmt.Errorf("%w: %w", ErrValueTooLarge, err)
	}
	return b, err
}

// search is the first entry of n whose key is at least key.
func search(n *node, key []byte) int {
	return sort.Search(len(n.entries), func(i int) bool { return bytes.Compare(n.entries[i].key, key) >= 0 })
}

// Get returns key's value.
func (m *Map) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	n, err := m.read(ctx, m.root)
	if err != nil {
		return nil, false, err
	}
	for n.level > 0 {
		i := search(n, key)
		if i == len(n.entries) {
			return nil, false, nil
		}
		if n, err = m.child(ctx, n, i); err != nil {
			return nil, false, err
		}
	}
	i := search(n, key)
	if i == len(n.entries) || !bytes.Equal(n.entries[i].key, key) {
		return nil, false, nil
	}
	v, err := m.materialize(ctx, n.entries[i].val)
	if err != nil {
		return nil, false, err
	}
	return v, true, nil
}

// Iter walks entries in key order.
type Iter struct {
	ctx context.Context
	c   *cursor
	hi  []byte
}

// IterRange walks the keys in [lo, hi); nil is unbounded.
func (m *Map) IterRange(ctx context.Context, lo, hi []byte) (*Iter, error) {
	c, err := m.seek(ctx, 0, lo)
	if err != nil {
		return nil, err
	}
	c.i = search(c.n, lo)
	if c.i == len(c.n.entries) && len(c.n.entries) > 0 {
		c.i--
		if err := c.advance(ctx); err != nil {
			return nil, err
		}
	}
	return &Iter{ctx: ctx, c: c, hi: hi}, nil
}

// Next returns the next entry; ok is false at the end.
func (it *Iter) Next() (key, val []byte, ok bool, err error) {
	if !it.c.valid() {
		return nil, nil, false, nil
	}
	e := it.c.current()
	if it.hi != nil && bytes.Compare(e.key, it.hi) >= 0 {
		return nil, nil, false, nil
	}
	if val, err = it.c.m.materialize(it.ctx, e.val); err != nil {
		return nil, nil, false, err
	}
	if err := it.c.advance(it.ctx); err != nil {
		return nil, nil, false, err
	}
	return bytes.Clone(e.key), val, true, nil
}

// Editor collects puts and deletes against a map.
type Editor struct {
	m     *Map
	edits map[string]*[]byte // nil: delete
}

// Editor starts editing m.
func (m *Map) Editor() *Editor { return &Editor{m: m, edits: map[string]*[]byte{}} }

// Put sets key to val. A value over the map's MaxValue is
// ErrValueTooLarge, and the edit is not applied.
func (e *Editor) Put(key, val []byte) error {
	if len(key) > MaxKeySize {
		return fmt.Errorf("%w: %d bytes", ErrKeyTooLarge, len(key))
	}
	if limit := e.m.cfg.maxValue(); len(val) > limit {
		return fmt.Errorf("%w: %d bytes, the limit is %d", ErrValueTooLarge, len(val), limit)
	}
	v := bytes.Clone(val)
	if v == nil {
		v = []byte{}
	}
	e.edits[string(key)] = &v
	return nil
}

// Delete removes key.
func (e *Editor) Delete(key []byte) error {
	if len(key) > MaxKeySize {
		return fmt.Errorf("%w: %d bytes", ErrKeyTooLarge, len(key))
	}
	e.edits[string(key)] = nil
	return nil
}

// Flush writes the edits and returns the new map; the editor then edits it.
func (e *Editor) Flush(ctx context.Context) (*Map, error) {
	keys := make([]string, 0, len(e.edits))
	for k := range e.edits {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	edits := make([]edit, 0, len(keys))
	for _, k := range keys {
		ed := edit{key: []byte(k)}
		if p := e.edits[k]; p == nil {
			ed.del = true
		} else {
			v := value{inline: *p}
			if len(*p) > e.m.cfg.InlineLimit {
				ref, err := stream.Write(ctx, e.m.s, bytes.NewReader(*p), e.m.cfg.Stream)
				if err != nil {
					return nil, err
				}
				v = value{ref: &ref}
			}
			ed.e = entry{key: ed.key, val: v}
		}
		edits = append(edits, ed)
	}
	var changed [][]byte
	next, err := e.m.apply(ctx, edits, &changed)
	if err != nil {
		return nil, err
	}
	// apply may hand back e.m itself (nothing changed), which others hold:
	// the record of this flush goes on a copy.
	m := *next
	m.flushed, m.base, m.changed = true, e.m.root, changed
	e.m, e.edits = &m, map[string]*[]byte{}
	return &m, nil
}

// Changes reports what the flush that made m changed: the root of the map
// it edited and the keys whose values differ between the two, in key
// order: what Diff of the two reports. ok is false for a map no flush made
// (Open, Empty), for which a caller must diff.
func (m *Map) Changes() (base hash.Hash, keys [][]byte, ok bool) {
	if !m.flushed {
		return hash.Hash{}, nil, false
	}
	keys = make([][]byte, len(m.changed))
	for i, k := range m.changed {
		keys[i] = bytes.Clone(k)
	}
	return m.base, keys, true
}
