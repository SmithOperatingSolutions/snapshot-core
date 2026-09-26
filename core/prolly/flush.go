package prolly

import (
	"bytes"
	"context"

	"github.com/SmithOperatingSolutions/snapshot-core/core/boundary"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
)

// edit is a put (e) or a delete of key at one level: user edits at level 0,
// and above, the nodes a level below replaced (keyed by their last keys).
type edit struct {
	key []byte
	del bool
	e   entry
}

// apply writes edits into the tree level by level (docs/DESIGN.md §7,
// "Editing and diff"). Below the root each level re-chunks only the
// stretches its edits touch; the root level is re-chunked whole, and the
// tree then grows or collapses to its canonical top. The result is the tree
// a bulk build of the same entries draws.
func (m *Map) apply(ctx context.Context, edits []edit, changed *[][]byte) (*Map, error) {
	root, err := m.read(ctx, m.root)
	if err != nil {
		return nil, err
	}
	for level := 0; level < root.level; level++ {
		if edits, err = m.applyLevel(ctx, level, edits, changed); err != nil {
			return nil, err
		}
		if len(edits) == 0 {
			return m, nil // nothing at this level changed, so nothing above does
		}
	}
	return m.applyRoot(ctx, root, edits, changed)
}

// chunker draws one level's nodes from its entries, storing each node as
// it ends and keeping the entries a level up that name them.
type chunker struct {
	ctx      context.Context
	m        *Map
	level    int
	split    *boundary.Splitter
	pending  []entry
	out      []entry // one per stored node: its last key, hash and count
	nodes    []*node // kept, not stored, when hold is set
	hold     bool
	existing map[hash.Hash]bool // nodes already stored: drawing one again writes nothing
	// changed collects, at level 0, the keys whose values the edits
	// change: what a Diff of the old tree against the new one reports.
	changed *[][]byte
}

func (m *Map) newChunker(ctx context.Context, level int, hold bool) *chunker {
	return &chunker{ctx: ctx, m: m, level: level, split: m.rule.Splitter(level), hold: hold}
}

func (c *chunker) add(e entry) error {
	c.pending = append(c.pending, e)
	if !c.split.Append(hash.Sum(e.key), entrySize(c.level, e)) {
		return nil
	}
	return c.end()
}

// log has a level-0 chunker collect the keys its merge changes into changed.
func (c *chunker) log(changed *[][]byte) {
	if c.level == 0 {
		c.changed = changed
	}
}

func (c *chunker) change(key []byte) {
	if c.changed != nil {
		*c.changed = append(*c.changed, key)
	}
}

// end closes the node being filled, if it has entries.
func (c *chunker) end() error {
	if len(c.pending) == 0 {
		return nil
	}
	n := &node{level: c.level, entries: c.pending}
	c.pending = nil
	if c.hold {
		c.nodes = append(c.nodes, n)
		return nil
	}
	e, err := c.m.store(c.ctx, n, c.existing)
	if err != nil {
		return err
	}
	c.out = append(c.out, e)
	return nil
}

// store writes n, unless it is one of existing, and returns the entry a
// level up that names it.
func (m *Map) store(ctx context.Context, n *node, existing map[hash.Hash]bool) (entry, error) {
	b := n.encode()
	h := hash.Sum(b)
	if !existing[h] {
		put := m.s.Put
		if rw, ok := m.s.(chunk.RawWriter); ok {
			// Nodes are hashes and short keys: zstd saves little on them
			// and costs its encoder on every commit (#42, D17).
			put = rw.PutRaw
		}
		if _, err := put(ctx, b); err != nil {
			return entry{}, err
		}
	}
	return entry{key: n.lastKey(), child: h, count: n.total()}, nil
}

// merge feeds c the entries of old with the edits whose keys are at most
// limit (all remaining edits when limit is nil) applied, in key order, and
// returns the edits not yet used.
func merge(c *chunker, old []entry, edits []edit, limit []byte) ([]edit, error) {
	j := 0
	for j < len(old) || (len(edits) > 0 && (limit == nil || bytes.Compare(edits[0].key, limit) <= 0)) {
		var cmp int
		switch {
		case len(edits) == 0 || (limit != nil && bytes.Compare(edits[0].key, limit) > 0):
			cmp = 1 // only old entries left in this node
		case j == len(old):
			cmp = -1
		default:
			cmp = bytes.Compare(edits[0].key, old[j].key)
		}
		var err error
		switch {
		case cmp < 0: // a new key
			if !edits[0].del {
				c.change(edits[0].key)
				err = c.add(edits[0].e)
			}
			edits = edits[1:]
		case cmp == 0: // an edited key
			if edits[0].del || !edits[0].e.val.equal(old[j].val) {
				c.change(edits[0].key)
			}
			if !edits[0].del {
				err = c.add(edits[0].e)
			}
			edits, j = edits[1:], j+1
		default:
			err = c.add(old[j])
			j++
		}
		if err != nil {
			return nil, err
		}
	}
	return edits, nil
}

// applyLevel applies edits to a level below the root and returns the edits
// they make a level up. Each stretch starts at the node holding its first
// edit (the boundary before it is untouched) and re-chunks old nodes until
// a new node ends exactly where an old one did: from there the old nodes
// are the same, and are skipped to the next edit.
func (m *Map) applyLevel(ctx context.Context, level int, edits []edit, changed *[][]byte) ([]edit, error) {
	var up []edit
	for len(edits) > 0 {
		c, err := m.seek(ctx, level, edits[0].key)
		if err != nil {
			return nil, err
		}
		ch := m.newChunker(ctx, level, false)
		ch.existing = map[hash.Hash]bool{} // a node drawn again ends where its old self did
		ch.log(changed)
		var removed []entry
		for {
			removed = append(removed, entry{key: c.n.lastKey(), child: nodeHash(c), count: c.n.total()})
			ch.existing[nodeHash(c)] = true
			var limit []byte
			if !c.lastNode() {
				limit = c.n.lastKey()
			}
			if edits, err = merge(ch, c.n.entries, edits, limit); err != nil {
				return nil, err
			}
			if len(ch.pending) == 0 {
				break // resynced: the old and new trees agree from the next node on
			}
			if limit == nil {
				if err := ch.end(); err != nil { // the level's last node
					return nil, err
				}
				break
			}
			if err := c.nextNode(ctx); err != nil {
				return nil, err
			}
		}
		up = append(up, replace(removed, ch.out)...)
	}
	return up, nil
}

// nodeHash is the hash of the node c is in, from its parent's entry.
func nodeHash(c *cursor) hash.Hash { return c.parent.n.entries[c.parent.i].child }

// replace turns the old nodes a stretch consumed and the new nodes it drew
// into edits a level up, both lists in key order: a new node with an old
// one's last key replaces it, and one identical to it is no edit at all.
func replace(removed, added []entry) []edit {
	var out []edit
	i, j := 0, 0
	for i < len(removed) || j < len(added) {
		switch {
		case j == len(added) || (i < len(removed) && bytes.Compare(removed[i].key, added[j].key) < 0):
			out = append(out, edit{key: removed[i].key, del: true})
			i++
		case i == len(removed) || bytes.Compare(removed[i].key, added[j].key) > 0:
			out = append(out, edit{key: added[j].key, e: added[j]})
			j++
		default:
			if removed[i].child != added[j].child || removed[i].count != added[j].count {
				out = append(out, edit{key: added[j].key, e: added[j]})
			}
			i, j = i+1, j+1
		}
	}
	return out
}

// applyRoot re-chunks the root node with its edits, then grows the tree
// until a level has one node, or collapses single-child roots.
func (m *Map) applyRoot(ctx context.Context, root *node, edits []edit, changed *[][]byte) (*Map, error) {
	ch := m.newChunker(ctx, root.level, true)
	ch.log(changed)
	if _, err := merge(ch, root.entries, edits, nil); err != nil {
		return nil, err
	}
	if err := ch.end(); err != nil {
		return nil, err
	}
	switch {
	case len(ch.nodes) == 0: // everything was deleted
		return Empty(ctx, m.s, m.cfg)
	case len(ch.nodes) == 1:
		n := ch.nodes[0]
		for n.level > 0 && len(n.entries) == 1 { // no single-child root
			c, err := m.child(ctx, n, 0)
			if err != nil {
				return nil, err
			}
			n = c
		}
		e, err := m.store(ctx, n, nil)
		if err != nil {
			return nil, err
		}
		return m.with(e.child, n), nil
	}
	up := make([]entry, 0, len(ch.nodes))
	for _, n := range ch.nodes {
		e, err := m.store(ctx, n, nil)
		if err != nil {
			return nil, err
		}
		up = append(up, e)
	}
	for level := root.level + 1; len(up) > 1; level++ { // grow: the levels above are new
		next := m.newChunker(ctx, level, false)
		for _, e := range up {
			if err := next.add(e); err != nil {
				return nil, err
			}
		}
		if err := next.end(); err != nil {
			return nil, err
		}
		up = next.out
	}
	top, err := m.read(ctx, up[0].child)
	if err != nil {
		return nil, err
	}
	return m.with(up[0].child, top), nil
}

func (m *Map) with(root hash.Hash, n *node) *Map {
	return &Map{s: m.s, cfg: m.cfg, rule: m.rule, root: root, count: n.total(), height: n.level, top: n}
}
