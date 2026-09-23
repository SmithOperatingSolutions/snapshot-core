package prolly

import "context"

// cursor is a position in one level of a tree, with its parent's position
// above it up to the root. Moving past the end of a node moves the parent
// and reads the next node; every read is checked against the parent entry.
type cursor struct {
	m      *Map
	n      *node
	i      int
	parent *cursor
	end    bool // past the level's last entry
}

// seek descends from the root to level, at each level above it into the
// child whose range holds key (the last child for a key past them all, the
// first for nil), and returns a cursor at the start of that level-node.
func (m *Map) seek(ctx context.Context, level int, key []byte) (*cursor, error) {
	n, err := m.read(ctx, m.root)
	if err != nil {
		return nil, err
	}
	c := &cursor{m: m, n: n}
	for c.n.level > level {
		c.i = min(search(c.n, key), len(c.n.entries)-1)
		child, err := m.child(ctx, c.n, c.i)
		if err != nil {
			return nil, err
		}
		c = &cursor{m: m, n: child, parent: c}
	}
	return c, nil
}

func (c *cursor) valid() bool    { return !c.end && c.i < len(c.n.entries) }
func (c *cursor) current() entry { return c.n.entries[c.i] }
func (c *cursor) atNodeEnd() bool {
	return c.i == len(c.n.entries)-1
}

// advance moves to the next entry of the level.
func (c *cursor) advance(ctx context.Context) error {
	if c.end {
		return nil
	}
	c.i++
	if c.i < len(c.n.entries) {
		return nil
	}
	return c.nextNode(ctx)
}

// nextNode moves to the first entry of the level's next node; at the end of
// the level the cursor is no longer valid.
func (c *cursor) nextNode(ctx context.Context) error {
	if c.parent == nil {
		c.end = true
		return nil
	}
	if err := c.parent.advance(ctx); err != nil {
		return err
	}
	if !c.parent.valid() {
		c.end = true
		return nil
	}
	return c.fetch(ctx)
}

// fetch reads the child the parent points at and starts at its first entry.
func (c *cursor) fetch(ctx context.Context) error {
	n, err := c.m.child(ctx, c.parent.n, c.parent.i)
	if err != nil {
		return err
	}
	c.n, c.i = n, 0
	return nil
}

// lastNode reports whether c's node is its level's last.
func (c *cursor) lastNode() bool {
	for p := c.parent; p != nil; p = p.parent {
		if p.i < len(p.n.entries)-1 {
			return false
		}
	}
	return true
}
