package prolly

import (
	"bytes"
	"context"
)

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
type DiffIter struct {
	ctx      context.Context
	from, to *cursor // nil for maps with equal roots
}

// Diff compares from with to. Equal roots differ nowhere and read nothing;
// otherwise both trees are walked in key order, and wherever the two sides'
// parent entries match (same key, same child) the whole child is skipped
// without reading it, at the highest level where they match.
func Diff(ctx context.Context, from, to *Map) (*DiffIter, error) {
	d := &DiffIter{ctx: ctx}
	if from.root == to.root {
		return d, nil
	}
	var err error
	if d.from, err = from.seek(ctx, 0, nil); err != nil {
		return nil, err
	}
	if d.to, err = to.seek(ctx, 0, nil); err != nil {
		return nil, err
	}
	return d, nil
}

// Next returns the next change; ok is false at the end.
func (d *DiffIter) Next() (Change, bool, error) {
	if d.from == nil {
		return Change{}, false, nil
	}
	for {
		fv, tv := d.from.valid(), d.to.valid()
		if !fv && !tv {
			return Change{}, false, nil
		}
		cmp := 0
		switch {
		case !tv:
			cmp = -1
		case !fv:
			cmp = 1
		default:
			cmp = bytes.Compare(d.from.current().key, d.to.current().key)
		}
		switch {
		case cmp < 0:
			return d.send(Removed, d.from, nil)
		case cmp > 0:
			return d.send(Added, nil, d.to)
		case !d.from.current().val.equal(d.to.current().val):
			return d.send(Modified, d.from, d.to)
		}
		if err := d.from.advance(d.ctx); err != nil {
			return Change{}, false, err
		}
		if err := d.to.advance(d.ctx); err != nil {
			return Change{}, false, err
		}
		if err := skipCommon(d.ctx, d.from, d.to); err != nil {
			return Change{}, false, err
		}
	}
}

// send reports the entries under from and to (either may be nil) and moves past them.
func (d *DiffIter) send(kind ChangeKind, from, to *cursor) (Change, bool, error) {
	ch := Change{Kind: kind}
	for _, side := range []struct {
		c   *cursor
		dst *[]byte
	}{{from, &ch.From}, {to, &ch.To}} {
		if side.c == nil {
			continue
		}
		e := side.c.current()
		ch.Key = bytes.Clone(e.key)
		v, err := side.c.m.materialize(d.ctx, e.val)
		if err != nil {
			return Change{}, false, err
		}
		*side.dst = v
		if err := side.c.advance(d.ctx); err != nil {
			return Change{}, false, err
		}
	}
	return ch, true, nil
}

// equalItems reports whether two cursors at the same level are at the same
// entry: the same key and value, or above the leaves, the same child.
func equalItems(a, b *cursor) bool {
	x, y := a.current(), b.current()
	if !bytes.Equal(x.key, y.key) {
		return false
	}
	if a.n.level > 0 {
		return x.child == y.child
	}
	return x.val.equal(y.val)
}

// skipCommon advances both cursors past the entries they share, climbing to
// the parents whenever they point at the same child, so shared subtrees are
// skipped whole at the highest level where they are shared.
func skipCommon(ctx context.Context, a, b *cursor) error {
	parentsAreNew := true
	for a.valid() && b.valid() && equalItems(a, b) {
		if parentsAreNew {
			if a.parent != nil && b.parent != nil && a.parent.valid() && b.parent.valid() && equalItems(a.parent, b.parent) {
				if err := skipCommonParents(ctx, a, b); err != nil {
					return err
				}
				continue
			}
		}
		parentsAreNew = a.atNodeEnd() || b.atNodeEnd()
		if err := a.advance(ctx); err != nil {
			return err
		}
		if err := b.advance(ctx); err != nil {
			return err
		}
	}
	return nil
}

// skipCommonParents skips the shared children at the parent level, then
// starts both cursors at the first entries of the children they reach.
func skipCommonParents(ctx context.Context, a, b *cursor) error {
	if err := skipCommon(ctx, a.parent, b.parent); err != nil {
		return err
	}
	for _, c := range []*cursor{a, b} {
		if !c.parent.valid() {
			c.end = true
			continue
		}
		if err := c.fetch(ctx); err != nil {
			return err
		}
	}
	return nil
}
