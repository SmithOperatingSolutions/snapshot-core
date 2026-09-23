package prolly

import (
	"context"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/stream"
)

// Walk calls visit for every chunk the map at root is made of, the root
// first: its nodes, each checked as a read checks it, and the chunks of its
// long values' streams; visit says whether to go on into what a chunk
// reaches. When value is not nil, each value of a leaf Walk goes into is
// handed to it, long ones read whole, so a caller can walk what values name.
func Walk(ctx context.Context, rd chunk.Reader, c Config, root hash.Hash, visit func(hash.Hash) (bool, error), value func(key, val []byte) error) error {
	if _, err := c.rule(); err != nil {
		return err
	}
	w := walker{ctx: ctx, rd: rd, limit: c.InlineLimit, visit: visit, value: value}
	deeper, err := visit(root)
	if err != nil || !deeper {
		return err
	}
	n, err := w.read(root)
	if err != nil {
		return err
	}
	if n.level > 0 && len(n.entries) < 2 {
		return corrupt("the root has one child")
	}
	return w.node(n)
}

type walker struct {
	ctx   context.Context
	rd    chunk.Reader
	limit int
	visit func(hash.Hash) (bool, error)
	value func(key, val []byte) error
}

func (w *walker) read(h hash.Hash) (*node, error) {
	b, err := w.rd.Get(w.ctx, h)
	if err != nil {
		return nil, err
	}
	return decodeNode(b, w.limit)
}

// node walks what n reaches: its children, or its values.
func (w *walker) node(n *node) error {
	for i, e := range n.entries {
		if n.level == 0 {
			if err := w.leafValue(e); err != nil {
				return err
			}
			continue
		}
		deeper, err := w.visit(e.child)
		if err != nil {
			return err
		}
		if !deeper {
			continue
		}
		c, err := w.read(e.child)
		if err != nil {
			return err
		}
		if err := checkChild(n, i, c); err != nil {
			return err
		}
		if err := w.node(c); err != nil {
			return err
		}
	}
	return nil
}

// leafValue walks a long value's stream, then hands the value over.
func (w *walker) leafValue(e entry) error {
	if e.val.ref != nil {
		if err := stream.Walk(w.ctx, w.rd, *e.val.ref, w.visit); err != nil {
			return err
		}
	}
	if w.value == nil {
		return nil
	}
	v := e.val.inline
	if e.val.ref != nil {
		var err error
		if v, err = stream.ReadAll(w.ctx, w.rd, *e.val.ref); err != nil {
			return err
		}
	}
	return w.value(e.key, v)
}
