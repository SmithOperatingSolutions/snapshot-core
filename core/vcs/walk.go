package vcs

import (
	"context"
	"errors"
	"strings"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/object"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
)

// Walk calls visit for every chunk the repository whose refs map is root
// reaches (docs/DESIGN.md §9): the map's nodes; each branch's commits back to
// the first, their namespaces and what those name; each tag and the commit
// it names; each working set, its namespaces and, during a merge, its base
// and theirs commits and its conflicts map, whose records name objects too.
// The version graph's own chunks (commits, tags, working sets) are read once
// each whatever visit answers, so one named in two roles is caught.
func Walk(ctx context.Context, rd chunk.Reader, o Options, root hash.Hash, visit func(h hash.Hash, leaf bool) (bool, error)) error {
	if o.Registry == nil {
		return errors.New("vcs: a repository needs a model registry")
	}
	w := &walker{ctx: ctx, rd: rd, o: o, visit: visit, kinds: map[hash.Hash]byte{}}
	err := prolly.Walk(ctx, rd, o.Config, root, visit, func(key, val []byte) error {
		if len(val) != hash.Size {
			return corrupt("ref %s holds %d bytes", key, len(val))
		}
		h := hash.Hash(val)
		switch k := string(key); {
		case strings.HasPrefix(k, "heads/"):
			w.commits = append(w.commits, h)
			return nil
		case strings.HasPrefix(k, "tags/"):
			return w.tag(h)
		case strings.HasPrefix(k, "work/"):
			return w.workingSet(h)
		}
		return corrupt("a ref %q the version graph does not write", key)
	})
	if err != nil {
		return err
	}
	return w.history()
}

type walker struct {
	ctx     context.Context
	rd      chunk.Reader
	o       Options
	visit   func(h hash.Hash, leaf bool) (bool, error)
	commits []hash.Hash        // commits named but not yet read
	kinds   map[hash.Hash]byte // the graph's chunks read so far, as what
}

// enter names h, one of the version graph's own chunks, and returns its
// bytes the first time it is named as kind (nil after that); named before as
// another kind, it is corrupt.
func (w *walker) enter(h hash.Hash, kind byte) ([]byte, error) {
	if k, ok := w.kinds[h]; ok {
		if k != kind {
			return nil, corrupt("chunk %s is named as kinds %#x and %#x", h.Short(), k, kind)
		}
		return nil, nil
	}
	w.kinds[h] = kind
	if _, err := w.visit(h, false); err != nil {
		return nil, err
	}
	return w.rd.Get(w.ctx, h)
}

func (w *walker) namespace(root hash.Hash) error {
	return object.Walk(w.ctx, w.rd, w.o.Config, w.o.Registry, root, w.visit)
}

func (w *walker) tag(h hash.Hash) error {
	b, err := w.enter(h, kindTag)
	if err != nil || b == nil {
		return err
	}
	t, err := decodeTag(b)
	if err != nil {
		return err
	}
	w.commits = append(w.commits, t.Target)
	return nil
}

func (w *walker) workingSet(h hash.Hash) error {
	b, err := w.enter(h, kindWorkingSet)
	if err != nil || b == nil {
		return err
	}
	ws, err := decodeWorkingSet(b)
	if err != nil {
		return err
	}
	for _, ns := range []hash.Hash{ws.Working, ws.Staged} {
		if err := w.namespace(ns); err != nil {
			return err
		}
	}
	if ws.Merge == nil {
		return nil
	}
	for _, ns := range []hash.Hash{ws.Merge.PreWorking, ws.Merge.PreStaged} {
		if err := w.namespace(ns); err != nil {
			return err
		}
	}
	w.commits = append(w.commits, ws.Merge.Base, ws.Merge.Theirs)
	return prolly.Walk(w.ctx, w.rd, w.o.Config, ws.Merge.Conflicts, w.visit, func(key, val []byte) error {
		c, err := decodeConflict(string(key), val)
		if err != nil {
			return err
		}
		for _, side := range []object.Ref{c.Base, c.Ours, c.Theirs} {
			if side == (object.Ref{}) {
				continue
			}
			if err := object.WalkRef(w.ctx, w.rd, w.o.Registry, side, w.visit); err != nil {
				return err
			}
		}
		return nil
	})
}

// history reads every commit named, and their parents, once each.
func (w *walker) history() error {
	for len(w.commits) > 0 {
		h := w.commits[len(w.commits)-1]
		w.commits = w.commits[:len(w.commits)-1]
		b, err := w.enter(h, kindCommit)
		if err != nil {
			return err
		}
		if b == nil {
			continue
		}
		c, err := DecodeCommit(b)
		if err != nil {
			return err
		}
		if err := w.namespace(c.Namespace); err != nil {
			return err
		}
		w.commits = append(w.commits, c.Parents...)
	}
	return nil
}
