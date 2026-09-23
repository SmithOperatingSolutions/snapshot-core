// Package merge is the generic three-way merge of namespaces (docs/DESIGN.md
// §8; Engine Spec L3). Both sides' diffs from base stream in path order and
// are zipped, so memory stays bounded however large the namespaces are. The
// driver decides what it can alone and asks an object's model only when both
// sides changed the same object under one model.
package merge

import (
	"context"
	"errors"
	"fmt"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/object"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
)

// DefaultMaxConflicts is the Engine Spec's limit.
const DefaultMaxConflicts = 100_000

// ErrTooManyConflicts aborts a merge past its conflict limit.
var ErrTooManyConflicts = errors.New("merge: too many conflicts")

// Kind says why a path conflicts.
type Kind uint8

// Conflict kinds.
const (
	BothChanged Kind = iota + 1 // one model's object changed on both sides, and the model could not combine them
	DeleteEdit                  // deleted on one side, changed on the other
	AddAdd                      // added differently on both sides
	ModelChange                 // the sides disagree about which model owns the path
)

// Conflict is one path the merge could not decide.
type Conflict struct {
	Path               string
	Kind               Kind
	Base, Ours, Theirs object.Ref // zero where absent
	Model              []model.Conflict
}

// Stats counts what a merge did.
type Stats struct {
	Added, Removed, Modified, Conflicts int
}

// Result is a merge's outcome. Merged holds ours with every change that
// could be decided applied; at a conflicting path it keeps ours.
type Result struct {
	Merged    *object.Namespace
	Conflicts []Conflict
	Stats     Stats
}

// Options tunes a merge.
type Options struct {
	MaxConflicts int // 0: DefaultMaxConflicts
}

// Merge merges theirs into ours, over base. When one side did not change,
// the other is the result and nothing is read. Otherwise the two diffs from
// base are zipped: what only theirs changed is applied to ours, what only
// ours changed is already there, and a path both changed is decided by the
// Engine Spec's table, asking the model when base, ours and theirs share
// one. A model's error, or more than MaxConflicts conflicts, aborts it.
func Merge(ctx context.Context, reg *model.Registry, base, ours, theirs *object.Namespace, rw chunk.ReadWriter, o Options) (Result, error) {
	if o.MaxConflicts == 0 {
		o.MaxConflicts = DefaultMaxConflicts
	}
	switch {
	case base.Root() == theirs.Root() || ours.Root() == theirs.Root():
		return Result{Merged: ours}, nil
	case base.Root() == ours.Root():
		return Result{Merged: theirs}, nil
	}
	dOurs, err := object.Diff(ctx, base, ours)
	if err != nil {
		return Result{}, err
	}
	dTheirs, err := object.Diff(ctx, base, theirs)
	if err != nil {
		return Result{}, err
	}
	z := &zipper{ctx: ctx, reg: reg, rw: rw, ed: ours.Editor()}
	co, okO, err := dOurs.Next()
	if err != nil {
		return Result{}, err
	}
	ct, okT, err := dTheirs.Next()
	for (okO || okT) && err == nil {
		switch {
		case !okT || (okO && co.Path < ct.Path): // only ours changed it
			co, okO, err = dOurs.Next()
		case !okO || ct.Path < co.Path: // only theirs changed it
			if err = z.take(ct); err == nil {
				ct, okT, err = dTheirs.Next()
			}
		default: // both changed it
			if err = z.both(co, ct); err == nil && len(z.res.Conflicts) > o.MaxConflicts {
				err = fmt.Errorf("%w: more than %d", ErrTooManyConflicts, o.MaxConflicts)
			}
			if err == nil {
				if co, okO, err = dOurs.Next(); err == nil {
					ct, okT, err = dTheirs.Next()
				}
			}
		}
	}
	if err != nil {
		return Result{}, err
	}
	if z.res.Merged, err = z.ed.Flush(ctx); err != nil {
		return Result{}, err
	}
	z.res.Stats.Conflicts = len(z.res.Conflicts)
	return z.res, nil
}

type zipper struct {
	ctx context.Context
	reg *model.Registry
	rw  chunk.ReadWriter
	ed  *object.Editor
	res Result
}

// take applies theirs' change to a path ours left alone.
func (z *zipper) take(c object.Change) error {
	switch c.Kind {
	case prolly.Removed:
		z.res.Stats.Removed++
		return z.ed.Delete(c.Path)
	case prolly.Added:
		z.res.Stats.Added++
	default:
		z.res.Stats.Modified++
	}
	return z.ed.Put(c.Path, c.To)
}

// both decides a path both sides changed.
func (z *zipper) both(ours, theirs object.Change) error {
	oGone, tGone := ours.Kind == prolly.Removed, theirs.Kind == prolly.Removed
	switch {
	case oGone && tGone, !oGone && !tGone && ours.To == theirs.To:
		return nil // the same change on both sides: ours already has it
	case oGone || tGone:
		return z.conflict(DeleteEdit, ours, theirs, nil)
	case ours.Kind == prolly.Added:
		return z.conflict(AddAdd, ours, theirs, nil)
	case ours.From.Model != ours.To.Model || theirs.To.Model != ours.To.Model:
		return z.conflict(ModelChange, ours, theirs, nil)
	}
	var m model.Model
	for _, side := range []object.Ref{ours.From, ours.To, theirs.To} {
		var err error
		if m, err = z.reg.Resolve(side.Model, side.Root.Format); err != nil {
			return fmt.Errorf("merge %s: %w", ours.Path, err)
		}
	}
	r, err := m.Merge(z.ctx, ours.From.Root, ours.To.Root, theirs.To.Root, z.rw)
	if err != nil {
		return fmt.Errorf("merge %s: model %d: %w", ours.Path, m.ID(), err)
	}
	if len(r.Conflicts) > 0 {
		return z.conflict(BothChanged, ours, theirs, r.Conflicts)
	}
	if r.Root.Format == 0 || r.Root.Format > m.FormatVersion() {
		return fmt.Errorf("merge %s: model %d merged to format %d, which it does not write", ours.Path, m.ID(), r.Root.Format)
	}
	z.res.Stats.Modified++
	return z.ed.Put(ours.Path, object.Ref{Model: m.ID(), Root: r.Root})
}

// conflict records a path; ours stays in the merged namespace.
func (z *zipper) conflict(k Kind, ours, theirs object.Change, mc []model.Conflict) error {
	z.res.Conflicts = append(z.res.Conflicts, Conflict{Path: ours.Path, Kind: k, Base: ours.From, Ours: ours.To, Theirs: theirs.To, Model: mc})
	return nil
}
