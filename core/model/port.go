// Package model is the data-model plugin port (Storage Core Spec,
// "Data-model plugins"). A model gives an object's bytes meaning: how to
// validate, diff and merge them. The core finds what changed; the model
// decides whether two changes can combine. Registries are built explicitly,
// with no globals and no init() side effects.
//
// Port version 1, frozen at C3.
package model

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
)

// ID names a model. It is stable forever; 0 is never a model.
type ID uint16

// Root is an object as its model sees it: its top chunk, logical size, the
// depth of the stream it is rooted in (0 for anything else), and the format
// version it was written in, so a model can read its older formats.
type Root struct {
	Hash   hash.Hash
	Size   uint64
	Depth  uint8
	Format uint16
}

// ChangeKind says how part of an object changed.
type ChangeKind uint8

// Change kinds.
const (
	Added ChangeKind = iota + 1
	Removed
	Modified
)

// Change is one part of an object that differs; Location is the model's own
// address for it (an entry's path, a byte range, a row key).
type Change struct {
	Kind     ChangeKind
	Location []byte
}

// DiffIter walks a model's changes; ok is false at the end.
type DiffIter interface {
	Next(ctx context.Context) (c Change, ok bool, err error)
}

// Conflict is one place two sides' changes could not be combined.
type Conflict struct {
	Location []byte // the model's own address for it
	Reason   string // for the person resolving it
}

// MergeResult is a merged object, valid only when there are no conflicts.
type MergeResult struct {
	Root      Root
	Conflicts []Conflict
}

// Model is one data model.
type Model interface {
	ID() ID
	FormatVersion() uint16 // bumped on any encoding change
	Validate(ctx context.Context, root Root, r chunk.Reader) error
	Diff(ctx context.Context, from, to Root, r chunk.Reader) (DiffIter, error)
	Merge(ctx context.Context, base, ours, theirs Root, rw chunk.ReadWriter) (MergeResult, error)
}

// Walker is a model whose objects GC can collect (docs/DESIGN.md §9). It is
// beside the frozen Model interface, not in it; GC refuses to collect a
// repository holding an object whose model does not implement it, and
// model/contract requires it.
type Walker interface {
	// Walk calls visit for every chunk the object reaches, root first; visit
	// says whether to go on into what that chunk reaches (no, for one already
	// marked, so history shared between commits is walked once).
	Walk(ctx context.Context, root Root, r chunk.Reader, visit func(hash.Hash) (bool, error)) error
}

// ErrUnknownModel is returned for an object whose model the registry lacks,
// or whose format is newer than its model knows. Nothing is decoded.
var ErrUnknownModel = errors.New("model: unknown model or format")

// Registry is the set of models a repository is opened with. It cannot
// change once built.
type Registry struct {
	models map[ID]Model
	ids    []ID
}

// NewRegistry builds a registry. Two models with one id, a nil model or
// model id 0 is an error, and no registry.
func NewRegistry(models ...Model) (*Registry, error) {
	r := &Registry{models: make(map[ID]Model, len(models))}
	for i, m := range models {
		if m == nil {
			return nil, fmt.Errorf("model: model %d of %d is nil", i+1, len(models))
		}
		id := m.ID()
		if id == 0 {
			return nil, errors.New("model: model id 0 is reserved")
		}
		if _, dup := r.models[id]; dup {
			return nil, fmt.Errorf("model: two models share id %d", id)
		}
		r.models[id] = m
		r.ids = append(r.ids, id)
	}
	sort.Slice(r.ids, func(i, j int) bool { return r.ids[i] < r.ids[j] })
	return r, nil
}

// Resolve returns the model for an object of model id written in format.
func (r *Registry) Resolve(id ID, format uint16) (Model, error) {
	m, ok := r.models[id]
	switch {
	case !ok:
		return nil, fmt.Errorf("%w: model %d is not registered", ErrUnknownModel, id)
	case format == 0 || format > m.FormatVersion():
		return nil, fmt.Errorf("%w: model %d writes format %d, the object is format %d", ErrUnknownModel, id, m.FormatVersion(), format)
	}
	return m, nil
}

// IDs lists the registered ids in order.
func (r *Registry) IDs() []ID { return append([]ID(nil), r.ids...) }
