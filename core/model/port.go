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

// ErrUnknownModel is returned for an object whose model the registry lacks,
// or whose format is newer than its model knows. Nothing is decoded.
var ErrUnknownModel = errors.New("model: unknown model or format")

// Registry is the set of models a repository is opened with. It cannot
// change once built.
type Registry struct{}

// NewRegistry builds a registry. Two models with one id, a nil model or
// model id 0 is an error, and no registry.
func NewRegistry(models ...Model) (*Registry, error) { return &Registry{}, nil }

// Resolve returns the model for an object of model id written in format.
func (r *Registry) Resolve(id ID, format uint16) (Model, error) { return nil, nil }

// IDs lists the registered ids in order.
func (r *Registry) IDs() []ID { return nil }
