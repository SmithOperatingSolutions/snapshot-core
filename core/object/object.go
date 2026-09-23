// Package object is the namespace: a prolly map from path to a typed object,
// the tree every commit names (docs/DESIGN.md §8). A table, a folder of files
// and a JSON document are objects of different models in one namespace, so
// they branch, diff and merge together.
package object

import (
	"context"
	"errors"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
)

// RefSize is the length of an encoded object reference.
const RefSize = 46

// Path limits.
const (
	MaxPathLen    = 4096 // the prolly key limit
	MaxSegmentLen = 255
	MaxDepth      = 64
)

// ErrInvalidPath is returned for a path outside the grammar.
var ErrInvalidPath = errors.New("object: invalid path")

// Ref is the namespace's value: which model owns an object, and its root.
type Ref struct {
	Model model.ID
	Flags uint8 // zero in format v1
	Root  model.Root
}

// Encode returns the 46-byte record: model u16 · format u16 · flags u8 ·
// depth u8 · size u64 · root [32], little-endian.
func (r Ref) Encode() []byte { return nil }

// DecodeRef parses a record, refusing any other length, flags, model id 0 or
// format 0 (chunk.ErrCorrupt).
func DecodeRef(b []byte) (Ref, error) { return Ref{}, nil }

// ValidPath reports whether p is in the path grammar: valid UTF-8,
// '/'-separated segments of 1 to 255 bytes, none "." or "..", no byte under
// 0x20 or 0x7f, at most 64 segments and 4,096 bytes.
func ValidPath(p string) error { return nil }

// Namespace is an immutable map from path to object.
type Namespace struct{}

// New returns the empty namespace.
func New(ctx context.Context, s chunk.ReadWriter, c prolly.Config, reg *model.Registry) (*Namespace, error) {
	return &Namespace{}, nil
}

// Open returns the namespace whose root node is root.
func Open(ctx context.Context, s chunk.ReadWriter, c prolly.Config, reg *model.Registry, root hash.Hash) (*Namespace, error) {
	return &Namespace{}, nil
}

// Root is the namespace's root hash.
func (n *Namespace) Root() hash.Hash { return hash.Hash{} }

// Count is the number of objects.
func (n *Namespace) Count() uint64 { return 0 }

// Get returns the object at path and its model. An object whose model the
// registry lacks is model.ErrUnknownModel, and nothing of it is read.
func (n *Namespace) Get(ctx context.Context, path string) (Ref, model.Model, bool, error) {
	return Ref{}, nil, false, nil
}

// Editor collects puts and deletes against a namespace.
type Editor struct{}

// Editor starts editing n.
func (n *Namespace) Editor() *Editor { return &Editor{} }

// Put sets path to ref; ref's model must be registered.
func (e *Editor) Put(path string, ref Ref) error { return nil }

// Delete removes path.
func (e *Editor) Delete(path string) error { return nil }

// Flush writes the edits and returns the new namespace.
func (e *Editor) Flush(ctx context.Context) (*Namespace, error) { return &Namespace{}, nil }

// Change is one path that differs between two namespaces.
type Change struct {
	Kind     prolly.ChangeKind
	Path     string
	From, To Ref // zero where absent
}

// DiffIter walks the changes between two namespaces in path order.
type DiffIter struct{}

// Diff compares from with to, skipping every subtree they share.
func Diff(ctx context.Context, from, to *Namespace) (*DiffIter, error) { return &DiffIter{}, nil }

// Next returns the next change; ok is false at the end.
func (d *DiffIter) Next() (Change, bool, error) { return Change{}, false, nil }

// Detail asks the model that owns both sides of a modified object where it
// changed; anything else (an add, a delete, a change of model) has no detail.
func (d *DiffIter) Detail(ctx context.Context, c Change) (model.DiffIter, error) { return nil, nil }
