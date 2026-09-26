// Package object is the namespace: a prolly map from path to a typed object,
// the tree every commit names (docs/DESIGN.md §8). A table, a folder of files
// and a JSON document are objects of different models in one namespace, so
// they branch, diff and merge together.
//
// A namespace resolves every object's model before handing it out: an object
// whose model the registry lacks is model.ErrUnknownModel, and nothing of it
// is read. Paths are checked against the grammar on every read and write.
package object

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
	"github.com/SmithOperatingSolutions/snapshot-core/core/wire"
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
func (r Ref) Encode() []byte {
	var w wire.Writer
	w.U16(uint16(r.Model))
	w.U16(r.Root.Format)
	w.U8(r.Flags)
	w.U8(r.Root.Depth)
	w.U64(r.Root.Size)
	w.Raw(r.Root.Hash[:])
	return w.Bytes()
}

// DecodeRef parses a record, refusing any other length, flags, model id 0 or
// format 0 (chunk.ErrCorrupt).
func DecodeRef(b []byte) (Ref, error) {
	if len(b) != RefSize {
		return Ref{}, fmt.Errorf("%w: an object reference of %d bytes", chunk.ErrCorrupt, len(b))
	}
	r := wire.NewReader(b)
	var ref Ref
	ref.Model, ref.Root.Format = model.ID(r.U16()), r.U16()
	ref.Flags, ref.Root.Depth = r.U8(), r.U8()
	ref.Root.Size = r.U64()
	copy(ref.Root.Hash[:], r.Fixed(hash.Size))
	if r.Done() != nil || ref.Flags != 0 || ref.Model == 0 || ref.Root.Format == 0 {
		return Ref{}, fmt.Errorf("%w: object reference %x", chunk.ErrCorrupt, b)
	}
	return ref, nil
}

// ValidPath reports whether p is in the path grammar: valid UTF-8,
// '/'-separated segments of 1 to 255 bytes, none "." or "..", no byte under
// 0x20 or 0x7f, at most 64 segments and 4,096 bytes.
func ValidPath(p string) error {
	if p == "" || len(p) > MaxPathLen || !utf8.ValidString(p) {
		return fmt.Errorf("%w: %q", ErrInvalidPath, p)
	}
	depth := 0
	for seg := range strings.SplitSeq(p, "/") {
		depth++
		if depth > MaxDepth || seg == "" || len(seg) > MaxSegmentLen || seg == "." || seg == ".." {
			return fmt.Errorf("%w: %q", ErrInvalidPath, p)
		}
		for i := 0; i < len(seg); i++ {
			if seg[i] < 0x20 || seg[i] == 0x7f {
				return fmt.Errorf("%w: %q holds a control character", ErrInvalidPath, p)
			}
		}
	}
	return nil
}

// namespace is c for a namespace's map, whose values are object references:
// no longer value is read (#23).
func namespace(c prolly.Config) prolly.Config {
	c.MaxValue = RefSize
	return c
}

// Namespace is an immutable map from path to object.
type Namespace struct {
	s   chunk.ReadWriter
	m   *prolly.Map
	reg *model.Registry
}

// New returns the empty namespace.
func New(ctx context.Context, s chunk.ReadWriter, c prolly.Config, reg *model.Registry) (*Namespace, error) {
	if reg == nil {
		return nil, errors.New("object: a namespace needs a model registry")
	}
	m, err := prolly.Empty(ctx, s, namespace(c))
	if err != nil {
		return nil, err
	}
	return &Namespace{s: s, m: m, reg: reg}, nil
}

// Open returns the namespace whose root node is root.
func Open(ctx context.Context, s chunk.ReadWriter, c prolly.Config, reg *model.Registry, root hash.Hash) (*Namespace, error) {
	if reg == nil {
		return nil, errors.New("object: a namespace needs a model registry")
	}
	m, err := prolly.Open(ctx, s, namespace(c), root)
	if err != nil {
		return nil, err
	}
	return &Namespace{s: s, m: m, reg: reg}, nil
}

// Root is the namespace's root hash.
func (n *Namespace) Root() hash.Hash { return n.m.Root() }

// Count is the number of objects.
func (n *Namespace) Count() uint64 { return n.m.Count() }

// Get returns the object at path and its model. An object whose model the
// registry lacks is model.ErrUnknownModel, and nothing of it is read.
func (n *Namespace) Get(ctx context.Context, path string) (Ref, model.Model, bool, error) {
	if err := ValidPath(path); err != nil {
		return Ref{}, nil, false, err
	}
	b, ok, err := n.m.Get(ctx, []byte(path))
	if err != nil || !ok {
		return Ref{}, nil, false, err
	}
	ref, err := DecodeRef(b)
	if err != nil {
		return Ref{}, nil, false, err
	}
	m, err := n.reg.Resolve(ref.Model, ref.Root.Format)
	if err != nil {
		return ref, nil, true, fmt.Errorf("object %s: %w", path, err)
	}
	return ref, m, true, nil
}

// Editor collects puts and deletes against a namespace.
type Editor struct {
	n *Namespace
	e *prolly.Editor
}

// Editor starts editing n.
func (n *Namespace) Editor() *Editor { return &Editor{n: n, e: n.m.Editor()} }

// Put sets path to ref; ref's model must be registered.
func (e *Editor) Put(path string, ref Ref) error {
	if err := ValidPath(path); err != nil {
		return err
	}
	if ref.Flags != 0 {
		return fmt.Errorf("object %s: flags %#x, which format v1 does not define", path, ref.Flags)
	}
	if _, err := e.n.reg.Resolve(ref.Model, ref.Root.Format); err != nil {
		return fmt.Errorf("object %s: %w", path, err)
	}
	return e.e.Put([]byte(path), ref.Encode())
}

// Delete removes path.
func (e *Editor) Delete(path string) error {
	if err := ValidPath(path); err != nil {
		return err
	}
	return e.e.Delete([]byte(path))
}

// Flush writes the edits and returns the new namespace; the editor then edits it.
func (e *Editor) Flush(ctx context.Context) (*Namespace, error) {
	m, err := e.e.Flush(ctx)
	if err != nil {
		return nil, err
	}
	e.n = &Namespace{s: e.n.s, m: m, reg: e.n.reg}
	return e.n, nil
}

// Change is one path that differs between two namespaces.
type Change struct {
	Kind     prolly.ChangeKind
	Path     string
	From, To Ref // zero where absent
}

// DiffIter walks the changes between two namespaces in path order.
type DiffIter struct {
	d  *prolly.DiffIter
	to *Namespace
}

// Diff compares from with to, skipping every subtree they share.
func Diff(ctx context.Context, from, to *Namespace) (*DiffIter, error) {
	d, err := prolly.Diff(ctx, from.m, to.m)
	if err != nil {
		return nil, err
	}
	return &DiffIter{d: d, to: to}, nil
}

// Next returns the next change; ok is false at the end.
func (d *DiffIter) Next() (Change, bool, error) {
	pc, ok, err := d.d.Next()
	if err != nil || !ok {
		return Change{}, false, err
	}
	c := Change{Kind: pc.Kind, Path: string(pc.Key)}
	if err := ValidPath(c.Path); err != nil {
		return Change{}, false, fmt.Errorf("%w: a stored path: %w", chunk.ErrCorrupt, err)
	}
	if pc.Kind != prolly.Added {
		if c.From, err = DecodeRef(pc.From); err != nil {
			return Change{}, false, err
		}
	}
	if pc.Kind != prolly.Removed {
		if c.To, err = DecodeRef(pc.To); err != nil {
			return Change{}, false, err
		}
	}
	return c, true, nil
}

// Detail asks the model that owns both sides of a modified object where it
// changed; anything else (an add, a delete, a change of model) has no detail.
func (d *DiffIter) Detail(ctx context.Context, c Change) (model.DiffIter, error) {
	if c.Kind != prolly.Modified || c.From.Model != c.To.Model {
		return nil, fmt.Errorf("object %s: no one model owns both sides of the change", c.Path)
	}
	for _, side := range []Ref{c.From, c.To} {
		if _, err := d.to.reg.Resolve(side.Model, side.Root.Format); err != nil {
			return nil, fmt.Errorf("object %s: %w", c.Path, err)
		}
	}
	m, _ := d.to.reg.Resolve(c.To.Model, c.To.Root.Format)
	return m.Diff(ctx, c.From.Root, c.To.Root, d.to.s)
}

// ErrNotWalkable is returned for an object whose model does not implement
// model.Walker: what it reaches is unknown (docs/DESIGN.md §9).
var ErrNotWalkable = errors.New("object: the object's model cannot walk")

// Walk calls visit for every chunk the namespace at root reaches: its nodes,
// root first, and each object it names, through the object's model. An
// object whose model the registry lacks is model.ErrUnknownModel, and one
// whose model cannot walk ErrNotWalkable: Walk refuses rather than name too
// little.
func Walk(ctx context.Context, rd chunk.Reader, c prolly.Config, reg *model.Registry, root hash.Hash, visit func(h hash.Hash, leaf bool) (bool, error)) error {
	if reg == nil {
		return errors.New("object: a namespace needs a model registry")
	}
	return prolly.Walk(ctx, rd, namespace(c), root, visit, func(key, val []byte) error {
		path := string(key)
		if err := ValidPath(path); err != nil {
			return fmt.Errorf("%w: a stored path: %w", chunk.ErrCorrupt, err)
		}
		ref, err := DecodeRef(val)
		if err != nil {
			return err
		}
		if err := WalkRef(ctx, rd, reg, ref, visit); err != nil {
			return fmt.Errorf("object %s: %w", path, err)
		}
		return nil
	})
}

// WalkRef calls visit for every chunk one object reaches, through its model.
func WalkRef(ctx context.Context, rd chunk.Reader, reg *model.Registry, ref Ref, visit func(h hash.Hash, leaf bool) (bool, error)) error {
	m, err := reg.Resolve(ref.Model, ref.Root.Format)
	if err != nil {
		return err
	}
	w, ok := m.(model.Walker)
	if !ok {
		return fmt.Errorf("%w: model %d", ErrNotWalkable, ref.Model)
	}
	return w.Walk(ctx, ref.Root, rd, visit)
}

// Changes reports what the flush that made n changed: the root of the
// namespace it edited and the paths that differ between the two, in path
// order. ok is false for a namespace no flush made.
func (n *Namespace) Changes() (base hash.Hash, paths []string, ok bool) {
	base, keys, ok := n.m.Changes()
	if !ok {
		return hash.Hash{}, nil, false
	}
	paths = make([]string, len(keys))
	for i, k := range keys {
		paths[i] = string(k)
	}
	return base, paths, true
}
