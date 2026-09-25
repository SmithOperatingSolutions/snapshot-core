// Package tree is the directory-tree model (model id 2): a folder of files as
// one object, a prolly map from each file's path (the object path grammar) to
// its entry, the file's mode, modification time and content (a blob). It is
// merged per entry: one side's change is taken, a change both made is taken
// once, and a delete against an edit, an add against a different add, or two
// different edits of one entry are a conflict at that entry's path.
package tree

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/object"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
	"github.com/SmithOperatingSolutions/snapshot-core/core/stream"
	"github.com/SmithOperatingSolutions/snapshot-core/model/mapobject"
)

// ID is the tree model's id.
const ID model.ID = 2

// Format is the format version this package writes.
const Format = 1

// EntrySize is the length of an encoded entry.
const EntrySize = 55

// Entry is one file of a tree.
type Entry struct {
	Mode    uint32
	ModTime int64      // unix nanoseconds, UTC
	Content model.Root // the file's bytes, a blob
}

// Encode returns the 55-byte record: mode u32 · mtime i64 · size u64 ·
// depth u8 · format u16 · root [32], little-endian.
func (e Entry) Encode() []byte {
	b := make([]byte, 0, EntrySize)
	b = binary.LittleEndian.AppendUint32(b, e.Mode)
	b = binary.LittleEndian.AppendUint64(b, uint64(e.ModTime))
	b = binary.LittleEndian.AppendUint64(b, e.Content.Size)
	b = append(b, e.Content.Depth)
	b = binary.LittleEndian.AppendUint16(b, e.Content.Format)
	return append(b, e.Content.Hash[:]...)
}

// DecodeEntry parses a record (chunk.ErrCorrupt for any other length or a
// content format of 0).
func DecodeEntry(b []byte) (Entry, error) {
	if len(b) != EntrySize {
		return Entry{}, fmt.Errorf("%w: a tree entry of %d bytes", chunk.ErrCorrupt, len(b))
	}
	var e Entry
	e.Mode = binary.LittleEndian.Uint32(b[0:])
	e.ModTime = int64(binary.LittleEndian.Uint64(b[4:]))
	e.Content.Size = binary.LittleEndian.Uint64(b[12:])
	e.Content.Depth = b[20]
	e.Content.Format = binary.LittleEndian.Uint16(b[21:])
	copy(e.Content.Hash[:], b[23:])
	if e.Content.Format == 0 {
		return Entry{}, fmt.Errorf("%w: tree entry %x", chunk.ErrCorrupt, b)
	}
	return e, nil
}

// Model is the tree model; Config is how it writes the trees it merges.
type Model struct {
	Config prolly.Config
}

var (
	_ model.Model  = Model{}
	_ model.Walker = Model{}
)

// ID implements model.Model.
func (Model) ID() model.ID { return ID }

// FormatVersion implements model.Model.
func (Model) FormatVersion() uint16 { return Format }

// spec is the tree as a map-shaped model: a map from path to entry under a
// configuration, its records checked as entries under valid paths. No
// record is longer than an entry, so no longer value is read (#23).
func spec(c prolly.Config) mapobject.Spec {
	c.MaxValue = EntrySize
	return mapobject.Spec{Name: "tree", Format: Format, Config: c, Check: func(key, rec []byte) error {
		_, err := checked(key, rec)
		return err
	}}
}

// Write stores entries as a tree.
func Write(ctx context.Context, s chunk.ReadWriter, c prolly.Config, entries map[string]Entry) (model.Root, error) {
	m, err := prolly.Empty(ctx, s, c)
	if err != nil {
		return model.Root{}, err
	}
	e := m.Editor()
	for p, en := range entries {
		if err := object.ValidPath(p); err != nil {
			return model.Root{}, err
		}
		if err := e.Put([]byte(p), en.Encode()); err != nil {
			return model.Root{}, err
		}
	}
	if m, err = e.Flush(ctx); err != nil {
		return model.Root{}, err
	}
	return spec(c).Root(m), nil
}

// checked decodes a stored entry and its path.
func checked(key, rec []byte) (Entry, error) {
	if err := object.ValidPath(string(key)); err != nil {
		return Entry{}, fmt.Errorf("%w: a tree path: %w", chunk.ErrCorrupt, err)
	}
	return DecodeEntry(rec)
}

// Read returns a tree's entries.
func Read(ctx context.Context, r chunk.Reader, c prolly.Config, root model.Root) (map[string]Entry, error) {
	m, err := spec(c).Open(ctx, mapobject.ReadOnly(r), root)
	if err != nil {
		return nil, err
	}
	it, err := m.IterRange(ctx, nil, nil)
	if err != nil {
		return nil, err
	}
	out := map[string]Entry{} // not sized by m.Count(): the root's claim, checked only as iteration reaches each child (#24)
	for {
		k, v, ok, err := it.Next()
		if err != nil {
			return nil, err
		}
		if !ok {
			return out, nil
		}
		e, err := checked(k, v)
		if err != nil {
			return nil, err
		}
		out[string(k)] = e
	}
}

// Validate implements model.Model: every entry decodes under a valid path,
// and the root's size is the number of entries.
func (m Model) Validate(ctx context.Context, root model.Root, r chunk.Reader) error {
	_, err := Read(ctx, r, m.Config, root)
	return err
}

// Walk implements model.Walker: a tree's map, and each entry's content,
// which is a stream.
func (m Model) Walk(ctx context.Context, root model.Root, r chunk.Reader, visit func(h hash.Hash, leaf bool) (bool, error)) error {
	return spec(m.Config).Walk(ctx, root, r, visit, func(key, val []byte) error {
		e, err := checked(key, val)
		if err != nil {
			return err
		}
		return stream.Walk(ctx, r, stream.Ref{Root: e.Content.Hash, Size: e.Content.Size, Depth: e.Content.Depth}, visit)
	})
}

// Diff implements model.Model: a change per entry, located by its path.
func (m Model) Diff(ctx context.Context, from, to model.Root, r chunk.Reader) (model.DiffIter, error) {
	return spec(m.Config).Diff(ctx, from, to, r)
}

// Merge implements model.Model, per entry: one side's change is taken, a
// change both made is taken once, and a delete against an edit, an add
// against a different add, or two different edits of one entry are a
// conflict at that entry's path (mapobject.Disagreement).
func (m Model) Merge(ctx context.Context, base, ours, theirs model.Root, rw chunk.ReadWriter) (model.MergeResult, error) {
	return spec(m.Config).Merge(ctx, base, ours, theirs, rw, nil)
}
