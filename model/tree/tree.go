// Package tree is the directory-tree model (model id 2): a folder of files as
// one object, a prolly map from each file's path (the object path grammar) to
// its entry, the file's mode, modification time and content (a blob). It is
// merged per entry: one side's change is taken, a change both made is taken
// once, and a delete against an edit, an add against a different add, or two
// different edits of one entry are a conflict at that entry's path.
package tree

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/object"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
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

var _ model.Model = Model{}

// ID implements model.Model.
func (Model) ID() model.ID { return ID }

// FormatVersion implements model.Model.
func (Model) FormatVersion() uint16 { return Format }

// readOnly opens a map over a Reader: reading one never writes.
type readOnly struct{ chunk.Reader }

func (readOnly) Put(context.Context, []byte) (hash.Hash, error) {
	return hash.Hash{}, errors.New("tree: this store is read-only")
}

func rootOf(m *prolly.Map) model.Root {
	return model.Root{Hash: m.Root(), Size: m.Count(), Format: Format}
}

// open opens a tree's map and checks the root's claims against it.
func open(ctx context.Context, s chunk.ReadWriter, c prolly.Config, root model.Root) (*prolly.Map, error) {
	if root.Format != Format {
		return nil, fmt.Errorf("%w: tree format %d", model.ErrUnknownModel, root.Format)
	}
	if root.Depth != 0 {
		return nil, fmt.Errorf("%w: a tree root claims stream depth %d", chunk.ErrCorrupt, root.Depth)
	}
	m, err := prolly.Open(ctx, s, c, root.Hash)
	if err != nil {
		return nil, err
	}
	if m.Count() != root.Size {
		return nil, fmt.Errorf("%w: a tree of %d entries whose root says %d", chunk.ErrCorrupt, m.Count(), root.Size)
	}
	return m, nil
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
	return rootOf(m), nil
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
	m, err := open(ctx, readOnly{r}, c, root)
	if err != nil {
		return nil, err
	}
	it, err := m.IterRange(ctx, nil, nil)
	if err != nil {
		return nil, err
	}
	out := make(map[string]Entry, m.Count())
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

var kinds = map[prolly.ChangeKind]model.ChangeKind{prolly.Added: model.Added, prolly.Removed: model.Removed, prolly.Modified: model.Modified}

type diffIter struct{ d *prolly.DiffIter }

func (d diffIter) Next(context.Context) (model.Change, bool, error) {
	c, ok, err := d.d.Next()
	if err != nil || !ok {
		return model.Change{}, false, err
	}
	return model.Change{Kind: kinds[c.Kind], Location: c.Key}, true, nil
}

// Diff implements model.Model: a change per entry, located by its path.
func (m Model) Diff(ctx context.Context, from, to model.Root, r chunk.Reader) (model.DiffIter, error) {
	fm, err := open(ctx, readOnly{r}, m.Config, from)
	if err != nil {
		return nil, err
	}
	tm, err := open(ctx, readOnly{r}, m.Config, to)
	if err != nil {
		return nil, err
	}
	d, err := prolly.Diff(ctx, fm, tm)
	if err != nil {
		return nil, err
	}
	return diffIter{d}, nil
}

// Merge implements model.Model, per entry: it zips the two sides' diffs
// from base and applies what only theirs changed to ours.
func (m Model) Merge(ctx context.Context, base, ours, theirs model.Root, rw chunk.ReadWriter) (model.MergeResult, error) {
	var maps [3]*prolly.Map
	for i, r := range []model.Root{base, ours, theirs} {
		var err error
		if maps[i], err = open(ctx, rw, m.Config, r); err != nil {
			return model.MergeResult{}, err
		}
	}
	dOurs, err := prolly.Diff(ctx, maps[0], maps[1])
	if err != nil {
		return model.MergeResult{}, err
	}
	dTheirs, err := prolly.Diff(ctx, maps[0], maps[2])
	if err != nil {
		return model.MergeResult{}, err
	}
	ed := maps[1].Editor()
	var conflicts []model.Conflict
	co, okO, err := dOurs.Next()
	if err != nil {
		return model.MergeResult{}, err
	}
	ct, okT, err := dTheirs.Next()
	if err != nil {
		return model.MergeResult{}, err
	}
	for (okO || okT) && err == nil {
		switch {
		case !okT || (okO && bytes.Compare(co.Key, ct.Key) < 0): // only ours changed it
			co, okO, err = dOurs.Next()
		case !okO || bytes.Compare(ct.Key, co.Key) < 0: // only theirs changed it
			if err = apply(ed, ct); err == nil {
				ct, okT, err = dTheirs.Next()
			}
		default: // both changed it
			if reason := disagreement(co, ct); reason != "" {
				conflicts = append(conflicts, model.Conflict{Location: bytes.Clone(co.Key), Reason: reason})
			}
			if co, okO, err = dOurs.Next(); err == nil {
				ct, okT, err = dTheirs.Next()
			}
		}
	}
	if err != nil {
		return model.MergeResult{}, err
	}
	if len(conflicts) > 0 {
		return model.MergeResult{Root: ours, Conflicts: conflicts}, nil
	}
	merged, err := ed.Flush(ctx)
	if err != nil {
		return model.MergeResult{}, err
	}
	return model.MergeResult{Root: rootOf(merged)}, nil
}

// apply makes theirs' change to an entry in the merged tree.
func apply(ed *prolly.Editor, c prolly.Change) error {
	if c.Kind == prolly.Removed {
		return ed.Delete(c.Key)
	}
	if _, err := checked(c.Key, c.To); err != nil {
		return err
	}
	return ed.Put(c.Key, c.To)
}

// disagreement is why both sides' changes to one entry conflict, or "" if
// they left it the same.
func disagreement(ours, theirs prolly.Change) string {
	switch {
	case ours.Kind == prolly.Removed && theirs.Kind == prolly.Removed:
		return ""
	case ours.Kind == prolly.Removed || theirs.Kind == prolly.Removed:
		return "deleted on one side and changed on the other"
	case bytes.Equal(ours.To, theirs.To):
		return ""
	case ours.Kind == prolly.Added:
		return "added differently on both sides"
	}
	return "changed differently on both sides"
}
