// Package tree is the directory-tree model (model id 2): a folder of files as
// one object, a prolly map from each file's path (the object path grammar) to
// its entry, the file's mode, modification time and content (a blob). It is
// merged per entry: one side's change is taken, a change both made is taken
// once, and a delete against an edit, an add against a different add, or two
// different edits of one entry are a conflict at that entry's path.
package tree

import (
	"context"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
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
func (e Entry) Encode() []byte { return nil }

// DecodeEntry parses a record (chunk.ErrCorrupt for any other length or a
// content format of 0).
func DecodeEntry(b []byte) (Entry, error) { return Entry{}, nil }

// Model is the tree model; Config is how it writes the trees it merges.
type Model struct {
	Config prolly.Config
}

var _ model.Model = Model{}

// ID implements model.Model.
func (Model) ID() model.ID { return 0 }

// FormatVersion implements model.Model.
func (Model) FormatVersion() uint16 { return 0 }

// Write stores entries as a tree.
func Write(ctx context.Context, s chunk.ReadWriter, c prolly.Config, entries map[string]Entry) (model.Root, error) {
	return model.Root{}, nil
}

// Read returns a tree's entries.
func Read(ctx context.Context, r chunk.Reader, c prolly.Config, root model.Root) (map[string]Entry, error) {
	return nil, nil
}

// Validate implements model.Model: every entry decodes under a valid path,
// and the root's size is the number of entries.
func (m Model) Validate(ctx context.Context, root model.Root, r chunk.Reader) error { return nil }

// Diff implements model.Model: a change per entry, located by its path.
func (m Model) Diff(ctx context.Context, from, to model.Root, r chunk.Reader) (model.DiffIter, error) {
	return nil, nil
}

// Merge implements model.Model, per entry.
func (m Model) Merge(ctx context.Context, base, ours, theirs model.Root, rw chunk.ReadWriter) (model.MergeResult, error) {
	return model.MergeResult{}, nil
}
