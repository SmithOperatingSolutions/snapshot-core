// Package mapobject is what a data model whose object is one prolly map
// needs beside the frozen plugin port: opening the map under the root's
// claims, validating every record, walking the map for GC, a diff per key,
// and a three-way merge per key that zips both sides' diffs from base and
// asks a Resolver only where both sides changed one key. The core's tree
// model and every map-shaped model outside the core (snapshot-engine's kv,
// table and document) build on it instead of copying it.
//
// A model supplies a Spec: its format, its map configuration and a Check
// that decodes one record and refuses what is not one of its own.
package mapobject

import (
	"context"
	"errors"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
)

// Spec is one map-shaped model's shape.
type Spec struct {
	Name   string        // for messages: "tree", "kv"
	Format uint16        // the model's format version
	Config prolly.Config // the map's configuration
	// Check decodes one record and refuses what is not one of the model's:
	// Validate runs it on every record, Merge on every record it writes.
	Check func(key, value []byte) error
}

// Resolver decides one key both sides changed from base. It returns the
// value to store and put true; put false with reason "" keeps ours as it
// is (the sides agree); a reason names a conflict for the person resolving
// it, and the key stays as ours.
type Resolver func(key []byte, ours, theirs prolly.Change) (value []byte, put bool, reason string)

var errNotImplemented = errors.New("mapobject: not implemented")

// ReadOnly wraps a reader as a store that refuses every Put, for opening a
// map to read.
func ReadOnly(r chunk.Reader) chunk.ReadWriter { return readOnly{r} }

type readOnly struct{ chunk.Reader }

func (readOnly) Put(context.Context, []byte) (hash.Hash, error) {
	return hash.Hash{}, errors.New("mapobject: this store is read-only")
}

// Root is the model root of a map: its top chunk, its count as the size,
// depth 0 and the spec's format.
func (s Spec) Root(m *prolly.Map) model.Root {
	return model.Root{Hash: m.Root(), Size: m.Count(), Format: s.Format}
}

// Open opens the map under root and checks the root's claims against it:
// the spec's format, depth 0, and a count equal to the root's size.
func (s Spec) Open(ctx context.Context, st chunk.ReadWriter, root model.Root) (*prolly.Map, error) {
	return nil, errNotImplemented
}

// Validate opens the map and runs Check over every record.
func (s Spec) Validate(ctx context.Context, root model.Root, r chunk.Reader) error {
	return errNotImplemented
}

// Walk names every chunk of the map to visit, root first, and calls each
// for every record, so a model can walk what its records reach.
func (s Spec) Walk(ctx context.Context, root model.Root, r chunk.Reader, visit func(h hash.Hash, leaf bool) (bool, error), each func(key, value []byte) error) error {
	return errNotImplemented
}

// Diff is one change per key, located by the key.
func (s Spec) Diff(ctx context.Context, from, to model.Root, r chunk.Reader) (model.DiffIter, error) {
	return nil, errNotImplemented
}

// Merge merges per key: what only ours changed stays, what only theirs
// changed is applied to ours, and a key both changed goes to resolve
// (Disagreement when nil). With any conflict the result is ours and the
// conflicts; otherwise the merged map's root.
func (s Spec) Merge(ctx context.Context, base, ours, theirs model.Root, rw chunk.ReadWriter, resolve Resolver) (model.MergeResult, error) {
	return model.MergeResult{}, errNotImplemented
}

// Disagreement is the resolver every map-shaped model starts from: both
// deleted is clean; deleted on one side and changed on the other is a
// conflict; the same value on both sides is clean; different values are a
// conflict.
func Disagreement(key []byte, ours, theirs prolly.Change) (value []byte, put bool, reason string) {
	return nil, false, "mapobject: not implemented"
}
