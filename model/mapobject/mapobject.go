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
	"bytes"
	"context"
	"errors"
	"fmt"

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

// Decision is a Decider's answer for one key both sides changed.
type Decision struct {
	Value     []byte           // stored when Put, through the Spec's Check
	Put       bool             // false keeps ours as it is
	Conflicts []model.Conflict // where the sides could not combine; one with no Location is at the key
}

// Decider decides one key both sides changed, as a Resolver does, and can
// say more: conflicts located below the key (a field, a member, a cell), in
// the model's own location encoding, and an error, which aborts the merge
// (a stored record that does not decode, a store failure). With any
// conflict anywhere the merge's result is ours, so nothing is stored.
type Decider func(key []byte, ours, theirs prolly.Change) (Decision, error)

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

// claims checks what a root says before the map is opened.
func (s Spec) claims(root model.Root) error {
	if root.Format != s.Format {
		return fmt.Errorf("%w: %s format %d", model.ErrUnknownModel, s.Name, root.Format)
	}
	if root.Depth != 0 {
		return fmt.Errorf("%w: a %s root claims stream depth %d", chunk.ErrCorrupt, s.Name, root.Depth)
	}
	return nil
}

// Open opens the map under root and checks the root's claims against it:
// the spec's format, depth 0, and a count equal to the root's size.
func (s Spec) Open(ctx context.Context, st chunk.ReadWriter, root model.Root) (*prolly.Map, error) {
	if err := s.claims(root); err != nil {
		return nil, err
	}
	m, err := prolly.Open(ctx, st, s.Config, root.Hash)
	if err != nil {
		return nil, err
	}
	if m.Count() != root.Size {
		return nil, fmt.Errorf("%w: a %s of %d entries whose root says %d", chunk.ErrCorrupt, s.Name, m.Count(), root.Size)
	}
	return m, nil
}

// Validate opens the map and runs Check over every record.
func (s Spec) Validate(ctx context.Context, root model.Root, r chunk.Reader) error {
	m, err := s.Open(ctx, ReadOnly(r), root)
	if err != nil {
		return err
	}
	it, err := m.IterRange(ctx, nil, nil)
	if err != nil {
		return err
	}
	for {
		k, v, ok, err := it.Next()
		if err != nil || !ok {
			return err
		}
		if err := s.Check(k, v); err != nil {
			return err
		}
	}
}

// Walk names every chunk of the map to visit, root first, and calls each
// for every record, so a model can walk what its records reach.
func (s Spec) Walk(ctx context.Context, root model.Root, r chunk.Reader, visit func(h hash.Hash, leaf bool) (bool, error), each func(key, value []byte) error) error {
	if err := s.claims(root); err != nil {
		return err
	}
	if each == nil {
		each = func([]byte, []byte) error { return nil }
	}
	return prolly.Walk(ctx, r, s.Config, root.Hash, visit, each)
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

// Diff is one change per key, located by the key.
func (s Spec) Diff(ctx context.Context, from, to model.Root, r chunk.Reader) (model.DiffIter, error) {
	fm, err := s.Open(ctx, ReadOnly(r), from)
	if err != nil {
		return nil, err
	}
	tm, err := s.Open(ctx, ReadOnly(r), to)
	if err != nil {
		return nil, err
	}
	d, err := prolly.Diff(ctx, fm, tm)
	if err != nil {
		return nil, err
	}
	return diffIter{d}, nil
}

// Merge merges per key: what only ours changed stays, what only theirs
// changed is applied to ours, and a key both changed goes to resolve
// (Disagreement when nil). With any conflict the result is ours and the
// conflicts; otherwise the merged map's root.
func (s Spec) Merge(ctx context.Context, base, ours, theirs model.Root, rw chunk.ReadWriter, resolve Resolver) (model.MergeResult, error) {
	if resolve == nil {
		resolve = Disagreement
	}
	return s.MergeWith(ctx, base, ours, theirs, rw, func(key []byte, o, th prolly.Change) (Decision, error) {
		value, put, reason := resolve(key, o, th)
		if reason != "" {
			return Decision{Conflicts: []model.Conflict{{Reason: reason}}}, nil
		}
		return Decision{Value: value, Put: put}, nil
	})
}

// MergeWith is Merge with a Decider: a key both sides changed goes to
// decide, whose conflicts are kept where it located them (at the key when
// it located none) and whose error aborts the merge.
func (s Spec) MergeWith(ctx context.Context, base, ours, theirs model.Root, rw chunk.ReadWriter, decide Decider) (model.MergeResult, error) {
	var maps [3]*prolly.Map
	for i, r := range []model.Root{base, ours, theirs} {
		var err error
		if maps[i], err = s.Open(ctx, rw, r); err != nil {
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
			if err = s.apply(ed, ct); err == nil {
				ct, okT, err = dTheirs.Next()
			}
		default: // both changed it
			var d Decision
			if d, err = decide(co.Key, co, ct); err != nil {
				break
			}
			for _, c := range d.Conflicts {
				if c.Location == nil {
					c.Location = bytes.Clone(co.Key)
				}
				conflicts = append(conflicts, c)
			}
			if d.Put { // with any conflict the merge is ours and this edit is discarded
				err = s.put(ed, co.Key, d.Value)
			}
			if err == nil {
				if co, okO, err = dOurs.Next(); err == nil {
					ct, okT, err = dTheirs.Next()
				}
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
	return model.MergeResult{Root: s.Root(merged)}, nil
}

// apply makes theirs' change to a key in the merged map.
func (s Spec) apply(ed *prolly.Editor, c prolly.Change) error {
	if c.Kind == prolly.Removed {
		return ed.Delete(c.Key)
	}
	return s.put(ed, c.Key, c.To)
}

// put stores a record the model has checked.
func (s Spec) put(ed *prolly.Editor, key, value []byte) error {
	if err := s.Check(key, value); err != nil {
		return err
	}
	return ed.Put(key, value)
}

// Disagreement is the resolver every map-shaped model starts from: both
// deleted is clean; deleted on one side and changed on the other is a
// conflict; the same value on both sides is clean; different values are a
// conflict.
func Disagreement(key []byte, ours, theirs prolly.Change) (value []byte, put bool, reason string) {
	switch {
	case ours.Kind == prolly.Removed && theirs.Kind == prolly.Removed:
		return nil, false, ""
	case ours.Kind == prolly.Removed || theirs.Kind == prolly.Removed:
		return nil, false, "deleted on one side and changed on the other"
	case bytes.Equal(ours.To, theirs.To):
		return nil, false, ""
	case ours.Kind == prolly.Added:
		return nil, false, "added differently on both sides"
	}
	return nil, false, "changed differently on both sides"
}
