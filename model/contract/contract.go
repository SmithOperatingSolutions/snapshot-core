// Package contract is the suite every data model must pass, unchanged
// (Storage Core Spec, "Data-model plugins": round-trip, determinism,
// diff-matches-edits, merge(b, o, o) == o; each model also fuzzes its own
// decoders). A model must also walk (model.Walker), so GC can collect a
// repository holding its objects. A model that accumulates
// (model.Accumulator) may merge the same change on both sides to more than
// that change: its merge(b, o, o) must be clean and valid, not o.
package contract

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/memstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
)

// Subject is one model under test and how to make its objects.
type Subject struct {
	Model model.Model
	Store chunk.ReadWriter
	// Generate returns valid content for the model, a pure function of seed.
	Generate func(seed uint64) []byte
	// Mutate returns an edit of content, a pure function of seed, that differs from it.
	Mutate func(content []byte, seed uint64) []byte
	// Collide returns two edits of content, pure functions of seed, that the
	// model cannot combine: the same place changed two ways. Merging them
	// over content must report at least one conflict, each with a reason.
	Collide func(content []byte, seed uint64) (ours, theirs []byte)
	// Write stores content as an object; Read returns an object's content.
	Write func(t *testing.T, content []byte) model.Root
	Read  func(t *testing.T, root model.Root) []byte
}

// Factory returns a fresh subject over an empty store.
type Factory func(t *testing.T) Subject

var ctx = context.Background()

// Run runs the contract.
func Run(t *testing.T, newSubject Factory) {
	t.Run("Identity", func(t *testing.T) { identity(t, newSubject(t)) })
	t.Run("RoundTrip", func(t *testing.T) { roundTrip(t, newSubject(t)) })
	t.Run("Deterministic", func(t *testing.T) { deterministic(t, newSubject, newSubject(t)) })
	t.Run("DiffMatchesEdits", func(t *testing.T) { diffMatchesEdits(t, newSubject(t)) })
	t.Run("MergeIdentities", func(t *testing.T) { mergeIdentities(t, newSubject(t)) })
	t.Run("CollidingEditsConflict", func(t *testing.T) { collidingEditsConflict(t, newSubject(t)) })
	t.Run("ValidateRefusesGarbage", func(t *testing.T) { validateRefusesGarbage(t, newSubject(t)) })
	t.Run("WalkHoldsTheObject", func(t *testing.T) { walkHoldsTheObject(t, newSubject(t)) })
}

func identity(t *testing.T, s Subject) {
	if s.Model.ID() == 0 || s.Model.FormatVersion() == 0 {
		t.Fatalf("model id %d, format %d: neither may be 0", s.Model.ID(), s.Model.FormatVersion())
	}
	if r := s.Write(t, s.Generate(1)); r.Format != s.Model.FormatVersion() {
		t.Fatalf("an object written now has format %d, the model writes %d", r.Format, s.Model.FormatVersion())
	}
}

func roundTrip(t *testing.T, s Subject) {
	for seed := uint64(0); seed < 8; seed++ {
		c := s.Generate(seed)
		r := s.Write(t, c)
		if err := s.Model.Validate(ctx, r, s.Store); err != nil {
			t.Fatalf("seed %d: a freshly written object does not validate: %v", seed, err)
		}
		if got := s.Read(t, r); !bytes.Equal(got, c) {
			t.Fatalf("seed %d: an object read back as %d bytes differing from the %d written", seed, len(got), len(c))
		}
	}
}

func deterministic(t *testing.T, newSubject Factory, s Subject) {
	c := s.Generate(7)
	a, b := s.Write(t, c), s.Write(t, bytes.Clone(c))
	if d := s.Write(t, s.Mutate(c, 1)); d == a {
		t.Fatalf("fixture: different content made the same root %+v", a)
	}
	other := newSubject(t)
	if o := other.Write(t, c); a != b || a != o {
		t.Fatalf("the same content made roots %+v, %+v and (another store) %+v", a, b, o)
	}
}

func changes(t *testing.T, s Subject, from, to model.Root) []model.Change {
	t.Helper()
	d, err := s.Model.Diff(ctx, from, to, s.Store)
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if d == nil {
		t.Fatal("Diff returned neither an iterator nor an error")
	}
	var out []model.Change
	for {
		c, ok, err := d.Next(ctx)
		if err != nil {
			t.Fatalf("Diff: %v", err)
		}
		if !ok {
			return out
		}
		out = append(out, c)
	}
}

func diffMatchesEdits(t *testing.T, s Subject) {
	for seed := uint64(0); seed < 4; seed++ {
		c := s.Generate(seed)
		edited := s.Mutate(c, seed)
		if bytes.Equal(edited, c) {
			t.Fatalf("fixture: Mutate(seed %d) changed nothing", seed)
		}
		a, b := s.Write(t, c), s.Write(t, edited)
		if got := changes(t, s, a, a); len(got) != 0 {
			t.Fatalf("seed %d: an object diffed with itself has %d changes", seed, len(got))
		}
		if got := changes(t, s, a, b); len(got) == 0 {
			t.Fatalf("seed %d: an edited object diffs as unchanged", seed)
		}
		if got := changes(t, s, b, a); len(got) == 0 {
			t.Fatalf("seed %d: the reverse diff is empty", seed)
		}
	}
}

func merge(t *testing.T, s Subject, base, ours, theirs model.Root) model.MergeResult {
	t.Helper()
	r, err := s.Model.Merge(ctx, base, ours, theirs, s.Store)
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	return r
}

func mergeIdentities(t *testing.T, s Subject) {
	c := s.Generate(3)
	base := s.Write(t, c)
	o := s.Write(t, s.Mutate(c, 1))
	th := s.Write(t, s.Mutate(c, 2))
	if base == o || base == th || o == th {
		t.Fatalf("fixture: base, ours and theirs share a root (%+v, %+v, %+v)", base, o, th)
	}
	for name, tc := range map[string]struct{ b, o, t, want model.Root }{
		"merge(b, o, o) == o": {base, o, o, o},
		"merge(b, b, t) == t": {base, base, th, th},
		"merge(b, o, b) == o": {base, o, base, o},
		"merge(b, b, b) == b": {base, base, base, base},
	} {
		r := merge(t, s, tc.b, tc.o, tc.t)
		valid := func(root model.Root) error { return s.Model.Validate(ctx, root, s.Store) }
		if err := identityVerdict(s.Model, tc.b, tc.o, tc.t, tc.want, r, valid); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// identityVerdict judges a merge identity: the merge is clean and is want.
// A model that accumulates (model.Accumulator) merges the same change on
// both sides as two changes, so its merge(b, o, o) need not be o; it must
// still be clean and an object the model validates.
func identityVerdict(m model.Model, b, o, th, want model.Root, r model.MergeResult, valid func(model.Root) error) error {
	switch {
	case len(r.Conflicts) != 0:
		return fmt.Errorf("root %+v with %d conflicts, want none", r.Root, len(r.Conflicts))
	case model.Accumulates(m) && o == th && o != b:
		if err := valid(r.Root); err != nil {
			return fmt.Errorf("the model accumulated the same change on both sides to %+v, which it does not validate: %w", r.Root, err)
		}
	case r.Root != want:
		return fmt.Errorf("root %+v, want %+v", r.Root, want)
	}
	return nil
}

// collidingEditsConflict: two edits the model says cannot combine merge to
// conflicts, not to a silent choice of one side. The merge identities alone
// never exercise a conflict, so a model that merged everything by taking
// ours would pass them; this is where a model's conflict semantics are held.
func collidingEditsConflict(t *testing.T, s Subject) {
	if s.Collide == nil {
		t.Fatal("the subject has no Collide: a model must say how two edits collide, or its conflicts are never proved")
	}
	for seed := uint64(1); seed <= 3; seed++ {
		c := s.Generate(seed)
		ours, theirs := s.Collide(c, seed)
		base, o, th := s.Write(t, c), s.Write(t, ours), s.Write(t, theirs)
		if base == o || base == th || o == th {
			t.Fatalf("seed %d: Collide gave edits that do not both differ from the content and from each other (%+v, %+v, %+v)", seed, base, o, th)
		}
		if err := collisionVerdict(merge(t, s, base, o, th)); err != nil {
			t.Errorf("seed %d: %v", seed, err)
		}
	}
}

// collisionVerdict judges the merge of two colliding edits: at least one
// conflict, each with a reason.
func collisionVerdict(r model.MergeResult) error {
	if len(r.Conflicts) == 0 {
		return errors.New("two edits the model says cannot combine merged with no conflict: the merge took a side in silence")
	}
	for _, c := range r.Conflicts {
		if c.Reason == "" {
			return fmt.Errorf("a conflict at %q carries no reason for the person resolving it", c.Location)
		}
	}
	return nil
}

func validateRefusesGarbage(t *testing.T, s Subject) {
	format := s.Model.FormatVersion()
	missing := hash.Sum([]byte("a chunk no one stored"))
	if err := s.Model.Validate(ctx, model.Root{Hash: missing, Size: 10, Format: format}, s.Store); err == nil {
		t.Error("an object whose root chunk is missing validates")
	}
	for i := 0; i < 32; i++ {
		b := make([]byte, 0, 64*i)
		for j := 0; len(b) < 7*i+1; j++ {
			h := sha256.Sum256([]byte(fmt.Sprintf("garbage/%d/%d", i, j)))
			b = append(b, h[:]...)
		}
		b = b[:7*i+1]
		h, err := s.Store.Put(ctx, b)
		if err != nil {
			t.Fatal(err)
		}
		root := model.Root{Hash: h, Size: uint64(len(b)) + 1, Depth: uint8(i % 3), Format: format}
		if err := s.Model.Validate(ctx, root, s.Store); err == nil {
			t.Errorf("random bytes (%d) under a root whose size disagrees validate as an object", len(b))
		}
	}
}

// walkHoldsTheObject: GC keeps the chunks Walk names and deletes the rest
// (docs/DESIGN.md §9), so what Walk names must hold the object: copied alone
// into an empty store, it validates there. The root comes first, and a model
// that cannot walk is one GC cannot collect.
func walkHoldsTheObject(t *testing.T, s Subject) {
	w, ok := s.Model.(model.Walker)
	if !ok {
		t.Fatalf("model %d is not a model.Walker: GC refuses to collect a repository holding its objects", s.Model.ID())
	}
	for seed := uint64(0); seed < 4; seed++ {
		r := s.Write(t, s.Generate(seed))
		var order []hash.Hash
		named := map[hash.Hash]bool{}
		err := w.Walk(ctx, r, s.Store, func(h hash.Hash, _ bool) (bool, error) {
			order = append(order, h)
			fresh := !named[h]
			named[h] = true
			return fresh, nil
		})
		if err != nil {
			t.Fatalf("seed %d: Walk: %v", seed, err)
		}
		if len(order) == 0 || order[0] != r.Hash {
			t.Fatalf("seed %d: Walk did not name the object's root first", seed)
		}
		kept := memstore.New()
		for h := range named {
			b, err := s.Store.Get(ctx, h)
			if errors.Is(err, chunk.ErrNotFound) {
				continue // named but never stored: a fixture's stand-in for content
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := kept.Put(ctx, b); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.Model.Validate(ctx, r, kept); err != nil {
			t.Fatalf("seed %d: the chunks Walk names do not hold the object: from them alone it does not validate: %v", seed, err)
		}
	}
}
