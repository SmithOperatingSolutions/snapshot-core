// Package contract is the suite every data model must pass, unchanged
// (Storage Core Spec, "Data-model plugins": round-trip, determinism,
// diff-matches-edits, merge(b, o, o) == o; each model also fuzzes its own
// decoders).
package contract

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
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
	t.Run("ValidateRefusesGarbage", func(t *testing.T) { validateRefusesGarbage(t, newSubject(t)) })
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
		if len(r.Conflicts) != 0 || r.Root != tc.want {
			t.Errorf("%s: root %+v with %d conflicts, want %+v and none", name, r.Root, len(r.Conflicts), tc.want)
		}
	}
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
