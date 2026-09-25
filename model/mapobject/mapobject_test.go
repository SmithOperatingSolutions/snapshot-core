package mapobject_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/memstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
	"github.com/SmithOperatingSolutions/snapshot-core/model/mapobject"
)

var ctx = context.Background()

// notes is a map-shaped model whose records are values starting with 'v'.
func notes() mapobject.Spec {
	return mapobject.Spec{Name: "notes", Format: 7, Config: prolly.DefaultConfig(), Check: func(key, value []byte) error {
		if len(value) == 0 || value[0] != 'v' {
			return fmt.Errorf("%w: a note that is not a note under %q", chunk.ErrCorrupt, key)
		}
		return nil
	}}
}

func write(t *testing.T, s chunk.ReadWriter, sp mapobject.Spec, records map[string]string) model.Root {
	t.Helper()
	m, err := prolly.Empty(ctx, s, sp.Config)
	if err != nil {
		t.Fatal(err)
	}
	e := m.Editor()
	for k, v := range records {
		if err := e.Put([]byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	if m, err = e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	return sp.Root(m)
}

func read(t *testing.T, s chunk.ReadWriter, sp mapobject.Spec, root model.Root) map[string]string {
	t.Helper()
	m, err := sp.Open(ctx, s, root)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	it, err := m.IterRange(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for {
		k, v, ok, err := it.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			return out
		}
		out[string(k)] = string(v)
	}
}

func TestOpenChecksTheRootsClaims(t *testing.T) {
	s, sp := memstore.New(), notes()
	root := write(t, s, sp, map[string]string{"a": "v1", "b": "v2"})
	if got := read(t, s, sp, root); len(got) != 2 || got["a"] != "v1" {
		t.Fatalf("positive control: the map reads back as %v", got)
	}
	for name, bad := range map[string]struct {
		root model.Root
		want error
	}{
		"another format":    {model.Root{Hash: root.Hash, Size: root.Size, Format: 8}, model.ErrUnknownModel},
		"a stream depth":    {model.Root{Hash: root.Hash, Size: root.Size, Format: 7, Depth: 1}, chunk.ErrCorrupt},
		"a count that lies": {model.Root{Hash: root.Hash, Size: root.Size + 1, Format: 7}, chunk.ErrCorrupt},
	} {
		if _, err := sp.Open(ctx, s, bad.root); !errors.Is(err, bad.want) {
			t.Errorf("%s: Open = %v, want %v", name, err, bad.want)
		}
	}
}

func TestValidateRunsCheckOnEveryRecord(t *testing.T) {
	s, sp := memstore.New(), notes()
	good := write(t, s, sp, map[string]string{"a": "v1", "b": "v2"})
	if err := sp.Validate(ctx, good, s); err != nil {
		t.Fatalf("positive control: a map of notes does not validate: %v", err)
	}
	bad := write(t, s, sp, map[string]string{"a": "v1", "b": "not a note"})
	if err := sp.Validate(ctx, bad, s); !errors.Is(err, chunk.ErrCorrupt) || !strings.Contains(err.Error(), `"b"`) {
		t.Errorf("a map holding a record the model refuses validated: %v; want ErrCorrupt naming b", err)
	}
}

func TestDiffIsOneChangePerKey(t *testing.T) {
	s, sp := memstore.New(), notes()
	from := write(t, s, sp, map[string]string{"a": "v1", "b": "v2"})
	to := write(t, s, sp, map[string]string{"a": "v9", "c": "v3"})
	it, err := sp.Diff(ctx, from, to, s)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for {
		c, ok, err := it.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		got = append(got, fmt.Sprintf("%s:%d", c.Location, c.Kind))
	}
	want := []string{fmt.Sprintf("a:%d", model.Modified), fmt.Sprintf("b:%d", model.Removed), fmt.Sprintf("c:%d", model.Added)}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Errorf("Diff = %v, want %v: one change per key, in key order", got, want)
	}
}

// Merge per key with the default resolver: what one side changed lands,
// what both changed the same way is clean, what both changed differently
// or deleted against changed is a conflict at that key with a reason, and
// with any conflict the result is ours.
func TestMergePerKey(t *testing.T) {
	s, sp := memstore.New(), notes()
	base := write(t, s, sp, map[string]string{"a": "v1", "b": "v2", "c": "v3", "d": "v4", "same": "v5", "gone": "v6"})
	ours := write(t, s, sp, map[string]string{"a": "vA", "c": "v3", "d": "v4", "same": "vS", "e": "vE"})              // a changed, b deleted, same changed, e added, gone deleted
	theirs := write(t, s, sp, map[string]string{"a": "v1", "b": "v2", "c": "vC", "same": "vS", "f": "vF", "e": "vE"}) // c changed, d deleted, same changed the same way, e added the same, f added, gone deleted
	r, err := sp.Merge(ctx, base, ours, theirs, s, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Conflicts) != 0 {
		t.Fatalf("non-overlapping edits conflicted: %+v", r.Conflicts)
	}
	got := read(t, s, sp, r.Root)
	want := map[string]string{"a": "vA", "c": "vC", "same": "vS", "e": "vE", "f": "vF"}
	if fmt.Sprint(sorted(got)) != fmt.Sprint(sorted(want)) {
		t.Errorf("merged = %v, want %v", sorted(got), sorted(want))
	}
	if r.Root.Format != 7 || r.Root.Size != 5 {
		t.Errorf("the merged root claims format %d and %d records, want 7 and 5", r.Root.Format, r.Root.Size)
	}

	oursX := write(t, s, sp, map[string]string{"a": "vA", "b": "vB2", "d": "v4"})                         // a changed, b changed, c deleted, x absent
	theirsX := write(t, s, sp, map[string]string{"a": "vZ", "b": "v2", "c": "vC", "d": "v4", "x": "vX1"}) // a changed differently, b kept, c changed (ours deleted it), x added
	baseX := write(t, s, sp, map[string]string{"a": "v1", "b": "v2", "c": "v3", "d": "v4"})
	oursX2 := write(t, s, sp, map[string]string{"a": "vA", "b": "vB2", "d": "v4", "x": "vX2"}) // and x added differently
	r, err = sp.Merge(ctx, baseX, oursX2, theirsX, s, nil)
	if err != nil {
		t.Fatal(err)
	}
	reasons := map[string]string{}
	for _, c := range r.Conflicts {
		reasons[string(c.Location)] = c.Reason
	}
	for key, want := range map[string]string{"a": "changed differently", "c": "deleted on one side", "x": "added differently"} {
		if !strings.Contains(reasons[key], want) {
			t.Errorf("conflict at %s: %q, want a reason saying %q", key, reasons[key], want)
		}
	}
	if len(r.Conflicts) != 3 {
		t.Errorf("%d conflicts %+v, want exactly a, c and x", len(r.Conflicts), r.Conflicts)
	}
	if r.Root != oursX2 {
		t.Errorf("with conflicts the result is %+v, want ours %+v untouched", r.Root, oursX2)
	}
	_ = oursX
}

// A model's resolver decides a key both sides changed: here the two values
// join, and the joined record goes through the model's Check.
func TestAResolverCombinesBothSides(t *testing.T) {
	s, sp := memstore.New(), notes()
	base := write(t, s, sp, map[string]string{"a": "v", "b": "v"})
	ours := write(t, s, sp, map[string]string{"a": "vO", "b": "v"})
	theirs := write(t, s, sp, map[string]string{"a": "vT", "b": "v"})
	join := func(key []byte, o, th prolly.Change) ([]byte, bool, string) {
		return append(append([]byte{}, o.To...), th.To[1:]...), true, ""
	}
	r, err := sp.Merge(ctx, base, ours, theirs, s, join)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Conflicts) != 0 {
		t.Fatalf("the resolver combined both sides and still conflicts: %+v", r.Conflicts)
	}
	if got := read(t, s, sp, r.Root); got["a"] != "vOT" {
		t.Errorf("merged a = %q, want the resolver's vOT", got["a"])
	}
	bad := func(key []byte, o, th prolly.Change) ([]byte, bool, string) { return []byte("not a note"), true, "" }
	if _, err := sp.Merge(ctx, base, ours, theirs, s, bad); !errors.Is(err, chunk.ErrCorrupt) {
		t.Errorf("a resolver writing a record the model refuses merged: %v, want ErrCorrupt", err)
	}
}

func TestWalkNamesTheMapAndEachRecord(t *testing.T) {
	s, sp := memstore.New(), notes()
	records := map[string]string{}
	for i := range 300 {
		records[fmt.Sprintf("k%03d", i)] = fmt.Sprintf("v%d", i)
	}
	root := write(t, s, sp, records)
	seen := map[hash.Hash]bool{}
	each := 0
	err := sp.Walk(ctx, root, s, func(h hash.Hash, leaf bool) (bool, error) {
		seen[h] = true
		return true, nil
	}, func(key, value []byte) error { each++; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if !seen[root.Hash] {
		t.Error("Walk did not name the root")
	}
	if each != 300 {
		t.Errorf("each was called %d times, want once per record (300)", each)
	}
	if err := sp.Walk(ctx, model.Root{Hash: root.Hash, Size: root.Size, Format: 8}, s, func(hash.Hash, bool) (bool, error) { return true, nil }, nil); !errors.Is(err, model.ErrUnknownModel) {
		t.Errorf("Walk of another format = %v, want ErrUnknownModel", err)
	}
}

func sorted(m map[string]string) []string {
	var out []string
	for k, v := range m {
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return out
}

// The store's failures surface, not a zero map: a root whose chunk is not
// stored fails Open, Diff and Merge; a record theirs adds that the model
// refuses fails the merge; the read-only store refuses a write; Walk with
// nothing to do per record still walks.
func TestStoreFailuresSurface(t *testing.T) {
	s, sp := memstore.New(), notes()
	good := write(t, s, sp, map[string]string{"a": "v1"})
	missing := model.Root{Hash: hash.Sum([]byte("never stored")), Size: 1, Format: 7}
	if _, err := sp.Open(ctx, s, missing); err == nil {
		t.Error("Open of a root whose chunk is not stored succeeded")
	}
	if _, err := sp.Diff(ctx, good, missing, s); err == nil {
		t.Error("Diff to a root whose chunk is not stored succeeded")
	}
	if _, err := sp.Diff(ctx, missing, good, s); err == nil {
		t.Error("Diff from a root whose chunk is not stored succeeded")
	}
	if _, err := sp.Merge(ctx, missing, good, good, s, nil); err == nil {
		t.Error("Merge over a base whose chunk is not stored succeeded")
	}
	theirs := write(t, s, sp, map[string]string{"a": "v1", "b": "not a note"})
	if _, err := sp.Merge(ctx, good, good, theirs, s, nil); !errors.Is(err, chunk.ErrCorrupt) {
		t.Errorf("a merge applying a record the model refuses = %v, want ErrCorrupt", err)
	}
	if _, err := mapobject.ReadOnly(s).Put(ctx, []byte("x")); err == nil {
		t.Error("the read-only store took a write")
	}
	if err := sp.Walk(ctx, good, s, func(hash.Hash, bool) (bool, error) { return true, nil }, nil); err != nil {
		t.Errorf("Walk with nothing to do per record: %v", err)
	}
	if err := sp.Walk(ctx, model.Root{Hash: good.Hash, Size: 1, Format: 7, Depth: 2}, s, func(hash.Hash, bool) (bool, error) { return true, nil }, nil); !errors.Is(err, chunk.ErrCorrupt) {
		t.Errorf("Walk of a root claiming a stream depth = %v, want ErrCorrupt", err)
	}
}

// A decider reports conflicts below the key, in the model's own location
// encoding, each kept as given; one with no location is at the key; with
// any conflict the result is ours, untouched; and a key the decider leaves
// clean, or combines, merges as a resolver's would.
func TestADeciderLocatesConflictsBelowTheKey(t *testing.T) {
	s, sp := memstore.New(), notes()
	base := write(t, s, sp, map[string]string{"a": "v", "b": "v", "c": "v"})
	ours := write(t, s, sp, map[string]string{"a": "vO", "b": "vO", "c": "vO"})
	theirs := write(t, s, sp, map[string]string{"a": "vT", "b": "vT", "c": "vT"})
	decide := func(key []byte, o, th prolly.Change) (mapobject.Decision, error) {
		switch string(key) {
		case "a": // two fields of the record collide
			return mapobject.Decision{Conflicts: []model.Conflict{
				{Location: []byte("a/x"), Reason: "field x changed differently"},
				{Location: []byte("a/y"), Reason: "field y changed differently"},
			}}, nil
		case "b": // the record as a whole
			return mapobject.Decision{Conflicts: []model.Conflict{{Reason: "deleted on one side"}}}, nil
		}
		return mapobject.Decision{Value: []byte("vOT"), Put: true}, nil // combined
	}
	r, err := sp.MergeWith(ctx, base, ours, theirs, s, decide)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, c := range r.Conflicts {
		got = append(got, string(c.Location)+": "+c.Reason)
	}
	want := []string{"a/x: field x changed differently", "a/y: field y changed differently", "b: deleted on one side"}
	if strings.Join(got, "; ") != strings.Join(want, "; ") {
		t.Errorf("conflicts = %v, want %v: each where the decider put it, the key's where it put none", got, want)
	}
	if r.Root != ours {
		t.Errorf("with conflicts the result is %+v, want ours %+v untouched", r.Root, ours)
	}

	onlyC := func(key []byte, o, th prolly.Change) (mapobject.Decision, error) {
		if string(key) == "c" {
			return mapobject.Decision{Value: []byte("vOT"), Put: true}, nil
		}
		return mapobject.Decision{}, nil // the sides agree: ours stays
	}
	r, err = sp.MergeWith(ctx, base, ours, theirs, s, onlyC)
	if err != nil || len(r.Conflicts) != 0 {
		t.Fatalf("a decider leaving every key clean merged to %+v, %v", r.Conflicts, err)
	}
	if got := read(t, s, sp, r.Root); got["a"] != "vO" || got["b"] != "vO" || got["c"] != "vOT" {
		t.Errorf("merged = %v, want a and b as ours and c combined", sorted(got))
	}
}

// A decider's error aborts the merge with that error and no result: a
// record that does not decode mid-merge is the store's problem, not a
// conflict for a person to resolve.
func TestADecidersErrorAbortsTheMerge(t *testing.T) {
	s, sp := memstore.New(), notes()
	base := write(t, s, sp, map[string]string{"a": "v"})
	ours := write(t, s, sp, map[string]string{"a": "vO"})
	theirs := write(t, s, sp, map[string]string{"a": "vT"})
	errUndecodable := errors.New("a record that does not decode")
	r, err := sp.MergeWith(ctx, base, ours, theirs, s, func([]byte, prolly.Change, prolly.Change) (mapobject.Decision, error) {
		return mapobject.Decision{}, errUndecodable
	})
	if !errors.Is(err, errUndecodable) {
		t.Fatalf("a decider failing gave %v, want its error", err)
	}
	if r.Root != (model.Root{}) || len(r.Conflicts) != 0 {
		t.Errorf("a failed merge returned %+v: nothing must come of it", r)
	}
	r, err = sp.MergeWith(ctx, base, ours, theirs, s, func([]byte, prolly.Change, prolly.Change) (mapobject.Decision, error) {
		return mapobject.Decision{Value: []byte("not a note"), Put: true}, nil
	})
	if !errors.Is(err, chunk.ErrCorrupt) {
		t.Errorf("a decider storing a record the model refuses merged: %v, want ErrCorrupt", err)
	}
}

// A key both sides changed to the same value still goes to the resolver, so
// a model whose values add (a counter) can add both changes: a counter at
// 1000 that each side set to 1001 merges to 1002. The default resolver
// keeps such a key as it is.
func TestAResolverIsAskedAboutTheSameValueOnBothSides(t *testing.T) {
	s, sp := memstore.New(), notes()
	base := write(t, s, sp, map[string]string{"hits": "v1000"})
	same := write(t, s, sp, map[string]string{"hits": "v1001"})
	add := func(key []byte, o, th prolly.Change) ([]byte, bool, string) {
		n := func(v []byte) int { i, _ := strconv.Atoi(string(v[1:])); return i }
		return []byte("v" + strconv.Itoa(n(o.To)+n(th.To)-n(o.From))), true, ""
	}
	r, err := sp.Merge(ctx, base, same, same, s, add)
	if err != nil || len(r.Conflicts) != 0 {
		t.Fatalf("the same increment on both sides = conflicts %+v, %v; want a clean merge", r.Conflicts, err)
	}
	if got := read(t, s, sp, r.Root)["hits"]; got != "v1002" {
		t.Errorf("a counter each side added one to from 1000 reads %s after the merge, want v1002: an increment was lost", got)
	}
	if r, err := sp.Merge(ctx, base, same, same, s, nil); err != nil || read(t, s, sp, r.Root)["hits"] != "v1001" {
		t.Errorf("the default resolver changed the same value on both sides: %v", err)
	}
}
