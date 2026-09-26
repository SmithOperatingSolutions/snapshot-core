package merge_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	"pgregory.net/rapid"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/memstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/merge"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/object"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
)

var ctx = context.Background()

// tb is what the helpers need from a test: *testing.T and *rapid.T both have it.
type tb interface {
	Helper()
	Fatal(args ...any)
	Fatalf(format string, args ...any)
}

// counting is a memstore that counts reads.
type counting struct {
	*memstore.Store
	gets atomic.Int64
}

func (c *counting) Get(ctx context.Context, h hash.Hash) ([]byte, error) {
	c.gets.Add(1)
	return c.Store.Get(ctx, h)
}

// lines (model 7) is a set of lines, merged as sets: what either side added
// is in, what either side removed is out. It never conflicts.
type lines struct{}

func (lines) ID() model.ID                                             { return 7 }
func (lines) FormatVersion() uint16                                    { return 1 }
func (lines) Validate(context.Context, model.Root, chunk.Reader) error { return nil }
func (lines) Diff(context.Context, model.Root, model.Root, chunk.Reader) (model.DiffIter, error) {
	return nil, errors.New("unused")
}

func set(ctx context.Context, r chunk.Reader, root model.Root) (map[string]bool, error) {
	b, err := r.Get(ctx, root.Hash)
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, l := range strings.Split(string(b), "\n") {
		if l != "" {
			out[l] = true
		}
	}
	return out, nil
}

func (lines) Merge(ctx context.Context, b, o, t model.Root, rw chunk.ReadWriter) (model.MergeResult, error) {
	var sets [3]map[string]bool
	for i, r := range []model.Root{b, o, t} {
		var err error
		if sets[i], err = set(ctx, rw, r); err != nil {
			return model.MergeResult{}, err
		}
	}
	out := map[string]bool{}
	for l := range sets[0] {
		if sets[1][l] && sets[2][l] {
			out[l] = true
		}
	}
	for _, side := range sets[1:] {
		for l := range side {
			if !sets[0][l] {
				out[l] = true
			}
		}
	}
	return model.MergeResult{Root: linesRoot(ctx, rw, out)}, nil
}

func linesRoot(ctx context.Context, w chunk.Writer, s map[string]bool) model.Root {
	ls := make([]string, 0, len(s))
	for l := range s {
		ls = append(ls, l)
	}
	sort.Strings(ls)
	b := []byte(strings.Join(ls, "\n"))
	h, err := w.Put(ctx, b)
	if err != nil {
		panic(err)
	}
	return model.Root{Hash: h, Size: uint64(len(b)), Format: 1}
}

// strict (model 8) cannot combine any two changes; failing (model 9) errors.
type strict struct{ lines }

func (strict) ID() model.ID { return 8 }
func (strict) Merge(context.Context, model.Root, model.Root, model.Root, chunk.ReadWriter) (model.MergeResult, error) {
	return model.MergeResult{Conflicts: []model.Conflict{{Location: []byte("all of it"), Reason: "strict"}}}, nil
}

var errModelFailed = errors.New("the model failed")

type failing struct{ lines }

func (failing) ID() model.ID { return 9 }
func (failing) Merge(context.Context, model.Root, model.Root, model.Root, chunk.ReadWriter) (model.MergeResult, error) {
	return model.MergeResult{}, errModelFailed
}

type fixture struct {
	t   tb
	s   *counting
	reg *model.Registry
}

func newFixture(t tb) *fixture {
	reg, err := model.NewRegistry(lines{}, strict{}, failing{})
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{t: t, s: &counting{Store: memstore.New()}, reg: reg}
}

// obj is an object of model m holding the given lines.
func (f *fixture) obj(m model.ID, ls ...string) object.Ref {
	s := map[string]bool{}
	for _, l := range ls {
		s[l] = true
	}
	return object.Ref{Model: m, Root: linesRoot(ctx, f.s, s)}
}

func (f *fixture) ns(entries map[string]object.Ref) *object.Namespace {
	f.t.Helper()
	n, err := object.New(ctx, f.s, prolly.DefaultConfig(), f.reg)
	if err != nil {
		f.t.Fatal(err)
	}
	e := n.Editor()
	for p, r := range entries {
		if err := e.Put(p, r); err != nil {
			f.t.Fatal(err)
		}
	}
	if n, err = e.Flush(ctx); err != nil {
		f.t.Fatal(err)
	}
	return n
}

func (f *fixture) merge(base, ours, theirs map[string]object.Ref, o merge.Options) (merge.Result, error) {
	f.t.Helper()
	return merge.Merge(ctx, f.reg, f.ns(base), f.ns(ours), f.ns(theirs), f.s, o)
}

// rootOf is a result's merged root, zero when it has none.
func rootOf(r merge.Result) hash.Hash {
	if r.Merged == nil {
		return hash.Hash{}
	}
	return r.Merged.Root()
}

func get(t tb, n *object.Namespace, p string) (object.Ref, bool) {
	t.Helper()
	r, _, ok, err := n.Get(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	return r, ok
}

// Engine Spec L3: "One table-driven test per row in the rule table", with
// the rows DESIGN §8 adds.
func TestEveryRuleOfTheTable(t *testing.T) {
	f := newFixture(t)
	x, y, z := f.obj(7, "a"), f.obj(7, "a", "b"), f.obj(7, "a", "c")
	combined := f.obj(7, "a", "b", "c")
	sx, sy, sz := f.obj(8, "a"), f.obj(8, "a", "b"), f.obj(8, "a", "c")
	var none object.Ref
	cases := []struct {
		name         string
		base, o, t   object.Ref // zero: absent
		want         object.Ref // zero: absent
		conflict     merge.Kind // 0: none
		modelConflic int
	}{
		{"x x y takes y", x, x, y, y, 0, 0},
		{"x y x takes y", x, y, x, y, 0, 0},
		{"x y y takes y (convergent)", x, y, y, y, 0, 0},
		{"x y z asks the model", x, y, z, combined, 0, 0},
		{"x y z the model cannot combine", sx, sy, sz, sy, merge.BothChanged, 1},
		{"absent y z conflicts", none, y, z, y, merge.AddAdd, 0},
		{"absent y y takes y", none, y, y, y, 0, 0},
		{"absent absent z takes z", none, none, z, z, 0, 0},
		{"absent y absent takes y", none, y, none, y, 0, 0},
		{"x deleted y conflicts", x, none, y, none, merge.DeleteEdit, 0},
		{"x y deleted conflicts", x, y, none, y, merge.DeleteEdit, 0},
		{"x deleted deleted is deleted", x, none, none, none, 0, 0},
		{"x deleted x is deleted", x, none, x, none, 0, 0},
		{"x x deleted is deleted", x, x, none, none, 0, 0},
		{"a change of model conflicts", x, sy, z, sy, merge.ModelChange, 0},
	}
	for _, c := range cases {
		m := func(r object.Ref) map[string]object.Ref {
			out := map[string]object.Ref{"other": x}
			if r != none {
				out["p"] = r
			}
			return out
		}
		ours := m(c.o)
		ours["mine"] = y // ours also changed something, so no row is a fast-forward
		r, err := f.merge(m(c.base), ours, m(c.t), merge.Options{})
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if r.Merged == nil {
			t.Fatalf("%s: the merge returned no namespace", c.name)
		}
		got, ok := get(t, r.Merged, "p")
		if (c.want == none) == ok || (ok && got != c.want) {
			t.Errorf("%s: p is %+v (present %v), want %+v", c.name, got, ok, c.want)
		}
		if other, _ := get(t, r.Merged, "other"); other != x {
			t.Errorf("%s: an untouched path changed", c.name)
		}
		if mine, _ := get(t, r.Merged, "mine"); mine != y {
			t.Errorf("%s: ours' own change elsewhere was lost", c.name)
		}
		switch {
		case c.conflict == 0 && len(r.Conflicts) != 0:
			t.Errorf("%s: conflicts %+v, want none", c.name, r.Conflicts)
		case c.conflict != 0 && (len(r.Conflicts) != 1 || r.Conflicts[0].Kind != c.conflict || r.Conflicts[0].Path != "p" ||
			len(r.Conflicts[0].Model) != c.modelConflic):
			t.Errorf("%s: conflicts %+v, want one %v at p with %d model conflicts", c.name, r.Conflicts, c.conflict, c.modelConflic)
		case c.conflict != 0 && (r.Conflicts[0].Base != c.base || r.Conflicts[0].Ours != c.o || r.Conflicts[0].Theirs != c.t):
			t.Errorf("%s: the conflict records %+v, want base, ours and theirs", c.name, r.Conflicts[0])
		}
	}
}

// Engine Spec L3: "Fast-forward: when base == ours, result root equals
// theirs' root with zero work".
func TestFastForwardDoesNoWork(t *testing.T) {
	f := newFixture(t)
	base := f.ns(f.deep(map[string]object.Ref{"a": f.obj(7, "a")}))
	theirs := f.ns(f.deep(map[string]object.Ref{"a": f.obj(7, "b"), "c": f.obj(7, "c")}))
	f.seesADiff(base, theirs)
	f.s.gets.Store(0)
	r, err := merge.Merge(ctx, f.reg, base, base, theirs, f.s, merge.Options{})
	if err != nil || rootOf(r) != theirs.Root() || len(r.Conflicts) != 0 {
		t.Fatalf("a fast-forward merged to %v (%d conflicts, %v), want theirs %v", rootOf(r), len(r.Conflicts), err, theirs.Root())
	}
	if n := f.s.gets.Load(); n != 0 {
		t.Fatalf("a fast-forward read %d nodes, want none", n)
	}
	if r, err := merge.Merge(ctx, f.reg, base, theirs, base, f.s, merge.Options{}); err != nil || rootOf(r) != theirs.Root() {
		t.Fatalf("merging an unchanged theirs gave %v, %v; want ours", rootOf(r), err)
	}
}

// deep adds a thousand paths to entries, the same on every side: a
// namespace of more than one node, whose diff reads nodes even though a
// map holds its root (#43). In a one-node namespace every merge reads
// nothing, and a test that counts reads cannot tell a diff from none.
func (f *fixture) deep(entries map[string]object.Ref) map[string]object.Ref {
	filler := f.obj(7, "filler")
	for i := range 1000 {
		entries[fmt.Sprintf("filler/%04d", i)] = filler
	}
	return entries
}

// seesADiff fails the test unless diffing from with to reads a node: the
// fixture's positive control, that a read count can see a diff.
func (f *fixture) seesADiff(from, to *object.Namespace) {
	f.t.Helper()
	f.s.gets.Store(0)
	d, err := object.Diff(ctx, from, to)
	if err != nil {
		f.t.Fatal(err)
	}
	for {
		_, ok, err := d.Next()
		if err != nil {
			f.t.Fatal(err)
		}
		if !ok {
			break
		}
	}
	if f.s.gets.Load() == 0 {
		f.t.Fatal("fixture: a diff of the two namespaces read no node, so a count of reads cannot see one")
	}
}

func draw(rt *rapid.T, f *fixture, label string) map[string]object.Ref {
	n := rapid.IntRange(0, 60).Draw(rt, label+" n")
	out := map[string]object.Ref{}
	for i := 0; i < n; i++ {
		p := fmt.Sprintf("d%d/f%d", rapid.IntRange(0, 5).Draw(rt, label+" dir"), rapid.IntRange(0, 40).Draw(rt, label+" file"))
		out[p] = f.obj(7, rapid.SampledFrom([]string{"a", "b", "c", "d"}).Draw(rt, label+" line"))
	}
	return out
}

func edit(rt *rapid.T, f *fixture, base map[string]object.Ref, keep func(string) bool, label string) map[string]object.Ref {
	out := map[string]object.Ref{}
	for p, r := range base {
		out[p] = r
	}
	for p := range draw(rt, f, label) {
		if !keep(p) {
			continue
		}
		if rapid.Bool().Draw(rt, label+" delete") {
			delete(out, p)
		} else {
			out[p] = f.obj(7, "edited", p)
		}
	}
	return out
}

// Engine Spec L3 "Property: merge(base, ours, ours) == ours for any edits".
func TestMergingTheSameEditsIsIdentity(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		f := newFixture(rt)
		base := draw(rt, f, "base")
		ours := edit(rt, f, base, func(string) bool { return true }, "ours")
		r, err := f.merge(base, ours, ours, merge.Options{})
		if err != nil || len(r.Conflicts) != 0 || rootOf(r) != f.ns(ours).Root() {
			rt.Fatalf("merge(b, o, o) = %v with %d conflicts (%v), want o", rootOf(r), len(r.Conflicts), err)
		}
	})
}

// Engine Spec L3 "Property: merges with no overlapping keys are symmetric:
// merge(b, o, t) == merge(b, t, o)".
func TestDisjointMergesAreSymmetric(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		f := newFixture(rt)
		base := draw(rt, f, "base")
		ours := edit(rt, f, base, func(p string) bool { return p < "d3" }, "ours")
		theirs := edit(rt, f, base, func(p string) bool { return p >= "d3" }, "theirs")
		a, err := f.merge(base, ours, theirs, merge.Options{})
		if err != nil {
			rt.Fatal(err)
		}
		b, err := f.merge(base, theirs, ours, merge.Options{})
		if err != nil {
			rt.Fatal(err)
		}
		if len(a.Conflicts)+len(b.Conflicts) != 0 || rootOf(a) != rootOf(b) || rootOf(a).IsZero() {
			rt.Fatalf("merge(b, o, t) = %v, merge(b, t, o) = %v (conflicts %d, %d)", rootOf(a), rootOf(b), len(a.Conflicts), len(b.Conflicts))
		}
	})
}

// Engine Spec L3: a model's error aborts the whole merge.
func TestAModelErrorAbortsTheMerge(t *testing.T) {
	f := newFixture(t)
	x, y, z := f.obj(9, "a"), f.obj(9, "b"), f.obj(9, "c")
	if _, err := f.merge(map[string]object.Ref{"p": x}, map[string]object.Ref{"p": y}, map[string]object.Ref{"p": x}, merge.Options{}); err != nil {
		t.Fatalf("positive control: a merge that never asks the failing model: %v", err)
	}
	if _, err := f.merge(map[string]object.Ref{"p": x}, map[string]object.Ref{"p": y}, map[string]object.Ref{"p": z}, merge.Options{}); !errors.Is(err, errModelFailed) {
		t.Fatalf("a merge whose model failed = %v, want the model's error", err)
	}
}

// Engine Spec L3: past the conflict limit the merge aborts cleanly.
func TestTooManyConflictsAbort(t *testing.T) {
	f := newFixture(t)
	conflicted := func(n int) (map[string]object.Ref, map[string]object.Ref) {
		o, th := map[string]object.Ref{}, map[string]object.Ref{}
		for i := 0; i < n; i++ {
			p := fmt.Sprintf("p%d", i)
			o[p], th[p] = f.obj(7, "ours", p), f.obj(7, "theirs", p)
		}
		return o, th
	}
	o, th := conflicted(10)
	if r, err := f.merge(nil, o, th, merge.Options{MaxConflicts: 10}); err != nil || len(r.Conflicts) != 10 {
		t.Fatalf("positive control: 10 conflicts at a limit of 10: %d, %v", len(r.Conflicts), err)
	}
	o, th = conflicted(11)
	if r, err := f.merge(nil, o, th, merge.Options{MaxConflicts: 10}); !errors.Is(err, merge.ErrTooManyConflicts) {
		t.Fatalf("11 conflicts at a limit of 10 = %d conflicts, %v; want ErrTooManyConflicts", len(r.Conflicts), err)
	}
}

// Engine Spec L3: "Merging 1M-row tables with 10 changed rows reads fewer
// than 1,000 nodes" — here 100,000 paths with ten changes on each side.
func TestMergingLargeNamespacesReadsLittle(t *testing.T) {
	f := newFixture(t)
	x := f.obj(7, "x")
	base := map[string]object.Ref{}
	for i := 0; i < 100000; i++ {
		base[fmt.Sprintf("dir%03d/file%05d", i%500, i)] = x
	}
	ours, theirs := map[string]object.Ref{}, map[string]object.Ref{}
	for p, r := range base {
		ours[p], theirs[p] = r, r
	}
	for i := 0; i < 10; i++ {
		ours[fmt.Sprintf("dir%03d/file%05d", (i*7919)%500, i*7919)] = f.obj(7, "ours", fmt.Sprint(i))
		theirs[fmt.Sprintf("dir%03d/file%05d", (i*104729+3)%500, (i*104729+3)%100000)] = f.obj(7, "theirs", fmt.Sprint(i))
	}
	b, o, th := f.ns(base), f.ns(ours), f.ns(theirs)
	f.s.gets.Store(0)
	r, err := merge.Merge(ctx, f.reg, b, o, th, f.s, merge.Options{})
	if err != nil || len(r.Conflicts) != 0 {
		t.Fatalf("merge: %d conflicts, %v", len(r.Conflicts), err)
	}
	if n := f.s.gets.Load(); n >= 1000 {
		t.Fatalf("merging ten changes a side into 100,000 paths read %d nodes, want fewer than 1,000", n)
	}
	if r.Stats.Modified != 10 {
		t.Fatalf("the merge applied %d of theirs' changes, want 10", r.Stats.Modified)
	}
}
