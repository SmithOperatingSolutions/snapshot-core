package merge_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/memstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/merge"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/object"
)

// counter is an object holding one number in decimal, and its merge adds
// both sides' changes from base: one added on each side is two. Model 10
// says it accumulates identical changes; model 11 is the same counter
// saying it does not, and so keeps the rule that the same change on both
// sides is taken once.
type counter struct{ acc bool }

func (c counter) ID() model.ID {
	if c.acc {
		return 10
	}
	return 11
}
func (counter) FormatVersion() uint16                                    { return 1 }
func (counter) Validate(context.Context, model.Root, chunk.Reader) error { return nil }
func (counter) Diff(context.Context, model.Root, model.Root, chunk.Reader) (model.DiffIter, error) {
	return nil, nil
}
func (c counter) Accumulates() bool { return c.acc }

func (counter) Merge(ctx context.Context, b, o, t model.Root, rw chunk.ReadWriter) (model.MergeResult, error) {
	var n [3]int64
	for i, r := range []model.Root{b, o, t} {
		data, err := rw.Get(ctx, r.Hash)
		if err != nil {
			return model.MergeResult{}, err
		}
		if n[i], err = strconv.ParseInt(string(data), 10, 64); err != nil {
			return model.MergeResult{}, err
		}
	}
	return model.MergeResult{Root: counterRoot(ctx, rw, n[1]+n[2]-n[0])}, nil
}

func counterRoot(ctx context.Context, w chunk.Writer, n int64) model.Root {
	b := []byte(strconv.FormatInt(n, 10))
	h, err := w.Put(ctx, b)
	if err != nil {
		panic(err)
	}
	return model.Root{Hash: h, Size: uint64(len(b)), Format: 1}
}

// fixtureOf is a fixture over the given models.
func fixtureOf(t tb, models ...model.Model) *fixture {
	reg, err := model.NewRegistry(models...)
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{t: t, s: &counting{Store: memstore.New()}, reg: reg}
}

func (f *fixture) count(id model.ID, n int64) object.Ref {
	return object.Ref{Model: id, Root: counterRoot(ctx, f.s, n)}
}

// A model that accumulates is asked to merge a path both sides changed to
// the same object: a counter each side added one to from 1000 is 1002,
// whether that is the only change on both sides (the two namespaces come
// out identical) or one change among others. A counter that says it does
// not accumulate still has the same change taken once, and so does the same
// counter added on both sides, which has no base to count from.
func TestAnAccumulatingModelIsAskedAboutTheSameChangeOnBothSides(t *testing.T) {
	f := fixtureOf(t, counter{acc: true}, counter{acc: false}, lines{})
	c, d := func(n int64) object.Ref { return f.count(10, n) }, func(n int64) object.Ref { return f.count(11, n) }
	for _, tc := range []struct {
		name               string
		base, ours, theirs map[string]object.Ref
		want               map[string]string
	}{{
		name:   "two branches each adding one to the only counter, from 1000",
		base:   map[string]object.Ref{"hits": c(1000)},
		ours:   map[string]object.Ref{"hits": c(1001)},
		theirs: map[string]object.Ref{"hits": c(1001)},
		want:   map[string]string{"hits": "1002"},
	}, {
		name:   "two branches each adding one to a counter from 1000, beside other changes",
		base:   map[string]object.Ref{"hits": c(1000), "a": d(1), "b": d(1)},
		ours:   map[string]object.Ref{"hits": c(1001), "a": d(2), "b": d(1)},
		theirs: map[string]object.Ref{"hits": c(1001), "a": d(1), "b": d(2)},
		want:   map[string]string{"hits": "1002", "a": "2", "b": "2"},
	}, {
		name:   "the same change to a counter that does not accumulate",
		base:   map[string]object.Ref{"hits": d(1000), "x": d(1)},
		ours:   map[string]object.Ref{"hits": d(1001), "x": d(2)},
		theirs: map[string]object.Ref{"hits": d(1001), "x": d(1)},
		want:   map[string]string{"hits": "1001", "x": "2"},
	}, {
		name:   "the same accumulating counter added on both sides",
		base:   map[string]object.Ref{"x": d(0)},
		ours:   map[string]object.Ref{"x": d(0), "hits": c(5)},
		theirs: map[string]object.Ref{"x": d(1), "hits": c(5)},
		want:   map[string]string{"hits": "5", "x": "1"},
	}} {
		r, err := f.merge(tc.base, tc.ours, tc.theirs, merge.Options{})
		if err != nil || len(r.Conflicts) != 0 {
			t.Errorf("%s: the merge = conflicts %+v, %v; want a clean merge", tc.name, r.Conflicts, err)
			continue
		}
		for path, want := range tc.want {
			ref, ok := get(t, r.Merged, path)
			if !ok {
				t.Errorf("%s: %s is gone after the merge", tc.name, path)
				continue
			}
			got, err := f.s.Get(ctx, ref.Root.Hash)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != want {
				t.Errorf("%s: %s reads %s after the merge, want %s", tc.name, path, got, want)
			}
		}
	}
}

// Beside a model that accumulates, a model that does not implement the
// interface is still never asked about the same change on both sides: one
// that cannot combine anything (strict) and one that fails (failing) merge
// it cleanly to that change, as they always have.
func TestAModelThatDoesNotAccumulateIsNotAskedAboutTheSameChange(t *testing.T) {
	f := fixtureOf(t, counter{acc: true}, lines{}, strict{}, failing{})
	base := map[string]object.Ref{"s": f.obj(8, "a"), "f": f.obj(9, "a")}
	same := map[string]object.Ref{"s": f.obj(8, "b"), "f": f.obj(9, "b")}
	r, err := f.merge(base, same, same, merge.Options{})
	if err != nil || len(r.Conflicts) != 0 {
		t.Fatalf("the same change on both sides to models that do not accumulate = conflicts %+v, %v; want it taken once, the models unasked", r.Conflicts, err)
	}
	if rootOf(r) != f.ns(same).Root() {
		t.Errorf("the merge of the same change on both sides is not that change")
	}
}
