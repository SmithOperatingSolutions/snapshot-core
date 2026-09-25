package model_test

import (
	"context"
	"errors"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
)

// fake is a model that only has an identity.
type fake struct {
	id     model.ID
	format uint16
}

func (f fake) ID() model.ID                                             { return f.id }
func (f fake) FormatVersion() uint16                                    { return f.format }
func (f fake) Validate(context.Context, model.Root, chunk.Reader) error { return nil }
func (f fake) Diff(context.Context, model.Root, model.Root, chunk.Reader) (model.DiffIter, error) {
	return nil, nil
}
func (f fake) Merge(context.Context, model.Root, model.Root, model.Root, chunk.ReadWriter) (model.MergeResult, error) {
	return model.MergeResult{}, nil
}

// Storage Core Spec: "NewRegistry with two models sharing an id returns an
// error and no registry".
func TestNewRegistryRefusesTwoModelsWithOneID(t *testing.T) {
	if r, err := model.NewRegistry(fake{1, 1}, fake{2, 1}); err != nil || r == nil {
		t.Fatalf("positive control: %v", err)
	}
	for name, ms := range map[string][]model.Model{
		"two models with one id": {fake{1, 1}, fake{2, 1}, fake{1, 3}},
		"a nil model":            {fake{1, 1}, nil},
		"model id 0":             {fake{0, 1}},
	} {
		r, err := model.NewRegistry(ms...)
		if err == nil || r != nil {
			t.Errorf("%s: NewRegistry = %v, %v; want an error and no registry", name, r, err)
		}
	}
}

func TestResolveKnowsOnlyItsModelsAndFormats(t *testing.T) {
	blob, tree := fake{1, 1}, fake{2, 3}
	r, err := model.NewRegistry(tree, blob)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		id     model.ID
		format uint16
		want   model.Model
	}{{1, 1, blob}, {2, 1, tree}, {2, 3, tree}} {
		if m, err := r.Resolve(c.id, c.format); err != nil || m != c.want {
			t.Errorf("Resolve(%d, %d) = %v, %v; want the registered model", c.id, c.format, m, err)
		}
	}
	for name, c := range map[string]struct {
		id     model.ID
		format uint16
	}{
		"an unregistered model":         {3, 1},
		"model id 0":                    {0, 1},
		"a format newer than the model": {2, 4},
		"format 0":                      {1, 0},
	} {
		if m, err := r.Resolve(c.id, c.format); !errors.Is(err, model.ErrUnknownModel) || m != nil {
			t.Errorf("%s: Resolve(%d, %d) = %v, %v; want ErrUnknownModel", name, c.id, c.format, m, err)
		}
	}
	if ids := r.IDs(); len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Errorf("IDs = %v, want [1 2]", ids)
	}
}

// A registry cannot change once built: not through the slice it was built
// from, nor through the one IDs returns.
func TestARegistryCannotChange(t *testing.T) {
	ms := []model.Model{fake{1, 1}, fake{2, 1}}
	r, err := model.NewRegistry(ms...)
	if err != nil {
		t.Fatal(err)
	}
	ms[0] = fake{9, 1}
	ids := r.IDs()
	if len(ids) != 2 {
		t.Fatalf("IDs = %v, want two", ids)
	}
	ids[0] = 9
	if _, err := r.Resolve(1, 1); err != nil {
		t.Errorf("changing the slice a registry was built from changed it: %v", err)
	}
	if _, err := r.Resolve(9, 1); !errors.Is(err, model.ErrUnknownModel) {
		t.Error("a model no one registered resolves")
	}
	if again := r.IDs(); again[0] != 1 {
		t.Errorf("changing the slice IDs returned changed the registry: %v", again)
	}
}

// adding is a model that says whether it accumulates.
type adding struct {
	fake
	acc bool
}

func (a adding) Accumulates() bool { return a.acc }

// A model accumulates when it implements Accumulator and says so; a
// registry accumulates when one of its models does. A model that does not
// implement the interface, or says false, does not, and a registry of only
// those does not.
func TestAModelAccumulatesWhenItSaysSo(t *testing.T) {
	yes, no, silent := adding{fake{1, 1}, true}, adding{fake{2, 1}, false}, fake{3, 1}
	for m, want := range map[model.Model]bool{yes: true, no: false, silent: false} {
		if got := model.Accumulates(m); got != want {
			t.Errorf("model %d accumulates = %v, want %v", m.ID(), got, want)
		}
	}
	for name, c := range map[string]struct {
		models []model.Model
		want   bool
	}{
		"one model of three says it accumulates":    {[]model.Model{silent, yes, no}, true},
		"one says it does not, one does not answer": {[]model.Model{silent, no}, false},
		"no models": {nil, false},
	} {
		r, err := model.NewRegistry(c.models...)
		if err != nil {
			t.Fatal(err)
		}
		if got := r.Accumulates(); got != c.want {
			t.Errorf("%s: the registry accumulates = %v, want %v, so an identical change is %s", name, got, c.want,
				map[bool]string{true: "summed by its model", false: "taken once"}[c.want])
		}
	}
}
