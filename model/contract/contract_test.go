package contract

import (
	"errors"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
)

// The verdict on two colliding edits: a merge that reports a conflict, with
// a reason for the person resolving it, passes; one that reports none took
// a side in silence and fails; one whose conflict has no reason fails.
func TestCollisionVerdict(t *testing.T) {
	if err := collisionVerdict(model.MergeResult{Conflicts: []model.Conflict{{Location: []byte("x"), Reason: "both sides changed it"}}}); err != nil {
		t.Errorf("a merge reporting a reasoned conflict was refused: %v", err)
	}
	if err := collisionVerdict(model.MergeResult{}); err == nil {
		t.Error("a merge of colliding edits that reported no conflict passed: the model took a side in silence")
	}
	if err := collisionVerdict(model.MergeResult{Conflicts: []model.Conflict{{Location: []byte("x")}}}); err == nil {
		t.Error("a conflict with no reason passed")
	}
}

// adder is a model that says whether it accumulates the same change on both
// sides.
type adder struct {
	model.Model
	acc bool
}

func (a adder) Accumulates() bool { return a.acc }

// The verdict on a merge identity: the merge is clean and is the root the
// identity names. A model that accumulates may merge the same change on
// both sides, merge(b, o, o), to something other than o, but cleanly and to
// a valid object; every other identity, and every other model, is exact.
func TestIdentityVerdict(t *testing.T) {
	b, o, th, x := root(1), root(2), root(3), root(4)
	valid := func(model.Root) error { return nil }
	invalid := func(model.Root) error { return errors.New("not an object of the model's") }
	sums, takes, silent := adder{acc: true}, adder{acc: false}, struct{ model.Model }{}
	clean := func(r model.Root) model.MergeResult { return model.MergeResult{Root: r} }
	conflicted := model.MergeResult{Root: o, Conflicts: []model.Conflict{{Location: []byte("k"), Reason: "both"}}}
	for _, c := range []struct {
		name      string
		m         model.Model
		b, o, t   model.Root
		want      model.Root
		r         model.MergeResult
		valid     func(model.Root) error
		wantError bool
	}{
		{"merge(b, o, o) == o, a model that does not accumulate", takes, b, o, o, o, clean(o), valid, false},
		{"merge(b, o, o) == o, a model that accumulates", sums, b, o, o, o, clean(o), valid, false},
		{"merge(b, o, o) is something else, a model that accumulates", sums, b, o, o, o, clean(x), valid, false},
		{"merge(b, o, o) is something else, a model that says it does not accumulate", takes, b, o, o, o, clean(x), valid, true},
		{"merge(b, o, o) is something else, a model with no answer", silent, b, o, o, o, clean(x), valid, true},
		{"merge(b, o, o) is something invalid, a model that accumulates", sums, b, o, o, o, clean(x), invalid, true},
		{"merge(b, o, o) conflicts, a model that accumulates", sums, b, o, o, o, conflicted, valid, true},
		{"merge(b, b, t) is not t, a model that accumulates", sums, b, b, th, th, clean(x), valid, true},
		{"merge(b, b, b) is not b, a model that accumulates", sums, b, b, b, b, clean(x), valid, true},
		{"merge(b, o, b) is not o, a model that accumulates", sums, b, o, b, o, clean(x), valid, true},
		{"merge(b, o, b) conflicts", takes, b, o, b, o, conflicted, valid, true},
	} {
		err := identityVerdict(c.m, c.b, c.o, c.t, c.want, c.r, c.valid)
		if (err != nil) != c.wantError {
			t.Errorf("%s: the verdict = %v, want refused %v", c.name, err, c.wantError)
		}
	}
}

func root(n byte) model.Root { return model.Root{Hash: hash.Sum([]byte{n}), Size: 1, Format: 1} }
