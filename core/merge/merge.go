// Package merge is the generic three-way merge of namespaces (docs/DESIGN.md
// §8; Engine Spec L3). Both sides' diffs from base stream in path order and
// are zipped, so memory stays bounded however large the namespaces are. The
// driver decides what it can alone and asks an object's model only when both
// sides changed the same object under one model.
package merge

import (
	"context"
	"errors"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/object"
)

// DefaultMaxConflicts is the Engine Spec's limit.
const DefaultMaxConflicts = 100_000

// ErrTooManyConflicts aborts a merge past its conflict limit.
var ErrTooManyConflicts = errors.New("merge: too many conflicts")

// Kind says why a path conflicts.
type Kind uint8

// Conflict kinds.
const (
	BothChanged Kind = iota + 1 // one model's object changed on both sides, and the model could not combine them
	DeleteEdit                  // deleted on one side, changed on the other
	AddAdd                      // added differently on both sides
	ModelChange                 // the sides disagree about which model owns the path
)

// Conflict is one path the merge could not decide.
type Conflict struct {
	Path               string
	Kind               Kind
	Base, Ours, Theirs object.Ref // zero where absent
	Model              []model.Conflict
}

// Stats counts what a merge did.
type Stats struct {
	Added, Removed, Modified, Conflicts int
}

// Result is a merge's outcome. Merged holds ours with every change that
// could be decided applied; at a conflicting path it keeps ours.
type Result struct {
	Merged    *object.Namespace
	Conflicts []Conflict
	Stats     Stats
}

// Options tunes a merge.
type Options struct {
	MaxConflicts int // 0: DefaultMaxConflicts
}

// Merge merges theirs into ours, over base.
func Merge(ctx context.Context, reg *model.Registry, base, ours, theirs *object.Namespace, rw chunk.ReadWriter, o Options) (Result, error) {
	return Result{}, nil
}
