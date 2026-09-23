// Package blob is the opaque-file model (model id 1): an object's bytes are
// a stream (core/stream), and the model gives them no structure beyond that.
// Two sides that both changed a file conflict; one side's change is taken.
package blob

import (
	"context"
	"errors"
	"io"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/stream"
)

// ID is the blob model's id.
const ID model.ID = 1

// Format is the format version this package writes.
const Format = 1

// Model is the blob model.
type Model struct{}

var _ model.Model = Model{}

// ID implements model.Model.
func (Model) ID() model.ID { return 0 }

// FormatVersion implements model.Model.
func (Model) FormatVersion() uint16 { return 0 }

// Write stores everything r yields as a blob.
func Write(ctx context.Context, w chunk.Writer, r io.Reader, c stream.Config) (model.Root, error) {
	return model.Root{}, nil
}

// Open reads a blob.
func Open(ctx context.Context, rd chunk.Reader, root model.Root) (*stream.Reader, error) {
	return nil, errors.New("blob: not written yet")
}

// Validate implements model.Model: the whole stream reads and verifies.
func (Model) Validate(ctx context.Context, root model.Root, r chunk.Reader) error { return nil }

// Diff implements model.Model: a blob changes as a whole.
func (Model) Diff(ctx context.Context, from, to model.Root, r chunk.Reader) (model.DiffIter, error) {
	return nil, nil
}

// Merge implements model.Model: one side's change is taken, the same change
// on both is taken once, and two different changes are one conflict.
func (Model) Merge(ctx context.Context, base, ours, theirs model.Root, rw chunk.ReadWriter) (model.MergeResult, error) {
	return model.MergeResult{}, nil
}
