// Package blob is the opaque-file model (model id 1): an object's bytes are
// a stream (core/stream), and the model gives them no structure beyond that.
// Two sides that both changed a file conflict; one side's change is taken.
package blob

import (
	"context"
	"fmt"
	"io"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/stream"
)

// ID is the blob model's id.
const ID model.ID = 1

// Format is the format version this package writes.
const Format = 1

// Model is the blob model.
type Model struct{}

var (
	_ model.Model  = Model{}
	_ model.Walker = Model{}
)

// ID implements model.Model.
func (Model) ID() model.ID { return ID }

// FormatVersion implements model.Model.
func (Model) FormatVersion() uint16 { return Format }

// Write stores everything r yields as a blob.
func Write(ctx context.Context, w chunk.Writer, r io.Reader, c stream.Config) (model.Root, error) {
	ref, err := stream.Write(ctx, w, r, c)
	if err != nil {
		return model.Root{}, err
	}
	return model.Root{Hash: ref.Root, Size: ref.Size, Depth: ref.Depth, Format: Format}, nil
}

// Open reads a blob.
func Open(ctx context.Context, rd chunk.Reader, root model.Root) (*stream.Reader, error) {
	if root.Format != Format {
		return nil, fmt.Errorf("%w: blob format %d", model.ErrUnknownModel, root.Format)
	}
	return stream.Open(ctx, rd, stream.Ref{Root: root.Hash, Size: root.Size, Depth: root.Depth})
}

// Walk implements model.Walker: a blob is its stream.
func (Model) Walk(ctx context.Context, root model.Root, r chunk.Reader, visit func(hash.Hash) (bool, error)) error {
	if root.Format != Format {
		return fmt.Errorf("%w: blob format %d", model.ErrUnknownModel, root.Format)
	}
	return stream.Walk(ctx, r, stream.Ref{Root: root.Hash, Size: root.Size, Depth: root.Depth}, visit)
}

// Validate implements model.Model: the whole stream reads and verifies.
func (Model) Validate(ctx context.Context, root model.Root, r chunk.Reader) error {
	rd, err := Open(ctx, r, root)
	if err != nil {
		return err
	}
	_, err = io.Copy(io.Discard, rd)
	return err
}

// changes is a DiffIter over a list.
type changes []model.Change

func (c *changes) Next(context.Context) (model.Change, bool, error) {
	if len(*c) == 0 {
		return model.Change{}, false, nil
	}
	next := (*c)[0]
	*c = (*c)[1:]
	return next, true, nil
}

// Diff implements model.Model: a blob changes as a whole (a nil location).
func (Model) Diff(ctx context.Context, from, to model.Root, r chunk.Reader) (model.DiffIter, error) {
	if from == to {
		return &changes{}, nil
	}
	return &changes{{Kind: model.Modified}}, nil
}

// Merge implements model.Model: one side's change is taken, the same change
// on both is taken once, and two different changes are one conflict.
func (Model) Merge(ctx context.Context, base, ours, theirs model.Root, rw chunk.ReadWriter) (model.MergeResult, error) {
	switch {
	case ours == theirs || theirs == base:
		return model.MergeResult{Root: ours}, nil
	case ours == base:
		return model.MergeResult{Root: theirs}, nil
	}
	return model.MergeResult{Root: ours, Conflicts: []model.Conflict{{Reason: "both sides changed the file"}}}, nil
}
