package gc

import (
	"context"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/packstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
)

// Mark is mark, for the property's bound on what a repository holds.
func Mark(ctx context.Context, rd chunk.Reader, o Options, root hash.Hash) (packstore.Live, func() error, error) {
	l, err := mark(ctx, rd, o, root)
	if err != nil {
		return nil, nil, err
	}
	return l, l.Close, nil
}
