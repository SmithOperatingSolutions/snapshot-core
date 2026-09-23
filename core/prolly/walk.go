package prolly

import (
	"context"
	"errors"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
)

// Walk calls visit for every chunk the map at root is made of, the root
// first: its nodes, each checked as a read checks it, and the chunks of its
// long values' streams; visit says whether to go on into what a chunk
// reaches. When value is not nil, each value of a leaf Walk goes into is
// handed to it, long ones read whole, so a caller can walk what values name.
func Walk(ctx context.Context, rd chunk.Reader, c Config, root hash.Hash, visit func(hash.Hash) (bool, error), value func(key, val []byte) error) error {
	return errors.New("prolly: Walk is not written yet")
}
