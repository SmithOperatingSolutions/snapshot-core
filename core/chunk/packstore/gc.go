package packstore

import (
	"context"
	"errors"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
)

// ErrMoved is Apply's answer when the manifest changed after Begin: a writer
// published, the round's mark is stale, and a new round must start.
var ErrMoved = errors.New("packstore: the manifest moved during the GC round")

// Round is one GC decision taken against one manifest (docs/DESIGN.md §9):
// Begin reads it, GC marks from its root, and Apply condemns, reprieves and
// expires against it, losing to any writer that published in between.
type Round struct{}

// Begin reads the manifest and every index object it lists.
func Begin(ctx context.Context, o Options) (*Round, error) {
	return nil, errors.New("packstore: Begin is not written yet")
}

// Root is the refs root GC marks from.
func (r *Round) Root() hash.Hash { return hash.Hash{} }

// Outcome is what a round did.
type Outcome struct {
	Condemned, Reprieved int             // packs
	Expired              []string        // packs and index objects the manifest no longer names: the GC role deletes them
	Named                map[string]bool // every pack and index object the manifest now names, live or condemned
}

// Apply decides against the round's manifest and swaps it: a pack none of
// whose chunks is live is condemned at now; a condemned pack with a live
// chunk is reprieved; one condemned at least grace before now and still
// unmarked expires, as does an index object condemned that long ago.
// Expired packs leave the index objects, which are rewritten (and the
// replaced ones condemned), and gcGen moves.
func (r *Round) Apply(ctx context.Context, live func(hash.Hash) bool, now time.Time, grace time.Duration) (Outcome, error) {
	return Outcome{}, errors.New("packstore: Apply is not written yet")
}
