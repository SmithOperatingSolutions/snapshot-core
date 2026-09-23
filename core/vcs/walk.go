package vcs

import (
	"context"
	"errors"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
)

// Walk calls visit for every chunk the repository whose refs map is root
// reaches (docs/DESIGN.md §9): the map's nodes; each branch's commits back to
// the first, their namespaces and what those name; each tag and the commit
// it names; each working set, its namespaces and, during a merge, its base
// and theirs commits and its conflicts map, whose records name objects too.
func Walk(ctx context.Context, rd chunk.Reader, o Options, root hash.Hash, visit func(h hash.Hash, leaf bool) (bool, error)) error {
	return errors.New("vcs: Walk is not written yet")
}
