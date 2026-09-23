// Package gc collects a repository (docs/DESIGN.md §9): it marks everything
// the refs reach, condemns the packs nothing reaches, and deletes a pack only
// once it has been condemned for a grace window and is still unreached; then
// the index objects compaction replaced, and the objects no writer ever
// published, once they too are older than the window. It is the GC role: the
// one caller of BlobStore.Delete, through the raw store the repository never
// holds.
package gc

import (
	"context"
	"errors"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
)

// DefaultGrace is the Storage Core Spec's default grace window.
const DefaultGrace = 7 * 24 * time.Hour

// Options configures a collection.
type Options struct {
	Blobs    blob.BlobStore   // the raw store: the GC role deletes through it
	Keys     *seal.Keyring    // the repository's key
	Repo     seal.RepoID      // and id
	Config   prolly.Config    // how the repository's maps are written
	Registry *model.Registry  // every model its objects use; each must walk
	Grace    time.Duration    // 0: DefaultGrace
	Clock    func() time.Time // nil: time.Now
}

// Report is what one Run did.
type Report struct {
	Rounds               int      // a writer that publishes during a round starts another
	Live                 int      // chunks marked
	Condemned, Reprieved int      // packs
	Deleted              []string // expired packs and index objects, then orphans
}

// Run collects the repository once.
func Run(ctx context.Context, o Options) (Report, error) {
	return Report{}, errors.New("gc: Run is not written yet")
}
