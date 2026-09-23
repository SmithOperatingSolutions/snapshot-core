// Package gc collects a repository (docs/DESIGN.md §9): it marks everything
// the refs reach, condemns the packs nothing reaches, and deletes a pack only
// once it has been condemned for a grace window and is still unreached; then
// the index objects compaction replaced, and the objects no writer ever
// published, once they too are older than the window. It is the GC role: the
// one caller of BlobStore.Delete, through the raw store the repository never
// holds.
package gc

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/packstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
	"github.com/SmithOperatingSolutions/snapshot-core/core/vcs"
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
	Repack   packstore.Repack // how packs that are mostly dead are rewritten; the zero value is the default policy
}

// Report is what one Run did.
type Report struct {
	Rounds               int      // a writer that publishes during a round starts another
	Live                 int      // chunks marked
	Condemned, Reprieved int      // packs
	Deleted              []string // expired packs and index objects, then orphans
	Repacked             int      // packs whose live chunks were copied into new packs
	Copied               int64    // bytes of frames copied by repacking
}

// maxRounds bounds the rounds one Run takes while writers keep publishing.
const maxRounds = 10

// Run collects the repository once: it takes a round against the manifest,
// marks from its root, and applies the round, starting over when a writer
// published meanwhile; then it deletes what expired, and the packs and
// index objects no manifest names that are older than the grace window.
// A repository it cannot walk whole (an object whose model is unknown or
// cannot walk, a forged chunk) is refused before anything is decided.
func Run(ctx context.Context, o Options) (Report, error) {
	if o.Blobs == nil || o.Keys == nil || o.Registry == nil {
		return Report{}, errors.New("gc: a store, a key and a model registry are required")
	}
	grace, clock := o.Grace, o.Clock
	if grace == 0 {
		grace = DefaultGrace
	}
	if grace < 0 {
		return Report{}, fmt.Errorf("gc: a grace window of %v", grace)
	}
	if clock == nil {
		clock = time.Now
	}
	po := packstore.Options{Blobs: o.Blobs, Keys: o.Keys, Repo: o.Repo}
	rd, err := packstore.Open(ctx, po)
	if err != nil {
		return Report{}, err
	}
	defer rd.Close()
	var rep Report
	for rep.Rounds < maxRounds {
		rep.Rounds++
		r, err := packstore.Begin(ctx, po)
		if err != nil {
			return rep, err
		}
		live, err := mark(ctx, rd, o, r.Root())
		if err != nil {
			return rep, err
		}
		now, err := backendNow(ctx, o.Blobs)
		if err != nil {
			return rep, err
		}
		old, probes, err := orphans(ctx, o.Blobs, now, grace)
		if err != nil {
			return rep, err
		}
		r.Orphans(old) // recorded in the swap, deleted only once it lands
		out, err := r.Apply(ctx, func(h hash.Hash) bool { return live[h] }, clock(), grace)
		if errors.Is(err, packstore.ErrMoved) {
			continue
		}
		if err != nil {
			return rep, err
		}
		rep.Live, rep.Condemned, rep.Reprieved = len(live), out.Condemned, out.Reprieved
		for _, name := range append(append(out.Expired, out.Orphans...), probes...) {
			if err := o.Blobs.Delete(ctx, name); err != nil {
				return rep, err
			}
			rep.Deleted = append(rep.Deleted, name)
		}
		return rep, nil
	}
	return rep, fmt.Errorf("gc: the repository changed during %d rounds in a row", maxRounds)
}

// mark returns every chunk the repository at root reaches. A chunk seen so
// far only as a leaf is still gone into when it turns up as a node.
func mark(ctx context.Context, rd chunk.Reader, o Options, root hash.Hash) (map[hash.Hash]bool, error) {
	live := map[hash.Hash]bool{}
	if root.IsZero() {
		return live, nil
	}
	gone := map[hash.Hash]bool{}
	err := vcs.Walk(ctx, rd, vcs.Options{Config: o.Config, Registry: o.Registry}, root, func(h hash.Hash, leaf bool) (bool, error) {
		live[h] = true
		if leaf || gone[h] {
			return false, nil
		}
		gone[h] = true
		return true, nil
	})
	return live, err
}

// backendNow reads the backend's clock, the one its objects are stamped by:
// the stamp a probe put now gets. An object's age is only read against it.
func backendNow(ctx context.Context, bs blob.BlobStore) (time.Time, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return time.Time{}, err
	}
	name := probePrefix + hex.EncodeToString(b[:])
	if err := bs.Put(ctx, name, bytes.NewReader(nil), 0); err != nil {
		return time.Time{}, err
	}
	info, err := bs.Stat(ctx, name)
	if derr := bs.Delete(ctx, name); err == nil {
		err = derr
	}
	return info.ModTime, err
}

const probePrefix = "gc/clock-"

// orphans lists the packs and index objects older than the grace window by
// the backend's clock, for the round to keep those its manifest names and
// record the rest (left by writers that never published, or by a run that
// stopped between its swap and its deletions); and, apart, the probes a
// stopped run left behind.
func orphans(ctx context.Context, bs blob.BlobStore, now time.Time, grace time.Duration) (old, probes []string, err error) {
	for _, prefix := range []string{"packs/", "index/", probePrefix} {
		after := ""
		for {
			page, err := bs.List(ctx, prefix, after, blob.MaxListPage)
			if err != nil {
				return nil, nil, err
			}
			for _, info := range page {
				switch {
				case now.Sub(info.ModTime) < grace:
				case prefix == probePrefix:
					probes = append(probes, info.Name)
				default:
					old = append(old, info.Name)
				}
			}
			if len(page) < blob.MaxListPage {
				break
			}
			after = page[len(page)-1].Name
		}
	}
	return old, probes, nil
}
