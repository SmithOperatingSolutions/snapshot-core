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
	"github.com/SmithOperatingSolutions/snapshot-core/core/dedup"
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
	// A collection's memory does not grow with the repository (#6): its
	// reader indexes at most IndexInMemory published chunks in memory
	// (0: packstore.DefaultIndexInMemory), the rest in a table under
	// WorkDir ("": the system's temporary directory), where the mark and the
	// round's own index go too; MarkNodes (0: DefaultMarkNodes) bounds the
	// nodes the walk remembers to skip, past which a shared subtree is walked
	// again, costing time and never chunks.
	WorkDir       string
	IndexInMemory int
	MarkNodes     int
}

// DefaultMarkNodes is the nodes a walk remembers: about 60 MiB.
const DefaultMarkNodes = 1 << 20

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
	// The reader checks the work directory at Open, before anything needs
	// it. The walk reads each chunk once: a cache would only hold memory.
	// Without the journal: GC is a reader beside any writer, a journal
	// writer included, and replays a journal a writer that stopped left
	// (packstore, #34).
	po := packstore.Options{Blobs: o.Blobs, Keys: o.Keys, Repo: o.Repo, IndexDir: o.WorkDir, IndexInMemory: o.IndexInMemory, CacheBytes: -1,
		Journal: packstore.JournalOff}
	rd, err := packstore.Open(ctx, po)
	if err != nil {
		return Report{}, err
	}
	defer rd.Close()
	var rep Report
	for rep.Rounds < maxRounds {
		rep.Rounds++
		out, live, err := round(ctx, rd, po, o, clock(), grace)
		if errors.Is(err, packstore.ErrMoved) {
			continue
		}
		if err != nil {
			return rep, err
		}
		rep.Live, rep.Condemned, rep.Reprieved = live, out.Condemned, out.Reprieved
		rep.Repacked, rep.Copied = out.Repacked, out.Copied
		for _, name := range append(out.Expired, out.Orphans...) {
			if err := o.Blobs.Delete(ctx, name); err != nil {
				return rep, err
			}
			rep.Deleted = append(rep.Deleted, name)
		}
		return rep, nil
	}
	return rep, fmt.Errorf("gc: the repository changed during %d rounds in a row", maxRounds)
}

// round takes one round: it begins against the manifest, marks from its
// root, lists the orphans and applies. It returns what the round did and
// how many chunks were live; the round and the mark are closed either way.
func round(ctx context.Context, rd chunk.Reader, po packstore.Options, o Options, now time.Time, grace time.Duration) (packstore.Outcome, int, error) {
	r, err := packstore.Begin(ctx, po)
	if err != nil {
		return packstore.Outcome{}, 0, err
	}
	defer r.Close()
	live, err := mark(ctx, rd, o, r.Root())
	if err != nil {
		return packstore.Outcome{}, 0, err
	}
	defer live.Close()
	backend, err := backendNow(ctx, o.Blobs)
	if err != nil {
		return packstore.Outcome{}, 0, err
	}
	old, probes, err := orphans(ctx, o.Blobs, backend, grace)
	if err != nil {
		return packstore.Outcome{}, 0, err
	}
	r.Orphans(old) // recorded in the swap, deleted only once it lands
	r.Repack(o.Repack)
	out, err := r.Apply(ctx, live, now, grace)
	if err != nil {
		return packstore.Outcome{}, 0, err
	}
	out.Orphans = append(out.Orphans, probes...)
	return out, int(live.Len()), nil
}

// liveTable is the mark: every chunk the repository reaches, sorted in a
// table on disk (#6).
type liveTable struct{ t *dedup.Table }

func (l *liveTable) Has(h hash.Hash) (bool, error) { return l.t.Has(h) }
func (l *liveTable) Len() int64                    { return l.t.Len() }
func (l *liveTable) Close() error                  { return l.t.Close() }
func (l *liveTable) Each(f func(hash.Hash) error) error {
	c := l.t.Cursor()
	for {
		h, _, ok, err := c.Next()
		if err != nil || !ok {
			return err
		}
		if err := f(h); err != nil {
			return err
		}
	}
}

// mark returns every chunk the repository at root reaches, as a table on
// disk. The walk remembers up to o.MarkNodes nodes it has gone into and
// skips them when reached again; past that a shared subtree is walked
// again, at a cost in time and never in chunks. A chunk seen so far only
// as a leaf is still gone into when it turns up as a node.
func mark(ctx context.Context, rd chunk.Reader, o Options, root hash.Hash) (*liveTable, error) {
	b, err := dedup.NewBuilder(o.WorkDir, 0)
	if err != nil {
		return nil, err
	}
	limit := o.MarkNodes
	if limit == 0 {
		limit = DefaultMarkNodes
	}
	if !root.IsZero() {
		seen := map[hash.Hash]struct{}{}
		err := vcs.Walk(ctx, rd, vcs.Options{Config: o.Config, Registry: o.Registry}, root, func(h hash.Hash, leaf bool) (bool, error) {
			if err := b.Add(h, nil); err != nil {
				return false, err
			}
			if leaf {
				return false, nil
			}
			if _, ok := seen[h]; ok {
				return false, nil
			}
			if len(seen) < limit {
				seen[h] = struct{}{}
			}
			return true, nil
		})
		if err != nil {
			b.Abort()
			return nil, err
		}
	}
	t, err := b.Finish()
	if err != nil {
		return nil, fmt.Errorf("gc: marking into %s: %w", o.WorkDir, err)
	}
	return &liveTable{t}, nil
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
