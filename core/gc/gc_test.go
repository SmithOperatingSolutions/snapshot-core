package gc_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/auth"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/mem"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/packstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/gc"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/object"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
	"github.com/SmithOperatingSolutions/snapshot-core/core/vcs"
)

var (
	ctx   = context.Background()
	alice = auth.Principal{ID: "user:alice"}
	repo  = seal.RepoID{0x6c}
	grace = time.Hour
)

// note (model 7) is an object of one chunk: its bytes. It walks; mute (8)
// is the same but cannot walk.
type note struct{}

func (note) ID() model.ID                                             { return 7 }
func (note) FormatVersion() uint16                                    { return 1 }
func (note) Validate(context.Context, model.Root, chunk.Reader) error { return nil }
func (note) Diff(context.Context, model.Root, model.Root, chunk.Reader) (model.DiffIter, error) {
	return nil, errors.New("unused")
}
func (note) Merge(context.Context, model.Root, model.Root, model.Root, chunk.ReadWriter) (model.MergeResult, error) {
	return model.MergeResult{}, errors.New("unused")
}
func (note) Walk(_ context.Context, root model.Root, _ chunk.Reader, visit func(hash.Hash, bool) (bool, error)) error {
	_, err := visit(root.Hash, true)
	return err
}

type mute struct{}

func (mute) ID() model.ID                                             { return 8 }
func (mute) FormatVersion() uint16                                    { return 1 }
func (mute) Validate(context.Context, model.Root, chunk.Reader) error { return nil }
func (mute) Diff(context.Context, model.Root, model.Root, chunk.Reader) (model.DiffIter, error) {
	return nil, errors.New("unused")
}
func (mute) Merge(context.Context, model.Root, model.Root, model.Root, chunk.ReadWriter) (model.MergeResult, error) {
	return model.MergeResult{}, errors.New("unused")
}

// world is a repository on a packstore over one raw blob store. GC's clock
// runs with real time plus a jump the test sets; the backend stamps objects
// by a clock of its own, the same plus a skew.
type world struct {
	t     *testing.T
	blobs blob.BlobStore
	keys  *seal.Keyring
	reg   *model.Registry
	s     *packstore.Store
	r     *vcs.Repo
	jump  time.Duration // GC's clock, ahead of real time
	skew  time.Duration // the backend's clock, ahead of GC's
}

// clocked is a blob store that stamps each object by the backend's clock.
type clocked struct {
	blob.BlobStore
	now    func() time.Time
	mu     sync.Mutex
	stamps map[string]time.Time
}

func (c *clocked) Put(ctx context.Context, name string, r io.Reader, size int64) error {
	err := c.BlobStore.Put(ctx, name, r, size)
	if err == nil {
		c.mu.Lock()
		c.stamps[name] = c.now()
		c.mu.Unlock()
	}
	return err
}

func (c *clocked) stamped(i blob.Info) blob.Info {
	c.mu.Lock()
	defer c.mu.Unlock()
	if t, ok := c.stamps[i.Name]; ok {
		i.ModTime = t
	}
	return i
}

func (c *clocked) Stat(ctx context.Context, name string) (blob.Info, error) {
	i, err := c.BlobStore.Stat(ctx, name)
	return c.stamped(i), err
}

func (c *clocked) List(ctx context.Context, prefix, after string, limit int) ([]blob.Info, error) {
	is, err := c.BlobStore.List(ctx, prefix, after, limit)
	for k := range is {
		is[k] = c.stamped(is[k])
	}
	return is, err
}

func newWorld(t *testing.T) *world {
	t.Helper()
	w := &world{t: t}
	w.blobs = &clocked{BlobStore: mem.New(), stamps: map[string]time.Time{},
		now: func() time.Time { return time.Now().Add(w.jump + w.skew) }}
	var err error
	if w.keys, err = seal.NewKeyring(); err != nil {
		t.Fatal(err)
	}
	if w.reg, err = model.NewRegistry(note{}, mute{}); err != nil {
		t.Fatal(err)
	}
	w.s = w.store()
	if w.r, err = vcs.Init(ctx, w.s, alice, w.vcs()); err != nil {
		t.Fatal(err)
	}
	return w
}

// store opens a packstore with packs small enough that a repository spans
// many of them.
func (w *world) store() *packstore.Store {
	w.t.Helper()
	s, err := packstore.Open(ctx, packstore.Options{Blobs: w.blobs, Keys: w.keys, Repo: repo, PackSize: 4 << 10})
	if err != nil {
		w.t.Fatal(err)
	}
	w.t.Cleanup(func() { _ = s.Close() })
	return s
}

func (w *world) vcs() vcs.Options {
	return vcs.Options{Config: prolly.DefaultConfig(), Registry: w.reg, Authorizer: auth.AllowAll{}}
}

func (w *world) now() time.Time { return time.Now().Add(w.jump) }

func (w *world) gc() (gc.Report, error) {
	return gc.Run(ctx, gc.Options{Blobs: w.blobs, Keys: w.keys, Repo: repo, Config: prolly.DefaultConfig(),
		Registry: w.reg, Grace: grace, Clock: w.now})
}

// note stores content as an object of model m.
func (w *world) note(m model.ID, content string) object.Ref {
	w.t.Helper()
	h, err := w.s.Put(ctx, []byte(content))
	if err != nil {
		w.t.Fatal(err)
	}
	return object.Ref{Model: m, Root: model.Root{Hash: h, Size: uint64(len(content)), Format: 1}}
}

func (w *world) put(branch, path string, ref object.Ref) {
	w.t.Helper()
	ws, err := w.r.WorkingSet(ctx, alice, branch)
	if err != nil {
		w.t.Fatal(err)
	}
	n, err := w.r.Namespace(ctx, ws.Working)
	if err != nil {
		w.t.Fatal(err)
	}
	e := n.Editor()
	if err := e.Put(path, ref); err != nil {
		w.t.Fatal(err)
	}
	if n, err = e.Flush(ctx); err != nil {
		w.t.Fatal(err)
	}
	next := ws
	next.Working, next.Staged = n.Root(), n.Root()
	if _, err := w.r.UpdateWorkingSet(ctx, alice, branch, ws, next); err != nil {
		w.t.Fatal(err)
	}
}

func (w *world) commit(branch, msg string) vcs.Commit {
	w.t.Helper()
	c, err := w.r.CommitWorkingSet(ctx, alice, branch, msg)
	if err != nil {
		w.t.Fatal(err)
	}
	return c
}

// readable checks, on a fresh store, that every branch's history reads down
// to every object's content, and returns the contents it read.
func (w *world) readable() map[string]bool {
	w.t.Helper()
	s := w.store()
	r, err := vcs.Open(ctx, s, w.vcs())
	if err != nil {
		w.t.Fatalf("after GC the repository does not open: %v", err)
	}
	none, err := object.New(ctx, packstoreScratch(w.t), prolly.DefaultConfig(), w.reg)
	if err != nil {
		w.t.Fatal(err)
	}
	contents := map[string]bool{}
	branches, err := r.Branches(ctx, alice)
	if err != nil {
		w.t.Fatal(err)
	}
	for _, b := range branches {
		head, err := r.Head(ctx, alice, b)
		if err != nil {
			w.t.Fatal(err)
		}
		log, err := r.Log(ctx, alice, head.Hash, vcs.MaxLog)
		if err != nil {
			w.t.Fatalf("after GC branch %s's history does not read: %v", b, err)
		}
		for _, c := range log {
			n, err := r.Namespace(ctx, c.Namespace)
			if err != nil {
				w.t.Fatalf("after GC commit %q's namespace does not open: %v", c.Message, err)
			}
			d, err := object.Diff(ctx, none, n)
			if err != nil {
				w.t.Fatal(err)
			}
			for {
				ch, ok, err := d.Next()
				if err != nil {
					w.t.Fatalf("after GC commit %q's namespace does not read: %v", c.Message, err)
				}
				if !ok {
					break
				}
				b, err := s.Get(ctx, ch.To.Root.Hash)
				if err != nil {
					w.t.Fatalf("after GC %s in commit %q does not read: %v", ch.Path, c.Message, err)
				}
				contents[string(b)] = true
			}
		}
	}
	return contents
}

// packstoreScratch is a throwaway store for the empty namespace diffs start from.
func packstoreScratch(t *testing.T) chunk.ReadWriter {
	t.Helper()
	s, err := packstore.Open(ctx, packstore.Options{Blobs: mem.New(), Keys: mustKeys(t), Repo: repo})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func mustKeys(t *testing.T) *seal.Keyring {
	t.Helper()
	k, err := seal.NewKeyring()
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func count(t *testing.T, bs blob.BlobStore, prefix string) int {
	t.Helper()
	infos, err := bs.List(ctx, prefix, "", blob.MaxListPage)
	if err != nil {
		t.Fatal(err)
	}
	return len(infos)
}

// Orphan ages are the backend's to tell (DESIGN §9): its clock stamps the
// objects, so GC reads the backend's time too, and measures ages by it. A
// backend ten days behind GC's clock does not make a writer's upload of a
// moment ago look old; and when GC's clock runs a grace window ahead of a
// backend's that stood still, expired packs still go (their expiry is GC's
// own reckoning) while an object new by the backend's clock stays.
func TestOrphanAgesAreTheBackendsClock(t *testing.T) {
	w := newWorld(t)
	w.skew = -10 * 24 * time.Hour
	fresh := "packs/cd/cd" + strings.Repeat("0", 62)
	if err := w.blobs.Put(ctx, fresh, strings.NewReader("just uploaded"), 13); err != nil {
		t.Fatal(err)
	}
	if _, err := w.gc(); err != nil {
		t.Fatal(err)
	}
	if n := count(t, w.blobs, "packs/cd/"); n != 1 {
		t.Fatal("GC deleted an upload of a moment ago because the backend's clock is behind its own")
	}

	w = newWorld(t)
	w.put(vcs.MainBranch, "a", w.note(7, "a draft"))
	w.put(vcs.MainBranch, "a", w.note(7, "kept"))
	w.commit(vcs.MainBranch, "kept")
	if first, err := w.gc(); err != nil || first.Condemned == 0 {
		t.Fatalf("fixture: the first run condemned %d packs (%v), want some", first.Condemned, err)
	}
	if err := w.blobs.Put(ctx, fresh, strings.NewReader("new by the backend"), 18); err != nil {
		t.Fatal(err)
	}
	packs := count(t, w.blobs, "packs/")
	w.jump, w.skew = grace+time.Minute, -(grace + time.Minute) // GC's clock ahead, the backend's standing still
	second, err := w.gc()
	if err != nil {
		t.Fatal(err)
	}
	if n := count(t, w.blobs, "packs/"); n >= packs {
		t.Fatalf("with GC's clock a grace window on, %d packs remain of %d: the expired ones were left for an orphan scan the backend's clock cannot pass", n, packs)
	}
	if n := count(t, w.blobs, "packs/cd/"); n != 1 {
		t.Fatalf("an object new by the backend's clock was deleted (deleted %v)", second.Deleted)
	}
	if got := w.readable(); !got["kept"] {
		t.Fatal("after GC the kept note does not read")
	}
}

// The spec's GC property on one history (DESIGN §9): GC keeps everything
// the refs reach and, a grace window after condemning them, deletes the
// packs nothing reaches: a deleted branch's, and every replaced working set
// and refs node's; an object no writer published is deleted once older
// than the window, and not before. After every run the whole repository
// reads.
func TestGCKeepsWhatTheRefsReachAndDeletesTheRest(t *testing.T) {
	w := newWorld(t)
	for i := range 6 {
		w.put(vcs.MainBranch, fmt.Sprintf("notes/%d", i), w.note(7, fmt.Sprintf("note %d on main", i)))
		w.commit(vcs.MainBranch, fmt.Sprint("main ", i))
	}
	if err := w.r.CreateBranch(ctx, alice, "gone", w.commitOf(vcs.MainBranch)); err != nil {
		t.Fatal(err)
	}
	for i := range 4 {
		w.put("gone", fmt.Sprintf("only-here/%d", i), w.note(7, fmt.Sprintf("only on gone %d", i)))
		w.commit("gone", fmt.Sprint("gone ", i))
	}
	if err := w.r.DeleteBranch(ctx, alice, "gone"); err != nil {
		t.Fatal(err)
	}
	stray := "packs/ab/ab" + strings.Repeat("0", 62)
	if err := w.blobs.Put(ctx, stray, strings.NewReader("never published"), 15); err != nil {
		t.Fatal(err)
	}
	packs := count(t, w.blobs, "packs/")

	first, err := w.gc()
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if first.Condemned == 0 || len(first.Deleted) != 0 || first.Live == 0 {
		t.Fatalf("the first run condemned %d packs, deleted %v, marked %d; want some condemned, none deleted", first.Condemned, first.Deleted, first.Live)
	}
	if n := count(t, w.blobs, "packs/"); n != packs {
		t.Fatalf("the first run left %d packs of %d: it deleted before the grace window", n, packs)
	}
	want := w.readable()

	w.jump = grace + time.Minute
	second, err := w.gc()
	if err != nil {
		t.Fatalf("GC a grace window later: %v", err)
	}
	deleted := map[string]bool{}
	for _, n := range second.Deleted {
		deleted[n] = true
	}
	if !deleted[stray] || len(second.Deleted) < first.Condemned {
		t.Fatalf("a grace window later GC deleted %d objects (the stray one: %v), want the %d condemned packs and the stray object", len(second.Deleted), deleted[stray], first.Condemned)
	}
	got := w.readable()
	for c := range want {
		if !got[c] {
			t.Fatalf("after the deletions %q no longer reads", c)
		}
	}
	for i := range 4 {
		if got[fmt.Sprintf("only on gone %d", i)] {
			t.Fatal("the deleted branch's objects still read through the refs")
		}
	}
	if _, err := w.store().Get(ctx, hash.Sum([]byte("only on gone 0"))); !errors.Is(err, chunk.ErrNotFound) {
		t.Fatalf("an object only the deleted branch held reads as %v, want ErrNotFound", err)
	}

	w.jump = 2*grace + 2*time.Minute
	if _, err := w.gc(); err != nil {
		t.Fatalf("GC two grace windows later: %v", err)
	}
	got = w.readable()
	for c := range want {
		if !got[c] {
			t.Fatalf("after the replaced index objects went, %q no longer reads", c)
		}
	}
}

func (w *world) commitOf(branch string) hash.Hash {
	w.t.Helper()
	c, err := w.r.Head(ctx, alice, branch)
	if err != nil {
		w.t.Fatal(err)
	}
	return c.Hash
}

// GC fails closed: a repository holding an object whose model cannot walk
// is refused (ErrNotWalkable), and nothing is condemned or deleted, however
// long it waits.
func TestGCRefusesWhatItCannotWalk(t *testing.T) {
	w := newWorld(t)
	w.put(vcs.MainBranch, "keep", w.note(7, "walks"))
	w.commit(vcs.MainBranch, "walkable")
	w.put(vcs.MainBranch, "mute", w.note(8, "cannot walk"))
	w.commit(vcs.MainBranch, "not walkable")
	packs := count(t, w.blobs, "packs/")
	for _, jump := range []time.Duration{0, grace + time.Minute, 2*grace + 2*time.Minute} {
		w.jump = jump
		if _, err := w.gc(); !errors.Is(err, object.ErrNotWalkable) {
			t.Fatalf("GC of a repository holding an object that cannot walk = %v, want ErrNotWalkable", err)
		}
	}
	if n := count(t, w.blobs, "packs/"); n != packs {
		t.Fatalf("the refused runs left %d packs of %d", n, packs)
	}
}

// onFirstRead runs f once, before the first pack read GC's marking makes.
type onFirstRead struct {
	blob.BlobStore
	once sync.Once
	f    func()
}

func (o *onFirstRead) Get(ctx context.Context, name string, off, n int64) (io.ReadCloser, error) {
	if strings.HasPrefix(name, "packs/") {
		o.once.Do(o.f)
	}
	return o.BlobStore.Get(ctx, name, off, n)
}

// A writer that publishes while GC marks makes that round's decision
// stale: GC takes another round, and what the writer published survives.
// (A round that would decide nothing has nothing to lose to a writer, so
// the fixture leaves a pack that is all garbage: a draft overwritten
// before it was committed.)
func TestGCTakesAnotherRoundWhenAWriterPublishes(t *testing.T) {
	w := newWorld(t)
	w.put(vcs.MainBranch, "a", w.note(7, "a draft"))
	w.put(vcs.MainBranch, "a", w.note(7, "before"))
	w.commit(vcs.MainBranch, "before")
	raw := w.blobs
	hooked := &onFirstRead{BlobStore: raw, f: func() {
		w.put(vcs.MainBranch, "b", w.note(7, "published during the mark"))
		w.commit(vcs.MainBranch, "during")
	}}
	report, err := gc.Run(ctx, gc.Options{Blobs: hooked, Keys: w.keys, Repo: repo, Config: prolly.DefaultConfig(),
		Registry: w.reg, Grace: grace, Clock: w.now})
	if err != nil {
		t.Fatal(err)
	}
	if report.Rounds != 2 {
		t.Fatalf("GC took %d rounds with a writer publishing during the first, want 2", report.Rounds)
	}
	w.jump = grace + time.Minute
	if _, err := w.gc(); err != nil {
		t.Fatal(err)
	}
	if got := w.readable(); !got["published during the mark"] || !got["before"] {
		t.Fatalf("after GC the repository reads %v, want both notes", got)
	}
}
