package gc_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/gc"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
	"github.com/SmithOperatingSolutions/snapshot-core/core/vcs"
)

var errInjected = errors.New("injected: the backend failed")

// The blob calls failing can fail.
var blobCalls = []string{"root", "get", "put", "stat", "list", "delete", "swap"}

// failing fails exactly one call, the at-th of one kind (at 0: none).
type failing struct {
	blob.BlobStore
	kind  string
	at    int
	calls map[string]int
}

func (f *failing) fails(kind string) bool {
	f.calls[kind]++
	return kind == f.kind && f.calls[kind] == f.at
}

func (f *failing) Root(ctx context.Context) (blob.Root, error) {
	if f.fails("root") {
		return blob.Root{}, errInjected
	}
	return f.BlobStore.Root(ctx)
}

func (f *failing) Get(ctx context.Context, name string, off, n int64) (io.ReadCloser, error) {
	if f.fails("get") {
		return nil, errInjected
	}
	return f.BlobStore.Get(ctx, name, off, n)
}

func (f *failing) Put(ctx context.Context, name string, r io.Reader, size int64) error {
	if f.fails("put") {
		return errInjected
	}
	return f.BlobStore.Put(ctx, name, r, size)
}

func (f *failing) Stat(ctx context.Context, name string) (blob.Info, error) {
	if f.fails("stat") {
		return blob.Info{}, errInjected
	}
	return f.BlobStore.Stat(ctx, name)
}

func (f *failing) List(ctx context.Context, prefix, after string, limit int) ([]blob.Info, error) {
	if f.fails("list") {
		return nil, errInjected
	}
	return f.BlobStore.List(ctx, prefix, after, limit)
}

func (f *failing) Delete(ctx context.Context, name string) error {
	if f.fails("delete") {
		return errInjected
	}
	return f.BlobStore.Delete(ctx, name)
}

func (f *failing) SwapRoot(ctx context.Context, expected blob.Version, next []byte) (blob.Version, error) {
	if f.fails("swap") {
		return blob.NoVersion, errInjected
	}
	return f.BlobStore.SwapRoot(ctx, expected, next)
}

// ripe is a world a grace window past its first collection: the next run
// expires, compacts and deletes.
func ripe(t *testing.T) *world {
	t.Helper()
	w := newWorld(t)
	w.put(vcs.MainBranch, "a", w.note(7, "a draft"))
	w.put(vcs.MainBranch, "a", w.note(7, "kept"))
	w.commit(vcs.MainBranch, "kept")
	if rep, err := w.gc(); err != nil || rep.Condemned == 0 {
		t.Fatalf("fixture: the first run condemned %d (%v)", rep.Condemned, err)
	}
	w.jump = grace + time.Minute
	return w
}

func (w *world) gcOn(bs blob.BlobStore) (gc.Report, error) {
	return gc.Run(ctx, gc.Options{Blobs: bs, Keys: w.keys, Repo: repo, Config: prolly.DefaultConfig(),
		Registry: w.reg, Grace: grace, Clock: w.now})
}

// A failed backend call is never a smaller repository: every blob call a
// collection makes (reading the manifest and index objects, marking,
// swapping, probing the backend's clock, listing, deleting), failed in
// turn, is GC's error; and the next run, the backend well again, finishes
// the job and the whole repository reads.
func TestEveryStoreFailureIsGCsError(t *testing.T) {
	w0 := ripe(t)
	counter := &failing{BlobStore: w0.blobs, calls: map[string]int{}}
	if _, err := w0.gcOn(counter); err != nil {
		t.Fatal(err)
	}
	failures := 0
	for _, kind := range blobCalls {
		for at := 1; at <= counter.calls[kind]; at++ {
			w := ripe(t)
			f := &failing{BlobStore: w.blobs, kind: kind, at: at, calls: map[string]int{}}
			if _, err := w.gcOn(f); !errors.Is(err, errInjected) {
				t.Fatalf("with %s %d of %d failing, GC = %v, want the backend's error", kind, at, counter.calls[kind], err)
			}
			if _, err := w.gc(); err != nil {
				t.Fatalf("after %s %d failed, the next run: %v", kind, at, err)
			}
			if !w.readable()["kept"] {
				t.Fatalf("after %s %d failed and a clean run, the repository does not read", kind, at)
			}
			failures++
		}
	}
	if failures < 20 {
		t.Fatalf("fixture: a collection made %d blob calls, want many", failures)
	}
}

// GC refuses options it cannot use: no store, key or registry, a negative
// grace window.
func TestGCRefusesOptionsItCannotUse(t *testing.T) {
	w := newWorld(t)
	good := gc.Options{Blobs: w.blobs, Keys: w.keys, Repo: repo, Config: prolly.DefaultConfig(), Registry: w.reg, Grace: grace, Clock: w.now}
	for name, set := range map[string]func(o *gc.Options){
		"no store":         func(o *gc.Options) { o.Blobs = nil },
		"no key":           func(o *gc.Options) { o.Keys = nil },
		"no registry":      func(o *gc.Options) { o.Registry = nil },
		"a negative grace": func(o *gc.Options) { o.Grace = -time.Second },
	} {
		o := good
		set(&o)
		if _, err := gc.Run(ctx, o); err == nil {
			t.Errorf("GC with %s succeeded", name)
		}
	}
	if _, err := gc.Run(ctx, good); err != nil {
		t.Fatalf("positive control: %v", err)
	}
}

// A zero grace window is the spec's seven days, and a nil clock is the wall
// clock: condemnations it dates are today's, so a run a minute later
// expires none of them, and one seven days on expires them all.
func TestGCDefaultsToSevenDaysAndTheWallClock(t *testing.T) {
	w := newWorld(t)
	w.put(vcs.MainBranch, "a", w.note(7, "a draft"))
	w.put(vcs.MainBranch, "a", w.note(7, "kept"))
	w.commit(vcs.MainBranch, "kept")
	o := gc.Options{Blobs: w.blobs, Keys: w.keys, Repo: repo, Config: prolly.DefaultConfig(), Registry: w.reg}
	if rep, err := gc.Run(ctx, o); err != nil || rep.Condemned == 0 {
		t.Fatalf("fixture: GC on the wall clock condemned %d (%v), want some", rep.Condemned, err)
	}
	o.Clock = w.now
	for _, c := range []struct {
		jump   time.Duration
		expire bool
	}{{time.Minute, false}, {7*24*time.Hour - time.Minute, false}, {7*24*time.Hour + time.Minute, true}} {
		w.jump = c.jump
		rep, err := gc.Run(ctx, o)
		if err != nil {
			t.Fatal(err)
		}
		if got := len(rep.Deleted) > 0; got != c.expire {
			t.Fatalf("%v after a wall-clock run, GC deleted %v; want deletions: %v", c.jump, rep.Deleted, c.expire)
		}
	}
}

// A store holding no repository has nothing live: GC deletes what no
// manifest names once it is older than the window, every page of it.
func TestGCOnAStoreWithNoRepository(t *testing.T) {
	w := newWorld(t)
	bare := &clocked{BlobStore: memBlobs(), stamps: map[string]time.Time{}, now: w.now}
	for i := range blob.MaxListPage + 1 {
		name := fmt.Sprintf("packs/%02x/%064x", i%256, i)
		if err := bare.Put(ctx, name, strings.NewReader("stray"), 5); err != nil {
			t.Fatal(err)
		}
	}
	if rep, err := w.gcOn(bare); err != nil || len(rep.Deleted) != 0 {
		t.Fatalf("GC deleted %d strays younger than the window (%v)", len(rep.Deleted), err)
	}
	w.jump = grace + time.Minute
	rep, err := w.gcOn(bare)
	if err != nil || len(rep.Deleted) != blob.MaxListPage+1 {
		t.Fatalf("GC deleted %d of %d strays past the window (%v)", len(rep.Deleted), blob.MaxListPage+1, err)
	}
}

// everyRead publishes a commit before every pack read GC makes: writers
// that never stop.
type everyRead struct {
	blob.BlobStore
	f func()
}

func (e *everyRead) Get(ctx context.Context, name string, off, n int64) (io.ReadCloser, error) {
	if strings.HasPrefix(name, "packs/") && e.f != nil {
		f := e.f
		e.f = nil
		f()
		defer func() { e.f = f }()
	}
	return e.BlobStore.Get(ctx, name, off, n)
}

// GC gives up, deciding nothing, when writers publish during every round.
func TestGCGivesUpWhenWritersNeverStop(t *testing.T) {
	w := newWorld(t)
	w.put(vcs.MainBranch, "a", w.note(7, "a draft"))
	w.put(vcs.MainBranch, "a", w.note(7, "kept"))
	w.commit(vcs.MainBranch, "kept")
	i := 0
	busy := &everyRead{BlobStore: w.blobs, f: func() {
		i++
		w.put(vcs.MainBranch, fmt.Sprint("n", i), w.note(7, fmt.Sprint("note ", i)))
	}}
	rep, err := w.gcOn(busy)
	if err == nil || rep.Rounds < 10 || rep.Condemned != 0 || len(rep.Deleted) != 0 {
		t.Fatalf("with a writer publishing during every round GC took %d rounds, condemned %d, deleted %d (%v); want it to give up, deciding nothing", rep.Rounds, rep.Condemned, len(rep.Deleted), err)
	}
}
