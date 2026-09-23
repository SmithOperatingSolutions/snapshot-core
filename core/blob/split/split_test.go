package split_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/contract"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/mem"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/split"
)

var ctx = context.Background()

// recording is an objects store that records every copy of the root written
// to it, can hold each write until released, and can fail them.
type recording struct {
	*mem.Store
	mu       sync.Mutex
	writes   [][]byte
	hold     chan struct{} // WriteMirror blocks on it while set
	entered  chan struct{} // closed when a WriteMirror first blocks on hold
	failing  bool
	failOnce bool
}

func (r *recording) WriteMirror(ctx context.Context, value []byte) error {
	r.mu.Lock()
	hold, failing := r.hold, r.failing || r.failOnce
	r.failOnce = false
	r.mu.Unlock()
	if hold != nil {
		r.mu.Lock()
		if r.entered != nil {
			close(r.entered)
			r.entered = nil
		}
		r.mu.Unlock()
		<-hold
	}
	if failing {
		return errors.New("injected: the objects store is unreachable")
	}
	r.mu.Lock()
	r.writes = append(r.writes, bytes.Clone(value))
	r.mu.Unlock()
	return r.Store.WriteMirror(ctx, value)
}

func (r *recording) written() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, w := range r.writes {
		out = append(out, string(w))
	}
	return out
}

func (r *recording) set(f func(r *recording)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	f(r)
}

func newSplit(t *testing.T, objects blob.BlobStore, m split.Mirror) (*split.Store, *mem.Store) {
	t.Helper()
	roots := mem.New()
	s, err := split.New(split.Options{Objects: objects, Roots: roots, Mirror: m})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, roots
}

func swap(t *testing.T, s blob.BlobStore, value string) {
	t.Helper()
	r, err := s.Root(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.SwapRoot(ctx, r.Version, []byte(value)); err != nil {
		t.Fatalf("SwapRoot(%q) = %v", value, err)
	}
}

func mirrorOf(t *testing.T, m split.Mirrorer) string {
	t.Helper()
	b, err := m.ReadMirror(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// The blob contract holds over a split store whose two halves are whole
// stores: objects and the root each keep their store's promises.
func TestContractOverASplitStore(t *testing.T) {
	contract.Run(t, func(t *testing.T) blob.BlobStore {
		s, _ := newSplit(t, mem.New(), split.Mirror{Mode: split.MirrorOff})
		return s
	}, contract.Options{})
}

// Objects go to the objects store and nowhere else; the root goes to the
// root store and, as a copy, to the objects store.
func TestObjectsGoToOneStoreAndTheRootToTheOther(t *testing.T) {
	objects := &recording{Store: mem.New()}
	s, roots := newSplit(t, objects, split.Mirror{})
	if err := s.Put(ctx, "packs/a", strings.NewReader("a pack"), 6); err != nil {
		t.Fatal(err)
	}
	if _, err := objects.Stat(ctx, "packs/a"); err != nil {
		t.Fatalf("the object is not on the objects store: %v", err)
	}
	if _, err := roots.Stat(ctx, "packs/a"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("the object is on the root store too: %v", err)
	}
	swap(t, s, "root one")
	if r, err := roots.Root(ctx); err != nil || string(r.Value) != "root one" {
		t.Fatalf("the root store holds %q (%v), want the root", r.Value, err)
	}
	if r, err := objects.Root(ctx); err != nil || r.Version != blob.NoVersion {
		t.Fatalf("the objects store holds a root %q (%v); the root must live on the root store alone", r.Value, err)
	}
	if got := mirrorOf(t, objects); got != "root one" {
		t.Fatalf("the objects store's copy of the root is %q, want %q", got, "root one")
	}
}

// MirrorWait, the default: a swap returns once the copy has landed, so the
// copy equals the root after every swap; a copy that fails once is retried
// within the call and lands; one that keeps failing leaves the swap
// reported all the same (it happened) and the status saying the copy trails
// and why, until a later swap's copy lands.
func TestWaitMirrorsEachSwapBeforeReturning(t *testing.T) {
	objects := &recording{Store: mem.New()}
	s, roots := newSplit(t, objects, split.Mirror{})
	for _, v := range []string{"root one", "root two"} {
		swap(t, s, v)
		if got := mirrorOf(t, objects); got != v {
			t.Fatalf("after the swap to %q returned, the copy is %q", v, got)
		}
	}
	if st := s.MirrorStatus(); !st.Mirrored || st.LastError != nil || st.LastWrite.IsZero() {
		t.Fatalf("after copies landed the status is %+v; want mirrored, no error, a time", st)
	}
	objects.set(func(r *recording) { r.failOnce = true })
	swap(t, s, "root three")
	if got, st := mirrorOf(t, objects), s.MirrorStatus(); got != "root three" || !st.Mirrored || st.LastError != nil {
		t.Fatalf("after a copy failed once, the copy is %q and the status %+v; want the retry to have landed %q", got, st, "root three")
	}
	objects.set(func(r *recording) { r.failing = true })
	swap(t, s, "root four")
	if r, err := roots.Root(ctx); err != nil || string(r.Value) != "root four" {
		t.Fatalf("a swap whose copy failed did not land on the root store: %q (%v)", r.Value, err)
	}
	if st := s.MirrorStatus(); st.Mirrored || st.LastError == nil {
		t.Fatalf("with the objects store unreachable the status is %+v; want not mirrored, and the error", st)
	}
	if got := mirrorOf(t, objects); got != "root three" {
		t.Fatalf("the copy is %q, want the last one that landed, %q", got, "root three")
	}
	objects.set(func(r *recording) { r.failing = false })
	swap(t, s, "root five")
	if got, st := mirrorOf(t, objects), s.MirrorStatus(); got != "root five" || !st.Mirrored || st.LastError != nil {
		t.Fatalf("once the objects store is back, the copy is %q and the status %+v; want %q, mirrored, no error", got, st, "root five")
	}
}

// MirrorBackground: swaps do not wait for the copy; one writer uploads the
// newest root and skips any it was superseded on, so the copy never goes
// backwards; Close flushes it. The writer is held inside its first copy
// while two more swaps land, so exactly the first and the newest root are
// written.
func TestBackgroundMirrorsTheNewestRootAndCloseFlushes(t *testing.T) {
	objects := &recording{Store: mem.New(), hold: make(chan struct{}), entered: make(chan struct{})}
	entered := objects.entered
	s, _ := newSplit(t, objects, split.Mirror{Mode: split.MirrorBackground})
	swap(t, s, "root 1")
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the writer never began copying the first root")
	}
	start := time.Now()
	swap(t, s, "root 2")
	swap(t, s, "root 3")
	if d := time.Since(start); d > 200*time.Millisecond {
		t.Fatalf("two swaps took %v with the copy held: swaps must not wait for the copy", d)
	}
	if st := s.MirrorStatus(); st.Mirrored {
		t.Fatal("the status says mirrored while the copy is held")
	}
	close(objects.hold)
	if err := s.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
	if writes := objects.written(); strings.Join(writes, ", ") != "root 1, root 3" {
		t.Fatalf("the copies written were %q; want the held first root and then the newest alone", writes)
	}
	if got := mirrorOf(t, objects); got != "root 3" {
		t.Fatalf("after Close the copy is %q, want %q", got, "root 3")
	}
	if st := s.MirrorStatus(); !st.Mirrored {
		t.Fatalf("after Close the status is %+v, want mirrored", st)
	}
}

// MirrorBackground copies each swap on its own, no Close needed: once the
// first copy has landed the writer is idle, and the second swap must wake
// it.
func TestBackgroundCopiesEachSwap(t *testing.T) {
	objects := &recording{Store: mem.New()}
	s, _ := newSplit(t, objects, split.Mirror{Mode: split.MirrorBackground})
	for _, v := range []string{"root 1", "root 2"} {
		swap(t, s, v)
		deadline := time.Now().Add(5 * time.Second)
		for !s.MirrorStatus().Mirrored {
			if time.Now().After(deadline) {
				t.Fatalf("the copy of %q never landed; written %q", v, objects.written())
			}
			time.Sleep(time.Millisecond)
		}
		if got := mirrorOf(t, objects); got != v {
			t.Fatalf("after the swap to %q the copy is %q", v, got)
		}
	}
}

// A background copy that fails is reported and retried until one lands;
// Close retries it too.
func TestBackgroundReportsAndRetriesAFailedCopy(t *testing.T) {
	objects := &recording{Store: mem.New(), failing: true}
	s, _ := newSplit(t, objects, split.Mirror{Mode: split.MirrorBackground})
	swap(t, s, "root one")
	deadline := time.Now().Add(5 * time.Second)
	for s.MirrorStatus().LastError == nil {
		if time.Now().After(deadline) {
			t.Fatal("the status never reported the failing copy")
		}
		time.Sleep(time.Millisecond)
	}
	if st := s.MirrorStatus(); st.Mirrored {
		t.Fatalf("the status is %+v while every copy fails; want not mirrored", st)
	}
	objects.set(func(r *recording) { r.failing = false })
	if err := s.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
	if got, st := mirrorOf(t, objects), s.MirrorStatus(); got != "root one" || !st.Mirrored || st.LastError != nil {
		t.Fatalf("after Close the copy is %q and the status %+v; want %q, mirrored, no error", got, st, "root one")
	}
}

// MirrorPeriodic: the newest root goes up every period if it changed, and
// not otherwise.
func TestPeriodicMirrorsOnItsClock(t *testing.T) {
	objects := &recording{Store: mem.New()}
	s, _ := newSplit(t, objects, split.Mirror{Mode: split.MirrorPeriodic, Every: 20 * time.Millisecond})
	swap(t, s, "root one")
	swap(t, s, "root two")
	deadline := time.Now().Add(5 * time.Second)
	for !s.MirrorStatus().Mirrored {
		if time.Now().After(deadline) {
			t.Fatalf("the copy never landed; written %q", objects.written())
		}
		time.Sleep(time.Millisecond)
	}
	if got := mirrorOf(t, objects); got != "root two" {
		t.Fatalf("the copy is %q, want the newest root %q", got, "root two")
	}
	n := len(objects.written())
	time.Sleep(100 * time.Millisecond)
	if again := len(objects.written()); again != n {
		t.Fatalf("with the root unchanged, %d more copies were written", again-n)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// MirrorOff writes no copy, and needs no Mirrorer of the objects store.
func TestOffKeepsNoCopy(t *testing.T) {
	objects := &recording{Store: mem.New()}
	s, _ := newSplit(t, objects, split.Mirror{Mode: split.MirrorOff})
	swap(t, s, "root one")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if w := objects.written(); len(w) != 0 {
		t.Fatalf("off, the store wrote copies %q", w)
	}
	if _, err := split.New(split.Options{Objects: plain{mem.New()}, Roots: mem.New(), Mirror: split.Mirror{Mode: split.MirrorOff}}); err != nil {
		t.Fatalf("off, an objects store that cannot hold a copy is refused: %v", err)
	}
}

// plain is an objects store that cannot hold the root's copy.
type plain struct{ blob.BlobStore }

// New refuses what it cannot run on: a missing store, a copy the objects
// store cannot hold, a period without a length.
func TestNewRefusesBadOptions(t *testing.T) {
	good := split.Options{Objects: mem.New(), Roots: mem.New()}
	if s, err := split.New(good); err != nil {
		t.Fatalf("positive control: %v", err)
	} else {
		s.Close()
	}
	for name, o := range map[string]split.Options{
		"no objects store":                     {Roots: mem.New()},
		"no root store":                        {Objects: mem.New()},
		"a copy the objects store cannot hold": {Objects: plain{mem.New()}, Roots: mem.New()},
		"a period of nothing":                  {Objects: mem.New(), Roots: mem.New(), Mirror: split.Mirror{Mode: split.MirrorPeriodic}},
		"a mode that is none":                  {Objects: mem.New(), Roots: mem.New(), Mirror: split.Mirror{Mode: 9}},
	} {
		if s, err := split.New(o); !errors.Is(err, split.ErrOptions) {
			if s != nil {
				s.Close()
			}
			t.Errorf("%s: New = %v, want ErrOptions", name, err)
		}
	}
}

// Recover seeds a root store that has no root from the copy: the recovered
// store swaps on from where the copy was. A root store that has a root, and
// an objects store with no copy, are refused.
func TestRecoverSeedsAnEmptyRootStoreFromTheCopy(t *testing.T) {
	objects := &recording{Store: mem.New()}
	s, _ := newSplit(t, objects, split.Mirror{})
	swap(t, s, "root one")
	swap(t, s, "root two")
	fresh := mem.New()
	if err := split.Recover(ctx, objects, fresh); err != nil {
		t.Fatalf("Recover = %v", err)
	}
	if r, err := fresh.Root(ctx); err != nil || string(r.Value) != "root two" {
		t.Fatalf("the recovered root store holds %q (%v), want the copy %q", r.Value, err, "root two")
	}
	again, err := split.New(split.Options{Objects: objects, Roots: fresh})
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	swap(t, again, "root three")
	if got := mirrorOf(t, objects); got != "root three" {
		t.Fatalf("after recovery the copy is %q, want %q", got, "root three")
	}
	if err := split.Recover(ctx, objects, fresh); !errors.Is(err, split.ErrRootPresent) {
		t.Fatalf("Recover onto a root store with a root = %v, want ErrRootPresent", err)
	}
	if err := split.Recover(ctx, &recording{Store: mem.New()}, mem.New()); !errors.Is(err, split.ErrNoMirror) {
		t.Fatalf("Recover from an objects store with no copy = %v, want ErrNoMirror", err)
	}
}

// An objects store's own errors come through unchanged.
func TestObjectErrorsComeThrough(t *testing.T) {
	s, _ := newSplit(t, mem.New(), split.Mirror{Mode: split.MirrorOff})
	if _, err := s.Get(ctx, "packs/none", 0, -1); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("Get of a missing object = %v, want ErrNotFound", err)
	}
	if err := s.Put(ctx, "packs/a", strings.NewReader("a"), 1); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, "packs/a", strings.NewReader("a"), 1); !errors.Is(err, blob.ErrExists) {
		t.Fatalf("a second put of one name = %v, want ErrExists", err)
	}
	rc, err := s.Get(ctx, "packs/a", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(rc)
	rc.Close()
	if string(b) != "a" {
		t.Fatalf("the object reads %q", b)
	}
}
