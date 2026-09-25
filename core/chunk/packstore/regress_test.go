package packstore_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/mem"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/packstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
)

// rangeOverrun is a backend that answers a ranged pack read with the range
// and then extra more bytes, as an endpoint that ignores Range streams the
// rest of the object; it counts the bytes a reader pulled past the range.
type rangeOverrun struct {
	blob.BlobStore
	extra  int64
	over   atomic.Int64 // most bytes pulled past the range, any one read
	ranged atomic.Int64 // ranged pack reads answered
}

func (b *rangeOverrun) Get(ctx context.Context, name string, off, n int64) (io.ReadCloser, error) {
	rc, err := b.BlobStore.Get(ctx, name, off, n)
	if err != nil || !strings.HasPrefix(name, "packs/") || n < 0 {
		return rc, err
	}
	b.ranged.Add(1)
	return &overrunBody{rc: rc, rest: io.LimitReader(zeros{}, b.extra), n: n, b: b}, nil
}

type overrunBody struct {
	rc   io.ReadCloser
	rest io.Reader
	n    int64 // the range asked for
	read int64
	b    *rangeOverrun
}

func (o *overrunBody) Read(p []byte) (int, error) {
	k, err := o.rc.Read(p)
	if err == io.EOF {
		k, err = o.rest.Read(p[k:])
	}
	o.read += int64(k)
	if past := o.read - o.n; past > o.b.over.Load() {
		o.b.over.Store(past)
	}
	return k, err
}

func (o *overrunBody) Close() error { return o.rc.Close() }

type zeros struct{}

func (zeros) Read(p []byte) (int, error) { clear(p); return len(p), nil }

// #26: a chunk read took whatever the backend sent for its frame's range
// (io.ReadAll, no limit); blob/s3 hands back the response body as is, so an
// endpoint that ignores Range could stream a whole pack, up to a gibibyte,
// into memory for one chunk. The read must stop one byte past the range.
func TestRegression_SC26_AChunkReadStopsAtItsRange(t *testing.T) {
	inner, kr := mem.New(), keyring(t)
	w := open(t, inner, kr)
	data := payload("ranged", 4000)
	h, err := w.Put(ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.CompareAndSetRoot(ctx, hash.Hash{}, h); err != nil {
		t.Fatal(err)
	}
	read := func(extra int64) (*rangeOverrun, []byte, error) {
		bs := &rangeOverrun{BlobStore: inner, extra: extra}
		s, err := packstore.Open(ctx, packstore.Options{Blobs: bs, Keys: kr, Repo: repo})
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		got, err := s.Get(ctx, h)
		return bs, got, err
	}

	// Positive control: an honest backend, the range exactly.
	bs, got, err := read(0)
	if err != nil || !bytes.Equal(got, data) || bs.ranged.Load() != 1 || bs.over.Load() != 0 {
		t.Fatalf("positive control: an honest backend's chunk read = %d bytes (want %d), %v, after %d ranged reads "+
			"(want 1) and %d bytes past the range", len(got), len(data), err, bs.ranged.Load(), bs.over.Load())
	}

	const extra = 1 << 20
	bs, got, err = read(extra)
	if over := bs.over.Load(); over > 1 {
		t.Errorf("a backend that sent %d bytes past a frame's range had %d of them read (limit 1): an endpoint "+
			"that ignores Range streams a whole pack, up to a gibibyte, into memory for one chunk", extra, over)
	}
	if err == nil {
		t.Errorf("a chunk read whose backend sent more than the range asked returned %d bytes and no error", len(got))
	}
}

// #26: a backend that sends more than the range asked for is at fault, not
// the pack: the read is refused as the backend's error, never as
// chunk.ErrCorrupt, which tells an operator the stored data is damaged and
// sends them to restore it. A backend that sends one byte fewer than the
// range is the corrupt case and the positive control of the classification.
func TestRegression_SC26_AnOverrunningBackendIsNotReportedAsCorruptData(t *testing.T) {
	inner, kr := mem.New(), keyring(t)
	w := open(t, inner, kr)
	h, err := w.Put(ctx, payload("overrun", 4000))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.CompareAndSetRoot(ctx, hash.Hash{}, h); err != nil {
		t.Fatal(err)
	}
	read := func(bs blob.BlobStore) error {
		s, err := packstore.Open(ctx, packstore.Options{Blobs: bs, Keys: kr, Repo: repo})
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		_, err = s.Get(ctx, h)
		return err
	}
	if err := read(&rangeShort{BlobStore: inner}); !errors.Is(err, chunk.ErrCorrupt) {
		t.Fatalf("positive control: a frame a byte short of its range read as %v, want ErrCorrupt", err)
	}
	if err := read(&rangeOverrun{BlobStore: inner, extra: 1}); err == nil || errors.Is(err, chunk.ErrCorrupt) {
		t.Errorf("a backend that sent a byte past a frame's range: %v; want the backend's error, not ErrCorrupt "+
			"(the pack is intact; an operator told it is corrupt restores data that is not damaged)", err)
	}
}

// rangeShort answers a ranged pack read with one byte fewer than asked.
type rangeShort struct{ blob.BlobStore }

func (b *rangeShort) Get(ctx context.Context, name string, off, n int64) (io.ReadCloser, error) {
	rc, err := b.BlobStore.Get(ctx, name, off, n)
	if err != nil || !strings.HasPrefix(name, "packs/") || n < 1 {
		return rc, err
	}
	return struct {
		io.Reader
		io.Closer
	}{io.LimitReader(rc, n-1), rc}, nil
}

// Found building #34: a publish waited for the finishers, then took the
// store's lock; a put from another goroutine that filled the pending pack
// in between handed it, and the chunks the new root reaches, to a
// finisher the publish did not wait for. The publish then wrote an index
// object without that pack and swapped a root whose chunk no index object
// lists and, until the finisher's upload, nothing durable holds. The
// finisher is held (up to a second, so a publish that waits for it still
// ends) until the publish has written its index object.
func TestRegression_SC34_APublishWaitsForAFinisherStartedAfterItsWait(t *testing.T) {
	raw, kr := mem.New(), keyring(t)
	indexed := make(chan struct{})
	var once atomic.Bool
	bs := &onIndexPut{BlobStore: raw, put: func() {
		if once.CompareAndSwap(false, true) {
			close(indexed)
		}
	}}
	s, err := packstore.Open(ctx, packstore.WithBackoff(packstore.Options{Blobs: bs, Keys: kr, Repo: repo, PackSize: 64 << 10}, time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	root, err := s.Put(ctx, payload("the root", 1000))
	if err != nil {
		t.Fatal(err)
	}
	packstore.HoldFinish(s, func() {
		select {
		case <-indexed:
		case <-time.After(time.Second):
		}
	})
	packstore.AfterWait(s, func() {
		packstore.AfterWait(s, nil)
		// Another writer fills the pending pack, the root's chunk in it.
		if _, err := s.Put(ctx, payload("filler", 64<<10)); err != nil {
			t.Error(err)
		}
	})
	if err := s.CompareAndSetRoot(ctx, hash.Hash{}, root); err != nil {
		t.Fatalf("CompareAndSetRoot: %v", err)
	}
	packstore.WaitUploads(s)
	fresh := open(t, raw, kr)
	if got, err := fresh.Root(ctx); err != nil || got != root {
		t.Fatalf("the published root is %s (%v), want %s", got.Short(), err, root.Short())
	}
	if _, err := fresh.Get(ctx, root); err != nil {
		t.Fatalf("another process cannot read the chunk the published root names (%v): the publish swapped a root whose pack no index object lists", err)
	}
}

// onIndexPut calls put after every index object it stores.
type onIndexPut struct {
	blob.BlobStore
	put func()
}

func (b *onIndexPut) Put(ctx context.Context, name string, r io.Reader, size int64) error {
	err := b.BlobStore.Put(ctx, name, r, size)
	if strings.HasPrefix(name, "index/") {
		b.put()
	}
	return err
}

// Found by TestGCSafetyPropertyWithTheJournal (#34), and not the journal's
// alone: a put counted on a chunk in a pack; a GC round then repacked that
// pack without it (nothing published reached it yet); the publish found it
// still stored (the repacked pack stays listed for a grace window) and
// named it; a grace window on, the repacked pack expired, as a repacked
// pack is never reprieved by the chunks it still holds, and the chunk the
// published root reaches was deleted.
func TestRegression_SC34_ACountedChunkOnlyARepackedPackHoldsSurvivesItsExpiry(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	s0 := open(t, bs, kr)
	y, x := []byte("y, live"), payload("x, dead when the round runs", 6000)
	hy, err := s0.Put(ctx, y)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s0.Put(ctx, x); err != nil {
		t.Fatal(err)
	}
	if err := s0.CompareAndSetRoot(ctx, hash.Hash{}, hy); err != nil {
		t.Fatal(err)
	}
	s := open(t, bs, kr)
	hx, err := s.Put(ctx, x) // counted on: the pack holding y and x
	if err != nil {
		t.Fatal(err)
	}
	out := repackRound(t, bs, kr, liveSet(hy), t0, packstore.Repack{})
	if out.Repacked != 1 {
		t.Fatalf("fixture: the round repacked %v, want the pack holding x", out.Repacked)
	}
	if err := s.CompareAndSetRoot(ctx, hy, hx); err != nil {
		t.Fatalf("publishing a root that reaches x: %v", err)
	}
	out = repackRound(t, bs, kr, liveSet(hx), t0.Add(2*time.Hour), packstore.Repack{Off: true})
	remove(t, bs, out.Expired)
	fresh := open(t, bs, kr)
	if got, err := fresh.Get(ctx, hx); err != nil || !bytes.Equal(got, x) {
		t.Fatalf("after the repacked pack expired, the chunk the published root names does not read (%v): GC deleted a reachable chunk", err)
	}
}
