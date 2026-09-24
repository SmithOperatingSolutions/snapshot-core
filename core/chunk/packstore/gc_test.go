package packstore_test

import (
	"bytes"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/mem"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/packstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
)

var t0 = time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)

// published puts each chunk in its own session and publishes it, so each
// sits in a pack of its own; the root is the first.
func published(t *testing.T, s *packstore.Store, chunks ...string) []hash.Hash {
	t.Helper()
	var hs []hash.Hash
	root := hash.Hash{}
	for _, c := range chunks {
		h, err := s.Put(ctx, []byte(c))
		if err != nil {
			t.Fatal(err)
		}
		hs = append(hs, h)
		if root.IsZero() {
			if err := s.CompareAndSetRoot(ctx, hash.Hash{}, h); err != nil {
				t.Fatal(err)
			}
			root = h
			continue
		}
		if err := s.CompareAndSetRoot(ctx, root, root); err != nil {
			t.Fatal(err)
		}
	}
	return hs
}

// liveSet is a live set in memory, for rounds decided against a few chunks.
func liveSet(hs ...hash.Hash) liveSetOf {
	set := map[hash.Hash]bool{}
	for _, h := range hs {
		set[h] = true
	}
	var out liveSetOf
	for h := range set {
		out = append(out, h)
	}
	slices.SortFunc(out, hash.Hash.Compare)
	return out
}

type liveSetOf []hash.Hash

func (l liveSetOf) Has(h hash.Hash) (bool, error) {
	_, ok := slices.BinarySearchFunc(l, h, hash.Hash.Compare)
	return ok, nil
}
func (l liveSetOf) Len() int64 { return int64(len(l)) }
func (l liveSetOf) Each(f func(hash.Hash) error) error {
	for _, h := range l {
		if err := f(h); err != nil {
			return err
		}
	}
	return nil
}

func round(t *testing.T, bs blob.BlobStore, kr *seal.Keyring, live packstore.Live, now time.Time) packstore.Outcome {
	t.Helper()
	r, err := packstore.Begin(ctx, packstore.Options{Blobs: bs, Keys: kr, Repo: repo})
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	r.Repack(packstore.Repack{Off: true}) // condemnation alone; repacking has tests of its own
	out, err := r.Apply(ctx, live, now, time.Hour)
	if err != nil {
		t.Fatalf("Apply at %v: %v", now.Sub(t0), err)
	}
	return out
}

func remove(t *testing.T, bs blob.BlobStore, names []string) {
	t.Helper()
	for _, n := range names {
		if err := bs.Delete(ctx, n); err != nil {
			t.Fatal(err)
		}
	}
}

// objects lists the names in the store under prefix.
func objects(t *testing.T, bs blob.BlobStore, prefix string) []string {
	t.Helper()
	infos, err := bs.List(ctx, prefix, "", blob.MaxListPage)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, i := range infos {
		out = append(out, i.Name)
	}
	return out
}

// DESIGN §9: a pack nothing reaches is condemned, not deleted; it expires
// only once condemned a grace window ago and still unreached, and then it
// leaves the index objects, which are rewritten, the replaced ones
// condemned in turn and expiring a grace window later. What is live reads
// throughout, and what expired is gone.
func TestARoundCondemnsWaitsThenExpires(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	hs := published(t, open(t, bs, kr), "a, the root", "b, garbage", "c, live")
	live := liveSet(hs[0], hs[2])
	packs := objects(t, bs, "packs/")
	indexes := objects(t, bs, "index/")
	if len(packs) != 3 || len(indexes) != 3 {
		t.Fatalf("fixture: %d packs and %d index objects, want 3 of each", len(packs), len(indexes))
	}

	out := round(t, bs, kr, live, t0)
	if out.Condemned != 1 || out.Reprieved != 0 || len(out.Expired) != 0 {
		t.Fatalf("the first round condemned %d, reprieved %d, expired %v; want 1, 0, none", out.Condemned, out.Reprieved, out.Expired)
	}
	for _, n := range append(packs, indexes...) {
		if !out.Named[n] {
			t.Fatalf("after the first round the manifest does not name %s, which nothing has expired", n)
		}
	}
	if out = round(t, bs, kr, live, t0.Add(time.Hour-time.Second)); out.Condemned != 0 || len(out.Expired) != 0 {
		t.Fatalf("a second before the grace window ends: condemned %d, expired %v; want nothing", out.Condemned, out.Expired)
	}

	out = round(t, bs, kr, live, t0.Add(time.Hour))
	if len(out.Expired) != 1 || !strings.HasPrefix(out.Expired[0], "packs/") {
		t.Fatalf("the grace window over, the round expired %v; want the one condemned pack", out.Expired)
	}
	gone := out.Expired[0]
	if out.Named[gone] {
		t.Fatal("the manifest still names the expired pack")
	}
	for _, n := range indexes {
		if !out.Named[n] {
			t.Fatalf("the replaced index object %s is neither kept nor condemned: a reader holding the old manifest would lose it", n)
		}
	}
	remove(t, bs, out.Expired)
	fresh := open(t, bs, kr)
	for _, h := range []hash.Hash{hs[0], hs[2]} {
		if _, err := fresh.Get(ctx, h); err != nil {
			t.Fatalf("a live chunk does not read after its neighbour's pack expired: %v", err)
		}
	}
	if _, err := fresh.Get(ctx, hs[1]); !errors.Is(err, chunk.ErrNotFound) {
		t.Fatalf("the expired chunk reads as %v, want ErrNotFound", err)
	}

	out = round(t, bs, kr, live, t0.Add(2*time.Hour))
	if len(out.Expired) != len(indexes) {
		t.Fatalf("a grace window after compaction the round expired %v, want the %d replaced index objects", out.Expired, len(indexes))
	}
	remove(t, bs, out.Expired)
	fresh = open(t, bs, kr)
	for _, h := range []hash.Hash{hs[0], hs[2]} {
		if _, err := fresh.Get(ctx, h); err != nil {
			t.Fatalf("a live chunk does not read after the replaced index objects expired: %v", err)
		}
	}
	for _, n := range append(objects(t, bs, "packs/"), objects(t, bs, "index/")...) {
		if !out.Named[n] {
			t.Fatalf("%s is in the store and not named by the manifest", n)
		}
	}
}

// A condemned pack something reaches again is reprieved, and never expires.
func TestAReachedPackIsReprieved(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	hs := published(t, open(t, bs, kr), "a, the root", "b, reached again")
	if out := round(t, bs, kr, liveSet(hs[0]), t0); out.Condemned != 1 {
		t.Fatalf("fixture: condemned %d, want 1", out.Condemned)
	}
	out := round(t, bs, kr, liveSet(hs...), t0.Add(time.Hour))
	if out.Reprieved != 1 || len(out.Expired) != 0 {
		t.Fatalf("a condemned pack reached again: reprieved %d, expired %v; want 1, none", out.Reprieved, out.Expired)
	}
	if out = round(t, bs, kr, liveSet(hs...), t0.Add(3*time.Hour)); len(out.Expired) != 0 || out.Condemned != 0 {
		t.Fatalf("the reprieved pack: condemned %d, expired %v later; want nothing", out.Condemned, out.Expired)
	}
}

// A round decides against one manifest: a writer that published since
// makes Apply ErrMoved, and the manifest keeps what the writer published.
func TestARoundLosesToAWriterThatPublished(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	s := open(t, bs, kr)
	hs := published(t, s, "a, the root", "b, garbage")
	r, err := packstore.Begin(ctx, packstore.Options{Blobs: bs, Keys: kr, Repo: repo})
	if err != nil {
		t.Fatal(err)
	}
	if r.Root() != hs[0] {
		t.Fatalf("the round marks from %s, want the root %s", r.Root().Short(), hs[0].Short())
	}
	d, err := s.Put(ctx, []byte("d, published during the round"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CompareAndSetRoot(ctx, hs[0], d); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Apply(ctx, liveSet(hs[0]), t0, time.Hour); !errors.Is(err, packstore.ErrMoved) {
		t.Fatalf("Apply after a writer published = %v, want ErrMoved", err)
	}
	fresh := open(t, bs, kr)
	if root, err := fresh.Root(ctx); err != nil || root != d {
		t.Fatalf("after the lost round the root is %s (%v), want the writer's %s", root.Short(), err, d.Short())
	}
	if out := round(t, bs, kr, liveSet(d), t0); out.Condemned != 2 {
		t.Fatalf("a round begun after the writer condemned %d packs, want the 2 its root does not reach", out.Condemned)
	}
}

// Marking always reaches the root, so a live set without it is a marking
// bug: Apply refuses it and decides nothing, rather than condemn the root.
func TestARoundRefusesALiveSetWithoutTheRoot(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	hs := published(t, open(t, bs, kr), "a, the root", "b")
	r, err := packstore.Begin(ctx, packstore.Options{Blobs: bs, Keys: kr, Repo: repo})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Apply(ctx, liveSet(hs[1]), t0, 0); err == nil {
		t.Fatal("Apply with a live set that leaves out the root succeeded")
	}
	if out := round(t, bs, kr, liveSet(hs...), t0.Add(time.Hour)); out.Condemned != 0 || len(out.Expired) != 0 {
		t.Fatalf("after the refused round: condemned %d, expired %v; the refused round decided something", out.Condemned, out.Expired)
	}
}

// condemnedAt publishes a root and a garbage chunk, each in a pack of its
// own, and condemns the garbage chunk's pack at t0.
func condemnedAt(t *testing.T, bs blob.BlobStore, kr *seal.Keyring) (root, garbage hash.Hash) {
	t.Helper()
	hs := published(t, open(t, bs, kr), "a, the root", "b, garbage")
	if out := round(t, bs, kr, liveSet(hs[0]), t0); out.Condemned != 1 {
		t.Fatalf("fixture: condemned %d packs, want 1", out.Condemned)
	}
	return hs[0], hs[1]
}

// A writer that knows a pack is condemned never counts on it: putting
// bytes a condemned pack holds stores them again. (Before condemnation, the
// positive control, a put of stored bytes stores nothing.)
func TestWritersDoNotDeduplicateAgainstCondemnedPacks(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	hs := published(t, open(t, bs, kr), "a, the root", "b, garbage")
	w := open(t, bs, kr)
	if _, err := w.Put(ctx, []byte("b, garbage")); err != nil {
		t.Fatal(err)
	}
	if err := w.CompareAndSetRoot(ctx, hs[0], hs[0]); err != nil {
		t.Fatal(err)
	}
	if n := len(objects(t, bs, "packs/")); n != 2 {
		t.Fatalf("positive control: putting stored bytes left %d packs, want the 2 already there", n)
	}
	if out := round(t, bs, kr, liveSet(hs[0]), t0); out.Condemned != 1 {
		t.Fatalf("fixture: condemned %d packs, want 1", out.Condemned)
	}
	w = open(t, bs, kr)
	h, err := w.Put(ctx, []byte("b, garbage"))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.CompareAndSetRoot(ctx, hs[0], h); err != nil {
		t.Fatal(err)
	}
	if n := len(objects(t, bs, "packs/")); n != 3 {
		t.Fatalf("after its pack was condemned, putting the bytes again left %d packs, want a third holding a fresh copy", n)
	}
}

// A writer that counted on a pack GC then expired publishes nothing: its
// session is lost (chunk.ErrSessionLost), the root stays where it was, and
// the store refuses every later write while it still reads. Reopened, it
// publishes. A writer whose chunks all survived the same collection
// publishes as usual.
func TestAWriterFencedWhenAPackItCountedOnExpires(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	hs := published(t, open(t, bs, kr), "a, the root", "b, garbage")
	stale, kept := open(t, bs, kr), open(t, bs, kr)
	if _, err := stale.Put(ctx, []byte("b, garbage")); err != nil { // counts on b's pack
		t.Fatal(err)
	}
	if _, err := kept.Put(ctx, []byte("a, the root")); err != nil { // counts on a's pack
		t.Fatal(err)
	}
	round(t, bs, kr, liveSet(hs[0]), t0)
	out := round(t, bs, kr, liveSet(hs[0]), t0.Add(time.Hour))
	if len(out.Expired) != 1 {
		t.Fatalf("fixture: expired %v, want b's pack", out.Expired)
	}
	remove(t, bs, out.Expired)
	if err := stale.CompareAndSetRoot(ctx, hs[0], hs[1]); !errors.Is(err, chunk.ErrSessionLost) {
		t.Fatalf("publishing a root on a chunk whose pack expired = %v, want ErrSessionLost", err)
	}
	if root, err := open(t, bs, kr).Root(ctx); err != nil || root != hs[0] {
		t.Fatalf("after the fenced publish the root is %s (%v), want %s", root.Short(), err, hs[0].Short())
	}
	if _, err := stale.Put(ctx, []byte("b, garbage")); !errors.Is(err, chunk.ErrSessionLost) {
		t.Fatalf("writing the lost chunk again on the same store = %v, want ErrSessionLost: the store must be reopened", err)
	}
	if err := stale.CompareAndSetRoot(ctx, hs[0], hs[0]); !errors.Is(err, chunk.ErrSessionLost) {
		t.Fatalf("a second publish on the store whose session was lost = %v, want ErrSessionLost", err)
	}
	if b, err := stale.Get(ctx, hs[0]); err != nil || string(b) != "a, the root" {
		t.Fatalf("the store whose session was lost reads the root as %q, %v; reads must go on", b, err)
	}
	if err := kept.CompareAndSetRoot(ctx, hs[0], hs[0]); err != nil {
		t.Fatalf("positive control: a writer whose chunks survived the collection: %v", err)
	}
	reopened := open(t, bs, kr)
	b, err := reopened.Put(ctx, []byte("b, garbage"))
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.CompareAndSetRoot(ctx, hs[0], b); err != nil {
		t.Fatalf("reopened, the store does not publish the chunk written again: %v", err)
	}
}

// A store that learns packs expired forgets their chunks: its index is
// rebuilt from the manifest, so it neither vouches for them (Has) nor
// reads them as corrupt (Get is ErrNotFound), and putting them again
// stores them.
func TestAStoreForgetsChunksWhosePacksExpired(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	root, garbage := condemnedAt(t, bs, kr)
	r := open(t, bs, kr) // knows b's pack
	if have, err := r.Has(ctx, []hash.Hash{garbage}); err != nil || !have[garbage] {
		t.Fatalf("fixture: a store opened before the expiry has b: %v, %v", have, err)
	}
	out := round(t, bs, kr, liveSet(root), t0.Add(time.Hour))
	remove(t, bs, out.Expired)
	if _, err := r.Root(ctx); err != nil { // a refresh
		t.Fatal(err)
	}
	if have, err := r.Has(ctx, []hash.Hash{garbage, root}); err != nil || have[garbage] || !have[root] {
		t.Fatalf("after the expiry the store has b: %v and the root: %v (%v); want b forgotten, the root kept", have[garbage], have[root], err)
	}
	if _, err := r.Get(ctx, garbage); !errors.Is(err, chunk.ErrNotFound) {
		t.Fatalf("reading the expired chunk = %v, want ErrNotFound", err)
	}
	before := len(objects(t, bs, "packs/"))
	if _, err := r.Put(ctx, []byte("b, garbage")); err != nil {
		t.Fatal(err)
	}
	if err := r.CompareAndSetRoot(ctx, root, root); err != nil {
		t.Fatal(err)
	}
	if after := len(objects(t, bs, "packs/")); after != before+1 {
		t.Fatalf("putting the expired bytes again left %d packs, want %d: they were counted as stored", after, before+1)
	}
}

// A store opened before an expiry that reads an expired chunk learns of
// the expiry from the missing pack: ErrNotFound, not corruption.
func TestReadingAnExpiredChunkIsNotFound(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	root, garbage := condemnedAt(t, bs, kr)
	r := open(t, bs, kr)
	out := round(t, bs, kr, liveSet(root), t0.Add(time.Hour))
	remove(t, bs, out.Expired)
	if _, err := r.Get(ctx, garbage); !errors.Is(err, chunk.ErrNotFound) {
		t.Fatalf("a store opened before the expiry reads the expired chunk as %v, want ErrNotFound", err)
	}
	if _, err := r.Get(ctx, root); err != nil {
		t.Fatalf("positive control: the root: %v", err)
	}
}

// A store's own packs come through a rebuild of its index as they should:
// a pack it finished but has not published stays readable, and publishes,
// after GC expired packs elsewhere; a pack it published itself, and GC
// later expired, is forgotten like any other.
func TestAStoresOwnPacksAcrossARebuild(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	s, err := packstore.Open(ctx, packstore.WithBackoff(packstore.Options{Blobs: bs, Keys: kr, Repo: repo, PackSize: 16 << 10}, time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	hs := published(t, s, "a, the root", "b, garbage") // s published b's pack itself
	round(t, bs, kr, liveSet(hs[0]), t0)
	c, err := s.Put(ctx, payload("own", 10<<10))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put(ctx, payload("more", 10<<10)); err != nil { // fills the pack holding c
		t.Fatal(err)
	}
	packstore.WaitUploads(s)
	if n := len(objects(t, bs, "packs/")); n != 3 {
		t.Fatalf("fixture: %d packs in the store, want a third: c's, finished and not published", n)
	}
	out := round(t, bs, kr, liveSet(hs[0]), t0.Add(time.Hour))
	if len(out.Expired) != 1 {
		t.Fatalf("fixture: expired %v, want b's pack", out.Expired)
	}
	remove(t, bs, out.Expired)
	if _, err := s.Root(ctx); err != nil { // a refresh, and the rebuild
		t.Fatal(err)
	}
	if have, err := s.Has(ctx, []hash.Hash{hs[1]}); err != nil || have[hs[1]] {
		t.Fatalf("the store that published b still has it after its pack expired: %v (%v)", have[hs[1]], err)
	}
	if b, err := s.Get(ctx, c); err != nil || !bytes.Equal(b, payload("own", 10<<10)) {
		t.Fatalf("after the rebuild, the store's own unpublished chunk reads as %d bytes, %v", len(b), err)
	}
	if err := s.CompareAndSetRoot(ctx, hs[0], c); err != nil {
		t.Fatalf("publishing the store's own chunk after the rebuild: %v", err)
	}
	if _, err := open(t, bs, kr).Get(ctx, c); err != nil {
		t.Fatalf("the published chunk does not read from a fresh store: %v", err)
	}
}
