package packstore_test

import (
	"errors"
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

func liveSet(hs ...hash.Hash) func(hash.Hash) bool {
	set := map[hash.Hash]bool{}
	for _, h := range hs {
		set[h] = true
	}
	return func(h hash.Hash) bool { return set[h] }
}

func round(t *testing.T, bs blob.BlobStore, kr *seal.Keyring, live func(hash.Hash) bool, now time.Time) packstore.Outcome {
	t.Helper()
	r, err := packstore.Begin(ctx, packstore.Options{Blobs: bs, Keys: kr, Repo: repo})
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
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
