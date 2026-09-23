package packstore_test

import (
	"bytes"
	"errors"
	"io"
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

// packed puts every chunk in one session and publishes once, so they share
// a pack; the root is the first.
func packed(t *testing.T, s *packstore.Store, root hash.Hash, chunks ...[]byte) []hash.Hash {
	t.Helper()
	var hs []hash.Hash
	for _, c := range chunks {
		h, err := s.Put(ctx, c)
		if err != nil {
			t.Fatal(err)
		}
		hs = append(hs, h)
	}
	if err := s.CompareAndSetRoot(ctx, root, hs[0]); err != nil {
		t.Fatal(err)
	}
	return hs
}

// repackRound is a GC round with a repacking policy.
func repackRound(t *testing.T, bs blob.BlobStore, kr *seal.Keyring, live func(hash.Hash) bool, now time.Time, p packstore.Repack) packstore.Outcome {
	t.Helper()
	r, err := packstore.Begin(ctx, packstore.Options{Blobs: bs, Keys: kr, Repo: repo})
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	r.Repack(p)
	out, err := r.Apply(ctx, live, now, time.Hour)
	if err != nil {
		t.Fatalf("Apply at %v: %v", now.Sub(t0), err)
	}
	return out
}

func repackedNames(t *testing.T, bs blob.BlobStore, kr *seal.Keyring) []string {
	t.Helper()
	names, err := packstore.RepackedNames(ctx, packstore.Options{Blobs: bs, Keys: kr, Repo: repo})
	if err != nil {
		t.Fatal(err)
	}
	return names
}

// packsOf is the packs among names, index objects left out.
func packsOf(names []string) []string {
	var out []string
	for _, n := range names {
		if strings.HasPrefix(n, "packs/") {
			out = append(out, n)
		}
	}
	return out
}

func locationOf(t *testing.T, s *packstore.Store, h hash.Hash) string {
	t.Helper()
	name, _, _, ok := s.Location(h)
	if !ok {
		t.Fatalf("%s is not located", h.Short())
	}
	return name
}

// DESIGN §9, #1: a kept pack whose live bytes are under half its size is
// repacked: its live chunks are copied into a new pack listed at once, the
// old pack is recorded as repacked and stays until it expires (a reader
// holding a dead chunk's hash still reads it within the grace window), and
// a fresh store finds the live chunks in the new pack. A grace window on,
// the old pack expires, and with it the dead chunks.
func TestARoundRepacksAPackThatIsMostlyDead(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	s := open(t, bs, kr)
	live := payload("live", 1<<10)
	hs := packed(t, s, hash.Hash{}, []byte("the root"), payload("dead one", 3<<10), payload("dead two", 3<<10), live)
	old := objects(t, bs, "packs/")
	if len(old) != 1 {
		t.Fatalf("fixture: %d packs, want the one shared pack", len(old))
	}
	out := repackRound(t, bs, kr, liveSet(hs[0], hs[3]), t0, packstore.Repack{})
	if out.Repacked != 1 || out.Copied == 0 {
		t.Fatalf("the round repacked %d packs and copied %d bytes; want the one pack, three quarters dead, and its live frames", out.Repacked, out.Copied)
	}
	if got := repackedNames(t, bs, kr); strings.Join(got, " ") != old[0] {
		t.Fatalf("the manifest records %q as repacked, want the old pack %s", got, old[0])
	}
	added := newNames(old, objects(t, bs, "packs/"))
	if len(added) != 1 {
		t.Fatalf("the round wrote %d new packs, want one holding the live chunks", len(added))
	}
	fresh := open(t, bs, kr)
	for _, h := range []hash.Hash{hs[0], hs[3]} {
		if got := locationOf(t, fresh, h); got != added[0] {
			t.Fatalf("a fresh store locates live chunk %s in %s, want the new pack %s", h.Short(), got, added[0])
		}
	}
	if b, err := fresh.Get(ctx, hs[3]); err != nil || !bytes.Equal(b, live) {
		t.Fatalf("the live chunk reads as %d bytes, %v", len(b), err)
	}
	if _, err := fresh.Get(ctx, hs[1]); err != nil {
		t.Fatalf("within the grace window a dead chunk in the repacked pack reads as %v, want it still readable", err)
	}

	out = repackRound(t, bs, kr, liveSet(hs[0], hs[3]), t0.Add(30*time.Minute), packstore.Repack{})
	if out.Repacked != 0 || len(out.Expired) != 0 {
		t.Fatalf("half a grace window on, the round repacked %d and expired %v; want nothing, the repacked pack neither reprieved nor expired", out.Repacked, out.Expired)
	}
	out = repackRound(t, bs, kr, liveSet(hs[0], hs[3]), t0.Add(time.Hour), packstore.Repack{})
	if got := packsOf(out.Expired); strings.Join(got, " ") != old[0] {
		t.Fatalf("a grace window on, the round expired %v, want the repacked pack %s alone (and the index object that listed it)", out.Expired, old[0])
	}
	remove(t, bs, out.Expired)
	fresh = open(t, bs, kr)
	if b, err := fresh.Get(ctx, hs[3]); err != nil || !bytes.Equal(b, live) {
		t.Fatalf("after the repacked pack expired the live chunk reads as %d bytes, %v", len(b), err)
	}
	if _, err := fresh.Get(ctx, hs[1]); !errors.Is(err, chunk.ErrNotFound) {
		t.Fatalf("after the repacked pack expired a dead chunk reads as %v, want ErrNotFound", err)
	}
}

// A pack whose live bytes are at or above the threshold is kept whole; the
// threshold is the policy's.
func TestAPackAboveTheThresholdIsKeptWhole(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	s := open(t, bs, kr)
	hs := packed(t, s, hash.Hash{}, []byte("the root"), payload("live", 3<<10), payload("dead", 1<<10))
	if out := repackRound(t, bs, kr, liveSet(hs[0], hs[1]), t0, packstore.Repack{}); out.Repacked != 0 {
		t.Fatalf("a pack three quarters live was repacked under the default policy")
	}
	if got := repackedNames(t, bs, kr); len(got) != 0 {
		t.Fatalf("the manifest records %q as repacked", got)
	}
	if out := repackRound(t, bs, kr, liveSet(hs[0], hs[1]), t0, packstore.Repack{MaxLive: 0.9, Off: true}); out.Repacked != 0 {
		t.Fatalf("with repacking off the round repacked %d packs", out.Repacked)
	}
	if out := repackRound(t, bs, kr, liveSet(hs[0], hs[1]), t0, packstore.Repack{MaxLive: 0.9}); out.Repacked != 1 {
		t.Fatalf("with the threshold at nine tenths the pack was not repacked")
	}
}

// The budget bounds a round: with several packs mostly dead, the emptiest
// are repacked first, and the round stops once it has copied its budget.
func TestRepackingSpendsItsBudgetOnTheEmptiestPacksFirst(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	s := open(t, bs, kr)
	hs := packed(t, s, hash.Hash{}, []byte("the root"))
	root := hs[0]
	// Three packs, each with one live chunk of one KiB and dead ones: the
	// emptiest has the most dead bytes.
	var live []hash.Hash
	var packs []string
	for i, dead := range []int{1, 4, 8} {
		before := objects(t, bs, "packs/")
		var chunks [][]byte
		chunks = append(chunks, payload("live "+string(rune('a'+i)), 1<<10))
		for d := 0; d < dead; d++ {
			chunks = append(chunks, payload("dead "+string(rune('a'+i))+string(rune('0'+d)), 1<<10))
		}
		var err error
		for _, c := range chunks {
			var h hash.Hash
			if h, err = s.Put(ctx, c); err != nil {
				t.Fatal(err)
			}
			if len(live) == i {
				live = append(live, h)
			}
		}
		if err := s.CompareAndSetRoot(ctx, root, root); err != nil {
			t.Fatal(err)
		}
		added := newNames(before, objects(t, bs, "packs/"))
		if len(added) != 1 {
			t.Fatalf("fixture: session %d wrote %d packs, want one", i, len(added))
		}
		packs = append(packs, added[0])
	}
	out := repackRound(t, bs, kr, liveSet(append(live, root)...), t0, packstore.Repack{Budget: 1})
	if out.Repacked != 1 {
		t.Fatalf("with a budget of one byte the round repacked %d packs, want the emptiest alone", out.Repacked)
	}
	if got := repackedNames(t, bs, kr); strings.Join(got, " ") != packs[2] {
		t.Fatalf("the round repacked %q, want the emptiest pack %s (one live KiB of nine)", got, packs[2])
	}
	out = repackRound(t, bs, kr, liveSet(append(live, root)...), t0.Add(time.Minute), packstore.Repack{})
	if out.Repacked != 2 {
		t.Fatalf("with the default budget the next round repacked %d packs, want the two left", out.Repacked)
	}
}

// A repack round moves gcGen and every store rebuilds, resolving the live
// chunks to the new packs first: a writer that deduplicates one afterwards
// writes no new pack. A chunk only the repacked pack holds is another
// matter: that pack is on its way out, so a writer storing its bytes again
// writes them afresh.
func TestAfterARepackWritersFindTheNewPacks(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	s := open(t, bs, kr)
	live, dead := payload("live", 1<<10), payload("dead one", 3<<10)
	hs := packed(t, s, hash.Hash{}, []byte("the root"), dead, payload("dead two", 3<<10), live)
	writer := open(t, bs, kr) // its index resolves the live chunk to the old pack
	if out := repackRound(t, bs, kr, liveSet(hs[0], hs[3]), t0, packstore.Repack{}); out.Repacked != 1 {
		t.Fatalf("fixture: repacked %d", out.Repacked)
	}
	if _, err := writer.Root(ctx); err != nil { // a refresh: the round moved gcGen
		t.Fatal(err)
	}
	packs := objects(t, bs, "packs/")
	if _, err := writer.Put(ctx, live); err != nil {
		t.Fatal(err)
	}
	if err := writer.CompareAndSetRoot(ctx, hs[0], hs[0]); err != nil {
		t.Fatal(err)
	}
	if n := len(newNames(packs, objects(t, bs, "packs/"))); n != 0 {
		t.Fatalf("after the repack a writer storing the live chunk wrote %d new packs, want none: it must deduplicate against the new pack", n)
	}
	if _, err := writer.Put(ctx, dead); err != nil {
		t.Fatal(err)
	}
	if err := writer.CompareAndSetRoot(ctx, hs[0], hs[0]); err != nil {
		t.Fatal(err)
	}
	if n := len(newNames(packs, objects(t, bs, "packs/"))); n != 1 {
		t.Fatalf("after the repack a writer storing a chunk only the repacked pack holds wrote %d new packs, want one: the repacked pack expires", n)
	}
}

// Two writers can store the same chunk, each in a pack of its own. When
// both packs are repacked in one round the chunk is copied once, and reads
// back.
func TestRepackingCopiesAChunkTwoPacksShareOnce(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	a, b := open(t, bs, kr), open(t, bs, kr)
	shared := payload("shared", 1<<10)
	hs := packed(t, a, hash.Hash{}, []byte("the root"), shared, payload("dead a", 3<<10))
	if _, err := b.Put(ctx, shared); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Put(ctx, payload("dead b", 3<<10)); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Root(ctx); err != nil { // b learns of a's publish, its own pack already holding the chunk
		t.Fatal(err)
	}
	if err := b.CompareAndSetRoot(ctx, hs[0], hs[0]); err != nil {
		t.Fatal(err)
	}
	if n := len(objects(t, bs, "packs/")); n != 2 {
		t.Fatalf("fixture: %d packs, want one per writer", n)
	}
	out := repackRound(t, bs, kr, liveSet(hs[0], hs[1]), t0, packstore.Repack{})
	if out.Repacked != 2 {
		t.Fatalf("the round repacked %d packs, want both, which share a live chunk", out.Repacked)
	}
	fresh := open(t, bs, kr)
	if got, err := fresh.Get(ctx, hs[1]); err != nil || !bytes.Equal(got, shared) {
		t.Fatalf("the shared chunk reads as %d bytes, %v", len(got), err)
	}
}

// A candidate whose bytes are not what its index says is corrupt: the round
// fails rather than repack it, and repacks it once the pack is whole again.
func TestRepackingRefusesACorruptPack(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	s := open(t, bs, kr)
	hs := packed(t, s, hash.Hash{}, []byte("the root"), payload("live", 1<<10), payload("dead", 3<<10))
	name := objects(t, bs, "packs/")[0]
	rc, err := bs.Get(ctx, name, 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	whole, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	_ = rc.Close()
	replace := func(b []byte) {
		t.Helper()
		if err := bs.Delete(ctx, name); err != nil {
			t.Fatal(err)
		}
		if err := bs.Put(ctx, name, bytes.NewReader(b), int64(len(b))); err != nil {
			t.Fatal(err)
		}
	}
	replace(whole[:len(whole)-1])
	r, err := packstore.Begin(ctx, packstore.Options{Blobs: bs, Keys: kr, Repo: repo})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Apply(ctx, liveSet(hs[0], hs[1]), t0, time.Hour); !errors.Is(err, chunk.ErrCorrupt) {
		t.Fatalf("repacking a pack a byte short: %v, want ErrCorrupt", err)
	}
	replace(whole)
	if out := repackRound(t, bs, kr, liveSet(hs[0], hs[1]), t0, packstore.Repack{}); out.Repacked != 1 {
		t.Fatalf("the pack whole again, the round repacked %d, want it", out.Repacked)
	}
}
