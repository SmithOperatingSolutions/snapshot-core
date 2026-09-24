package packstore_test

import (
	"bytes"
	"errors"
	"io"
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
func repackRound(t *testing.T, bs blob.BlobStore, kr *seal.Keyring, live packstore.Live, now time.Time, p packstore.Repack) packstore.Outcome {
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
// both packs are repacked in one round (each has live chunks of its own
// too) the shared chunk is copied once, and reads back.
func TestRepackingCopiesAChunkTwoPacksShareOnce(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	a, b := open(t, bs, kr), open(t, bs, kr)
	shared, own := payload("shared", 1<<10), payload("b's own", 1<<10)
	hs := packed(t, a, hash.Hash{}, []byte("the root"), shared, payload("dead a", 3<<10))
	var ownHash hash.Hash
	for _, c := range [][]byte{shared, own, payload("dead b", 3<<10)} {
		h, err := b.Put(ctx, c)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Equal(c, own) {
			ownHash = h
		}
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
	out := repackRound(t, bs, kr, liveSet(hs[0], hs[1], ownHash), t0, packstore.Repack{})
	if out.Repacked != 2 {
		t.Fatalf("the round repacked %d packs, want both, which share a live chunk", out.Repacked)
	}
	fresh := open(t, bs, kr)
	for name, want := range map[string][]byte{"shared": shared, "b's own": own} {
		if got, err := fresh.Get(ctx, hash.Sum(want)); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("the %s chunk reads as %d bytes, %v", name, len(got), err)
		}
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
	for name, cut := range map[string][]byte{"a byte short": whole[:len(whole)-1], "cut in the middle of its frames": whole[:len(whole)/2]} {
		replace(cut)
		r, err := packstore.Begin(ctx, packstore.Options{Blobs: bs, Keys: kr, Repo: repo})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := r.Apply(ctx, liveSet(hs[0], hs[1]), t0, time.Hour); !errors.Is(err, chunk.ErrCorrupt) {
			t.Fatalf("repacking a pack %s: %v, want ErrCorrupt", name, err)
		}
	}
	replace(whole)
	if out := repackRound(t, bs, kr, liveSet(hs[0], hs[1]), t0, packstore.Repack{}); out.Repacked != 1 {
		t.Fatalf("the pack whole again, the round repacked %d, want it", out.Repacked)
	}
}

// The packs in service come first in every index, in memory and on disk,
// whatever order the index objects list them in (they sort packs by hash):
// a chunk a repacked pack still holds resolves to its new pack. The
// fixture is drawn again until the repacked pack sorts before the new one,
// the order in which nothing but the rule gets this right.
func TestPacksInServiceComeFirstInTheIndex(t *testing.T) {
	for attempt := 0; attempt < 64; attempt++ {
		bs, kr := mem.New(), keyring(t)
		s := open(t, bs, kr)
		live := payload("live", 1<<10)
		hs := packed(t, s, hash.Hash{}, []byte("the root"), payload("dead one", 3<<10), payload("dead two", 3<<10), live)
		old := objects(t, bs, "packs/")
		if out := repackRound(t, bs, kr, liveSet(hs[0], hs[3]), t0, packstore.Repack{}); out.Repacked != 1 {
			t.Fatalf("fixture: repacked %d", out.Repacked)
		}
		added := newNames(old, objects(t, bs, "packs/"))
		order, err := packstore.PackOrder(ctx, packstore.Options{Blobs: bs, Keys: kr, Repo: repo})
		if err != nil {
			t.Fatal(err)
		}
		if len(order) != 2 {
			t.Fatalf("fixture: the index lists %d packs, want the repacked one and its replacement", len(order))
		}
		if order[0] != old[0] {
			continue // the new pack happens to sort first; draw again
		}
		fresh := open(t, bs, kr)
		if got := locationOf(t, fresh, hs[3]); got != added[0] {
			t.Fatalf("with the repacked pack listed first, a fresh store in memory locates the live chunk in it (%s), want the new pack %s", got, added[0])
		}
		spilled := openSpilled(t, bs, kr, t.TempDir())
		if got := locationOf(t, spilled, hs[3]); got != added[0] {
			t.Fatalf("with the repacked pack listed first, a fresh store on disk locates the live chunk in it (%s), want the new pack %s", got, added[0])
		}
		return
	}
	t.Fatal("in 64 draws the repacked pack never sorted before its replacement: the fixture cannot reach the order under test")
}

// The live chunks of a repacked pack fill new packs of the round's pack
// size: more live bytes than one pack takes go into several.
func TestRepackingFillsSeveralPacksWhenTheyAreFull(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	s := open(t, bs, kr)
	chunks := [][]byte{[]byte("the root")}
	for i := range 4 {
		chunks = append(chunks, payload("live "+string(rune('a'+i)), 1<<10))
	}
	chunks = append(chunks, payload("dead", 8<<10))
	hs := packed(t, s, hash.Hash{}, chunks...)
	old := objects(t, bs, "packs/")
	r, err := packstore.Begin(ctx, packstore.Options{Blobs: bs, Keys: kr, Repo: repo, PackSize: 3 << 10})
	if err != nil {
		t.Fatal(err)
	}
	out, err := r.Apply(ctx, liveSet(hs[:5]...), t0, time.Hour)
	if err != nil || out.Repacked != 1 {
		t.Fatalf("repacked %d, %v; want the one pack", out.Repacked, err)
	}
	if added := newNames(old, objects(t, bs, "packs/")); len(added) < 2 {
		t.Fatalf("four live KiB repacked at a pack size of 3 KiB went into %d new packs, want at least two", len(added))
	}
	fresh := open(t, bs, kr)
	for i := 1; i <= 4; i++ {
		if got, err := fresh.Get(ctx, hs[i]); err != nil || !bytes.Equal(got, chunks[i]) {
			t.Fatalf("live chunk %d reads as %d bytes, %v after the repack", i, len(got), err)
		}
	}
}

// A repack copies the chunks the round credited to the pack it empties, not
// every live chunk the pack holds: a chunk another pack in service holds
// too counts for that pack (the first listed) and stays there. Were it
// copied, the fresh pack would hold a chunk counted dead, look mostly dead
// next window, and be repacked again, every window, for ever. Here two
// writers stored the same chunk; the round credits it to the first pack
// listed, whose chunks are all live, and repacks the other, mostly dead:
// the fresh pack holds that pack's own live chunk alone. A window later
// the repacked pack expires and nothing is repacked or condemned; the
// index objects rewritten over it expire a window after that; and then
// GC has nothing left to do.
func TestARepackLeavesAChunkCreditedToAnotherPack(t *testing.T) {
	for attempt := 0; attempt < 64; attempt++ {
		bs, kr := mem.New(), keyring(t)
		a, b := open(t, bs, kr), open(t, bs, kr)
		shared, own := payload("shared", 2<<10), payload("b's own", 1<<10)
		hs := packed(t, a, hash.Hash{}, []byte("the root"), shared) // a's pack: the root and the shared chunk, both live
		var ownHash hash.Hash
		for _, c := range [][]byte{shared, own, payload("dead b", 6<<10)} { // b's pack: the shared chunk again, its own, and a dead one
			h, err := b.Put(ctx, c)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Equal(c, own) {
				ownHash = h
			}
		}
		if _, err := b.Root(ctx); err != nil { // b learns of a's publish, its own pack already holding the shared chunk
			t.Fatal(err)
		}
		if err := b.CompareAndSetRoot(ctx, hs[0], hs[0]); err != nil {
			t.Fatal(err)
		}
		before := objects(t, bs, "packs/")
		if len(before) != 2 {
			t.Fatalf("fixture: %d packs, want one per writer", len(before))
		}
		o := packstore.Options{Blobs: bs, Keys: kr, Repo: repo}
		order, err := packstore.PackOrder(ctx, o)
		if err != nil {
			t.Fatal(err)
		}
		if aPack := locationOf(t, a, hs[0]); len(order) != 2 || order[0] != aPack {
			continue // b's pack is listed first and credited the shared chunk; draw again
		}
		live := liveSet(hs[0], hs[1], ownHash)
		out := repackRound(t, bs, kr, live, t0, packstore.Repack{})
		if out.Repacked != 1 || out.Condemned != 0 {
			t.Fatalf("fixture: the round repacked %d packs and condemned %d, want b's pack repacked alone", out.Repacked, out.Condemned)
		}
		fresh := newNames(before, objects(t, bs, "packs/"))
		if len(fresh) != 1 {
			t.Fatalf("fixture: the repack wrote %d packs, want one", len(fresh))
		}
		entries, err := packstore.PackEntries(ctx, o, fresh[0])
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 1 || entries[0] != ownHash {
			t.Fatalf("the fresh pack holds %d chunks, want b's own chunk alone: the shared chunk is credited to a's pack and stays there", len(entries))
		}
		window := time.Hour + time.Minute
		out = repackRound(t, bs, kr, live, t0.Add(window), packstore.Repack{})
		if out.Repacked != 0 || out.Condemned != 0 {
			t.Fatalf("a window after the repack the round repacked %d packs and condemned %d, want neither: the fresh pack held a chunk credited elsewhere and looked mostly dead", out.Repacked, out.Condemned)
		}
		if !slices.Contains(out.Expired, order[1]) {
			t.Fatalf("a window after the repack the round expired %v, want b's pack %s among them", out.Expired, order[1])
		}
		remove(t, bs, out.Expired)
		out = repackRound(t, bs, kr, live, t0.Add(2*window), packstore.Repack{})
		if out.Repacked != 0 || out.Condemned != 0 {
			t.Fatalf("two windows after the repack the round repacked %d packs and condemned %d, want neither", out.Repacked, out.Condemned)
		}
		remove(t, bs, out.Expired)
		if out = repackRound(t, bs, kr, live, t0.Add(3*window), packstore.Repack{}); out.Repacked != 0 || out.Condemned != 0 || out.Reprieved != 0 || len(out.Expired) != 0 {
			t.Fatalf("three windows after the repack the round still repacked %d packs, condemned %d, reprieved %d and expired %v: GC does not converge", out.Repacked, out.Condemned, out.Reprieved, out.Expired)
		}
		return
	}
	t.Fatal("in 64 draws a's pack never came first in the index: the fixture cannot reach the order under test")
}
