package packstore_test

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/mem"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/packstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
)

// statCounting counts the lookups a store makes.
type statCounting struct {
	blob.BlobStore
	stats atomic.Int64
}

func (s *statCounting) Stat(ctx context.Context, name string) (blob.Info, error) {
	s.stats.Add(1)
	return s.BlobStore.Stat(ctx, name)
}

// slowWriter opens a store with 16 KiB packs whose uploads it dates by
// *now.
func slowWriter(t *testing.T, bs blob.BlobStore, kr *seal.Keyring, now *time.Time) *packstore.Store {
	t.Helper()
	s, err := packstore.Open(ctx, packstore.WithBackoff(packstore.Options{Blobs: bs, Keys: kr, Repo: repo, PackSize: 16 << 10,
		Clock: func() time.Time { return *now }}, time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// uploadUnpublished puts c and a second chunk that fills the pack holding
// c, so the store uploads that pack and publishes nothing; it returns the
// pack's name.
func uploadUnpublished(t *testing.T, bs blob.BlobStore, s *packstore.Store, c []byte) string {
	t.Helper()
	before := objects(t, bs, "packs/")
	if _, err := s.Put(ctx, c); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put(ctx, payload("filler for "+hash.Sum(c).String(), 10<<10)); err != nil { // never stored before, so it fills
		t.Fatal(err)
	}
	added := newNames(before, objects(t, bs, "packs/"))
	if len(added) != 1 {
		t.Fatalf("fixture: the writer uploaded packs %v, want one", added)
	}
	return added[0]
}

func newNames(before, after []string) []string {
	had := map[string]bool{}
	for _, n := range before {
		had[n] = true
	}
	var out []string
	for _, n := range after {
		if !had[n] {
			out = append(out, n)
		}
	}
	return out
}

// orphanRound is a GC round handed candidates, as GC hands it what it
// listed older than the grace window (an hour here).
func orphanRound(t *testing.T, bs blob.BlobStore, kr *seal.Keyring, live packstore.Live, now time.Time, candidates []string) packstore.Outcome {
	t.Helper()
	r, err := packstore.Begin(ctx, packstore.Options{Blobs: bs, Keys: kr, Repo: repo})
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	r.Orphans(candidates)
	r.Repack(packstore.Repack{Off: true}) // condemnation alone; repacking has tests of its own
	out, err := r.Apply(ctx, live, now, time.Hour)
	if err != nil {
		t.Fatalf("Apply at %v: %v", now.Sub(t0), err)
	}
	return out
}

func recorded(t *testing.T, bs blob.BlobStore, kr *seal.Keyring) string {
	t.Helper()
	names, err := packstore.Recorded(ctx, packstore.Options{Blobs: bs, Keys: kr, Repo: repo})
	if err != nil {
		t.Fatal(err)
	}
	return strings.Join(names, " ")
}

func sortedJoin(names []string) string {
	s := append([]string(nil), names...)
	sort.Strings(s)
	return strings.Join(s, " ")
}

// DESIGN §9, issue #3: of what GC hands a round, the round records every
// pack and index object its manifest does not name as a deleted orphan, and
// hands them back for deletion; what the manifest names, and a pack the
// same round expires, is never an orphan. The record stands for a grace
// window and an hour, and then lapses.
func TestARoundRecordsTheOrphansItDeletes(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	hs := published(t, open(t, bs, kr), "a, the root", "b, garbage")
	live := liveSet(hs[0])
	strayPack := "packs/ab/ab" + strings.Repeat("0", 62)
	strayIndex := "index/" + strings.Repeat("1", 64)
	for _, n := range []string{strayPack, strayIndex} {
		if err := bs.Put(ctx, n, strings.NewReader("never published"), 15); err != nil {
			t.Fatal(err)
		}
	}
	all := func() []string { return append(objects(t, bs, "packs/"), objects(t, bs, "index/")...) }

	out := orphanRound(t, bs, kr, live, t0, all())
	want := sortedJoin([]string{strayPack, strayIndex})
	if got := sortedJoin(out.Orphans); got != want {
		t.Fatalf("the round handed back orphans %q, want exactly the two no manifest names, %q", got, want)
	}
	if got := recorded(t, bs, kr); got != want {
		t.Fatalf("the manifest records %q as deleted orphans, want %q", got, want)
	}
	remove(t, bs, out.Orphans)

	out = orphanRound(t, bs, kr, live, t0.Add(time.Hour+59*time.Minute), all())
	if len(out.Expired) != 1 || len(out.Orphans) != 0 {
		t.Fatalf("a grace window on, the round expired %v and handed back orphans %v; want b's pack expired, and no orphan", out.Expired, out.Orphans)
	}
	if got := recorded(t, bs, kr); got != want {
		t.Fatalf("a grace window and 59 minutes after the deletion the manifest records %q, want the record to stand: %q", got, want)
	}
	remove(t, bs, out.Expired)

	orphanRound(t, bs, kr, live, t0.Add(2*time.Hour), all())
	if got := recorded(t, bs, kr); got != "" {
		t.Fatalf("a grace window and an hour after the deletion the manifest still records %q", got)
	}
}

// A writer that held a pack it uploaded while GC deleted it as an orphan
// cannot publish: the manifest it swaps against records the deletion (its
// own view was older, so it loses the swap and reads the record), and the
// publish fails with ErrSessionLost; the root stays. To the writer's clock
// the upload is a moment old, so only the record can tell. The store then
// refuses every write, even once the record has lapsed, and still reads;
// reopened, it writes the chunk again and publishes (issue #3).
func TestAPublishFailsOnAPackRecordedAsADeletedOrphan(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	now := t0
	s := slowWriter(t, bs, kr, &now)
	hs := published(t, s, "a, the root")
	c := payload("slow", 10<<10)
	pack := uploadUnpublished(t, bs, s, c)

	out := orphanRound(t, bs, kr, liveSet(hs[0]), t0.Add(2*time.Hour), []string{pack})
	if sortedJoin(out.Orphans) != pack {
		t.Fatalf("fixture: the round handed back %v, want the writer's pack", out.Orphans)
	}
	remove(t, bs, out.Orphans)
	if err := s.CompareAndSetRoot(ctx, hs[0], hash.Sum(c)); !errors.Is(err, chunk.ErrSessionLost) {
		t.Fatalf("publishing a chunk whose pack GC deleted as an orphan = %v, want ErrSessionLost", err)
	}
	if root, err := open(t, bs, kr).Root(ctx); err != nil || root != hs[0] {
		t.Fatalf("after the refused publish the root is %s (%v), want it unchanged", root.Short(), err)
	}
	if _, err := s.Put(ctx, []byte("a later put")); !errors.Is(err, chunk.ErrSessionLost) {
		t.Fatalf("a put on the store whose session was lost = %v, want ErrSessionLost", err)
	}
	orphanRound(t, bs, kr, liveSet(hs[0]), t0.Add(4*time.Hour), nil)
	if got := recorded(t, bs, kr); got != "" {
		t.Fatalf("fixture: the record %q has not lapsed", got)
	}
	if _, err := s.Root(ctx); err != nil { // a read: the store now sees the manifest without the record
		t.Fatal(err)
	}
	if err := s.CompareAndSetRoot(ctx, hs[0], hs[0]); !errors.Is(err, chunk.ErrSessionLost) {
		t.Fatalf("a second publish on the store whose session was lost, the record lapsed = %v, want ErrSessionLost", err)
	}
	if b, err := s.Get(ctx, hs[0]); err != nil || string(b) != "a, the root" {
		t.Fatalf("the store whose session was lost reads the root as %q, %v; reads must go on", b, err)
	}
	again := slowWriter(t, bs, kr, &now)
	if _, err := again.Put(ctx, c); err != nil {
		t.Fatal(err)
	}
	if err := again.CompareAndSetRoot(ctx, hs[0], hash.Sum(c)); err != nil {
		t.Fatalf("reopened, the store does not publish the chunk written again: %v", err)
	}
	if b, err := open(t, bs, kr).Get(ctx, hash.Sum(c)); err != nil || !bytes.Equal(b, c) {
		t.Fatalf("the chunk written again reads as %d bytes, %v", len(b), err)
	}
}

// Long after its record lapsed, a deleted upload is the writer's own check
// to find: one over an hour old by the writer's clock is looked up before
// publishing, and gone, fails the publish with ErrSessionLost. A publish of
// fresh uploads looks nothing up (issue #3).
func TestAPublishFailsOnAnOldUploadThatIsGone(t *testing.T) {
	counted := &statCounting{BlobStore: mem.New()}
	kr := keyring(t)
	now := t0
	s := slowWriter(t, counted, kr, &now)
	hs := published(t, s, "a, the root")
	fresh := payload("fresh", 10<<10)
	uploadUnpublished(t, counted, s, fresh)
	stats := counted.stats.Load()
	if err := s.CompareAndSetRoot(ctx, hs[0], hash.Sum(fresh)); err != nil {
		t.Fatalf("publishing fresh uploads: %v", err)
	}
	if n := counted.stats.Load() - stats; n != 0 {
		t.Fatalf("publishing uploads a moment old looked up %d objects, want none", n)
	}

	c := payload("slow", 10<<10)
	pack := uploadUnpublished(t, counted, s, c)
	remove(t, counted, []string{pack}) // deleted so long ago that no record of it remains
	now = t0.Add(time.Hour + time.Second)
	if err := s.CompareAndSetRoot(ctx, hash.Sum(fresh), hash.Sum(c)); !errors.Is(err, chunk.ErrSessionLost) {
		t.Fatalf("publishing an upload over an hour old that is gone = %v, want ErrSessionLost", err)
	}
	if root, err := open(t, counted, kr).Root(ctx); err != nil || root != hash.Sum(fresh) {
		t.Fatalf("after the refused publish the root is %s (%v), want it unchanged", root.Short(), err)
	}
}

// The writer's index objects are checked like its packs: one written at a
// publish that lost, then deleted as an orphan (its pack is still there),
// fails the next publish with ErrSessionLost instead of leaving a manifest
// that names it, which no store could open (issue #3).
func TestAPublishFailsOnAnIndexObjectRecordedAsADeletedOrphan(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	now := t0
	s := slowWriter(t, bs, kr, &now)
	hs := published(t, s, "a, the root")
	c, index := lostPublish(t, bs, s)
	out := orphanRound(t, bs, kr, liveSet(hs[0]), t0.Add(2*time.Hour), []string{index})
	if sortedJoin(out.Orphans) != index {
		t.Fatalf("fixture: the round handed back %v, want the index object", out.Orphans)
	}
	remove(t, bs, out.Orphans)
	if err := s.CompareAndSetRoot(ctx, hs[0], c); !errors.Is(err, chunk.ErrSessionLost) {
		t.Fatalf("publishing an index object GC deleted as an orphan = %v, want ErrSessionLost", err)
	}
	if root, err := open(t, bs, kr).Root(ctx); err != nil || root != hs[0] {
		t.Fatalf("after the refused publish a fresh store opens at %s (%v), want the old root", root.Short(), err)
	}
}

// An index object over an hour old that is gone, its record long lapsed,
// fails the publish too (issue #3).
func TestAPublishFailsOnAnOldIndexObjectThatIsGone(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	now := t0
	s := slowWriter(t, bs, kr, &now)
	hs := published(t, s, "a, the root")
	c, index := lostPublish(t, bs, s)
	remove(t, bs, []string{index}) // deleted so long ago that no record of it remains
	now = t0.Add(time.Hour + time.Second)
	if err := s.CompareAndSetRoot(ctx, hs[0], c); !errors.Is(err, chunk.ErrSessionLost) {
		t.Fatalf("publishing an index object over an hour old that is gone = %v, want ErrSessionLost", err)
	}
	if root, err := open(t, bs, kr).Root(ctx); err != nil || root != hs[0] {
		t.Fatalf("after the refused publish a fresh store opens at %s (%v), want the old root", root.Short(), err)
	}
}

// lostPublish puts a chunk and publishes it against the wrong root, which
// fails after the store uploaded its pack and wrote its index object; it
// returns the chunk and the index object's name.
func lostPublish(t *testing.T, bs blob.BlobStore, s *packstore.Store) (hash.Hash, string) {
	t.Helper()
	c, err := s.Put(ctx, []byte("in a publish that lost"))
	if err != nil {
		t.Fatal(err)
	}
	indexes := objects(t, bs, "index/")
	if err := s.CompareAndSetRoot(ctx, hash.Sum([]byte("not the root")), c); !errors.Is(err, chunk.ErrRootConflict) {
		t.Fatalf("fixture: a publish against the wrong root = %v, want ErrRootConflict", err)
	}
	added := newNames(indexes, objects(t, bs, "index/"))
	if len(added) != 1 {
		t.Fatalf("fixture: the lost publish wrote index objects %v, want one", added)
	}
	return c, added[0]
}

// Reading a chunk the store promised, and GC took, ends the session too: a
// chunk a put counted on, in a pack GC expired, and a chunk the store wrote,
// in a pack GC deleted as an orphan, read as ErrSessionLost, never as a
// plain miss or as corruption, and the store then refuses writes. A host
// reads what it just wrote before it publishes (issue #3).
func TestReadingAPromisedChunkThatIsGoneLosesTheSession(t *testing.T) {
	t.Run("counted on, in a pack GC expired", func(t *testing.T) {
		bs, kr := mem.New(), keyring(t)
		hs := published(t, open(t, bs, kr), "a, the root", "b, garbage")
		s := open(t, bs, kr)
		if _, err := s.Put(ctx, []byte("b, garbage")); err != nil { // counted on: b's pack
			t.Fatal(err)
		}
		round(t, bs, kr, liveSet(hs[0]), t0)
		out := round(t, bs, kr, liveSet(hs[0]), t0.Add(time.Hour))
		if len(out.Expired) != 1 {
			t.Fatalf("fixture: expired %v, want b's pack", out.Expired)
		}
		remove(t, bs, out.Expired)
		if _, err := s.Get(ctx, hs[1]); !errors.Is(err, chunk.ErrSessionLost) {
			t.Fatalf("reading a chunk the store counted on, whose pack expired = %v, want ErrSessionLost", err)
		}
		if _, err := s.Put(ctx, []byte("anything")); !errors.Is(err, chunk.ErrSessionLost) {
			t.Fatalf("a put after that read = %v, want ErrSessionLost", err)
		}
	})
	t.Run("written, in a pack GC deleted as an orphan", func(t *testing.T) {
		bs, kr := mem.New(), keyring(t)
		now := t0
		s := slowWriter(t, bs, kr, &now)
		hs := published(t, s, "a, the root")
		c := payload("written", 10<<10)
		pack := uploadUnpublished(t, bs, s, c)
		out := orphanRound(t, bs, kr, liveSet(hs[0]), t0.Add(2*time.Hour), []string{pack})
		if sortedJoin(out.Orphans) != pack {
			t.Fatalf("fixture: the round handed back %v, want the writer's pack", out.Orphans)
		}
		remove(t, bs, out.Orphans)
		if _, err := s.Get(ctx, hash.Sum(c)); !errors.Is(err, chunk.ErrSessionLost) {
			t.Fatalf("reading a chunk the store wrote, whose pack GC deleted as an orphan = %v, want ErrSessionLost", err)
		}
		if _, err := s.Put(ctx, []byte("anything")); !errors.Is(err, chunk.ErrSessionLost) {
			t.Fatalf("a put after that read = %v, want ErrSessionLost", err)
		}
	})
}
