package packstore_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/mem"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/packstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
)

// Every read of the root refreshes, and a refresh that opened the manifest
// each time decrypted and parsed all of it, which grows with every publish:
// on memory, 70% of a sustained load's CPU at 70,000 commits. A refresh
// whose root has not moved since the store last opened it answers from
// what the store holds; one that finds it moved opens the new manifest.
func TestARefreshOfAnUnmovedRootOpensNoManifest(t *testing.T) {
	bs := mem.New()
	kr := keyring(t)
	writer, reader := open(t, bs, kr), open(t, bs, kr)
	h, err := writer.Put(ctx, []byte("first"))
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.CompareAndSetRoot(ctx, hash.Hash{}, h); err != nil {
		t.Fatal(err)
	}
	if r, err := reader.Root(ctx); err != nil || r != h {
		t.Fatalf("the reader's root = %s (%v), want %s", r.Short(), err, h.Short())
	}
	for _, s := range []struct {
		who   string
		store *packstore.Store
	}{{"the reader", reader}, {"the writer, after its own publish", writer}} {
		before := packstore.ManifestOpens(s.store)
		for range 100 {
			if r, err := s.store.Root(ctx); err != nil || r != h {
				t.Fatalf("%s reads root %s (%v), want %s", s.who, r.Short(), err, h.Short())
			}
		}
		if n := packstore.ManifestOpens(s.store) - before; n != 0 {
			t.Fatalf("100 reads of a root nobody moved: %s opened the manifest %d times, want 0: every read decrypts and parses a manifest that grows with each publish", s.who, n)
		}
	}
	// Positive control: a root that moved is read, from the new manifest.
	h2, err := writer.Put(ctx, []byte("second"))
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.CompareAndSetRoot(ctx, h, h2); err != nil {
		t.Fatal(err)
	}
	before := packstore.ManifestOpens(reader)
	if r, err := reader.Root(ctx); err != nil || r != h2 {
		t.Fatalf("after the writer's second publish the reader's root = %s (%v), want %s", r.Short(), err, h2.Short())
	}
	if n := packstore.ManifestOpens(reader) - before; n != 1 {
		t.Fatalf("reading a root that moved opened %d manifests, want 1", n)
	}
	if got, err := reader.Get(ctx, h2); err != nil || string(got) != "second" {
		t.Fatalf("the reader cannot read the chunk the moved root names: %q, %v", got, err)
	}
}

// staleRoot answers its first root reads with an older root, as a reader
// does that read the root and then loaded its index objects only after
// newer manifests had replaced them and the replaced ones were deleted.
type staleRoot struct {
	blob.BlobStore
	stale blob.Root
	left  int
}

func (s *staleRoot) Root(ctx context.Context) (blob.Root, error) {
	if s.left > 0 {
		s.left--
		return s.stale, nil
	}
	return s.BlobStore.Root(ctx)
}

// A manifest names index objects that a newer one may replace, and the
// replaced ones are deleted once no manifest names them: by GC a grace
// window after it rewrote them, and at any time once they are orphans. A
// refresh that read the older manifest and then finds one of its index
// objects gone reads the root again and, finding it moved, loads the
// newer manifest instead of failing. A listed object gone from a root that
// has not moved is still an error.
func TestARefreshThatLosesAnIndexObjectToANewerManifestReadsIt(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	hs := published(t, open(t, bs, kr), "a, the root", "b, garbage", "c, live")
	old, err := bs.Root(ctx)
	if err != nil {
		t.Fatal(err)
	}
	replaced := objects(t, bs, "index/")
	live := liveSet(hs[0], hs[2])
	round(t, bs, kr, live, t0)
	round(t, bs, kr, live, t0.Add(time.Hour)) // b's pack expires: the index objects are rewritten
	out := round(t, bs, kr, live, t0.Add(2*time.Hour))
	if len(out.Expired) != len(replaced) {
		t.Fatalf("fixture: the round expired %v, want the %d replaced index objects", out.Expired, len(replaced))
	}
	remove(t, bs, out.Expired)

	s, err := packstore.Open(ctx, packstore.WithBackoff(packstore.Options{Blobs: &staleRoot{BlobStore: bs, stale: old, left: 1}, Keys: kr, Repo: repo}, time.Millisecond))
	if err != nil {
		t.Fatalf("opening a store whose first root read names index objects since replaced and deleted: %v; it must read the newer manifest", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	for _, h := range []hash.Hash{hs[0], hs[2]} {
		if _, err := s.Get(ctx, h); err != nil {
			t.Fatalf("that store cannot read the live chunk %s: %v", h.Short(), err)
		}
	}

	// Positive control: an object the current manifest lists, gone, is an
	// error, whatever the retries.
	remove(t, bs, objects(t, bs, "index/"))
	if _, err := packstore.Open(ctx, packstore.Options{Blobs: bs, Keys: kr, Repo: repo}); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("opening a store whose manifest lists an index object that is gone = %v, want ErrNotFound", err)
	}
}

// Each publish adds an index object, and GC rewrites them only when a pack
// expires or is repacked, so a repository without garbage listed one per
// publish until the manifest refused to grow past 100,000 ("run GC to
// compact"), every refresh and publish costing more on the way. Publishes
// compact the small ones: two writers taking turns over 300 one-chunk
// commits leave the manifest listing a few dozen index objects at most,
// every pack in exactly one of them, and every chunk reads from both
// writers and from a store opened afresh.
func TestPublishesCompactSmallIndexObjects(t *testing.T) {
	const commits, bound = 300, 40
	bs, kr := mem.New(), keyring(t)
	writers := []*packstore.Store{open(t, bs, kr), open(t, bs, kr)}
	o := packstore.Options{Blobs: bs, Keys: kr, Repo: repo}
	var hs []hash.Hash
	root, most := hash.Hash{}, 0
	for i := range commits {
		w := writers[i%2]
		h, err := w.Put(ctx, []byte(fmt.Sprintf("commit %d", i)))
		if err != nil {
			t.Fatal(err)
		}
		if err := w.CompareAndSetRoot(ctx, root, h); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
		root = h
		hs = append(hs, h)
		n, err := packstore.IndexObjects(ctx, o)
		if err != nil {
			t.Fatal(err)
		}
		most = max(most, n)
		if i%37 == 0 { // the other writer reads what this one published, across compactions
			if _, err := writers[(i+1)%2].Get(ctx, h); err != nil {
				t.Fatalf("after commit %d the other writer cannot read it: %v", i, err)
			}
		}
	}
	if most > bound {
		t.Fatalf("%d one-chunk commits: the manifest listed up to %d index objects, want at most %d: small index objects must be compacted as they accumulate", commits, most, bound)
	}
	order, err := packstore.PackOrder(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	listed := map[string]int{}
	for _, p := range order {
		listed[p]++
	}
	for _, p := range objects(t, bs, "packs/") {
		if listed[p] != 1 {
			t.Fatalf("pack %s is listed by %d of the manifest's index objects, want exactly 1", p, listed[p])
		}
	}
	if len(order) != len(listed) {
		t.Fatalf("the manifest's index objects list %d packs, %d of them distinct", len(order), len(listed))
	}
	for _, s := range append(writers, open(t, bs, kr)) {
		for i, h := range hs {
			if got, err := s.Get(ctx, h); err != nil || string(got) != fmt.Sprintf("commit %d", i) {
				t.Fatalf("commit %d reads as %q, %v", i, got, err)
			}
		}
	}
}

// A merged index object lists packs a reader has indexed already, from the
// objects it replaces. The reader indexes each pack once: a reader whose
// memory bound holds every chunk never spills to disk, and one that has
// spilled counts each chunk once, not again in memory beside its table.
func TestAReaderOfCompactedIndexObjectsIndexesEachPackOnce(t *testing.T) {
	const commits = 300
	bs, kr := mem.New(), keyring(t)
	writer := open(t, bs, kr)
	roomy, tight := t.TempDir(), t.TempDir()
	reader := func(dir string, bound int) *packstore.Store {
		s, err := packstore.Open(ctx, packstore.WithBackoff(packstore.Options{Blobs: bs, Keys: kr, Repo: repo, IndexDir: dir, IndexInMemory: bound}, time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	}
	inMemory, spilled := reader(roomy, 2*commits), reader(tight, commits*5/6)
	root := hash.Hash{}
	for i := range commits {
		h, err := writer.Put(ctx, []byte(fmt.Sprintf("commit %d", i)))
		if err != nil {
			t.Fatal(err)
		}
		if err := writer.CompareAndSetRoot(ctx, root, h); err != nil {
			t.Fatal(err)
		}
		root = h
		for _, r := range []*packstore.Store{inMemory, spilled} {
			if got, err := r.Root(ctx); err != nil || got != h {
				t.Fatalf("a reader's root after commit %d = %s (%v), want %s", i, got.Short(), err, h.Short())
			}
		}
	}
	if n := len(tables(t, roomy)); n != 0 {
		t.Fatalf("a reader whose memory bound holds %d chunks spilled %d table(s) for %d: it counted the packs of merged index objects again", 2*commits, n, commits)
	}
	if len(tables(t, tight)) == 0 {
		t.Fatal("fixture: the reader bounded at 250 chunks never spilled")
	}
	st, err := spilled.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.Chunks != commits {
		t.Fatalf("a spilled reader of %d chunks counts %d: it indexed the packs of merged index objects in memory beside its table", commits, st.Chunks)
	}
}

// Every publish leaves the store to start a new pending pack, and a pending
// pack's buffer was allocated at the pack's size (#10), 32 MiB zeroed for a
// one-row commit: about 3 ms of a commit on memory. A pending pack starts
// at about what the last one held and grows to the pack's size as it
// fills, so a store making small commits allocates for small packs.
func TestSmallCommitsAllocateForSmallPacks(t *testing.T) {
	const commits = 20
	s := open(t, mem.New(), keyring(t))
	root, err := s.Put(ctx, []byte("warm up"))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CompareAndSetRoot(ctx, hash.Hash{}, root); err != nil {
		t.Fatal(err)
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := range commits {
		h, err := s.Put(ctx, []byte(fmt.Sprintf("one small row %d", i)))
		if err != nil {
			t.Fatal(err)
		}
		if err := s.CompareAndSetRoot(ctx, root, h); err != nil {
			t.Fatal(err)
		}
		root = h
	}
	runtime.ReadMemStats(&after)
	if per := (after.TotalAlloc - before.TotalAlloc) / commits; per > 2<<20 {
		t.Fatalf("a one-chunk commit allocates %d KiB, want under 2 MiB: its pending pack is sized at the pack (%d MiB), not at what it holds", per>>10, packstore.DefaultPackSize>>20)
	}
}
