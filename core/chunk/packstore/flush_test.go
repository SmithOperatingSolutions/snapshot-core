package packstore_test

import (
	"bytes"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/mem"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/packstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
)

// #10: Flush hands the pending pack to a finisher when it holds at least
// an eighth of a pack (4 KiB of the 32 KiB packs here): the pack reaches
// the backend with no publish. A smaller pending pack waits for the
// publish and shares its pack with what comes after the flush: two 1 KiB
// chunks prepared and put after it make one pack at the publish, not
// two, and read back. A closed store refuses to flush.
func TestFlushUploadsAPackWorthAnUpload(t *testing.T) {
	g := newGated(t, mem.New())
	g.release() // nothing is held: landedPacks is the signal
	s := gatedStore(t, g)
	if err := s.Flush(ctx); err != nil {
		t.Fatalf("flushing a store with nothing pending: %v", err)
	}
	chunks, hs := chunksOf("flushed", 1)
	if _, err := s.Put(ctx, chunks[0]); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	g.landedPacks(t, 1) // the 4 KiB chunk's pack, with no publish
	small := [][]byte{bytes.Repeat([]byte{1}, 1<<10), bytes.Repeat([]byte{2}, 1<<10)}
	for _, c := range small { // stored as a stream's pipeline stores: prepared, then put into the pack after the flush
		p, err := s.Prepare(c)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.PutPrepared(ctx, p); err != nil {
			t.Fatal(err)
		}
		if err := s.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.CompareAndSetRoot(ctx, hash.Hash{}, hs[0]); err != nil {
		t.Fatal(err)
	}
	if n := len(objects(t, g, "packs/")); n != 2 {
		t.Fatalf("a flushed 4 KiB chunk, two flushed 1 KiB chunks and a publish left %d packs, want 2: a pending pack under an eighth of a pack waits for the publish", n)
	}
	for _, h := range append(hs, hash.Sum(small[0]), hash.Sum(small[1])) {
		if _, err := s.Get(ctx, h); err != nil {
			t.Fatalf("a flushed chunk does not read back: %v", err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(ctx); !errors.Is(err, chunk.ErrClosed) {
		t.Fatalf("Flush on a closed store = %v, want ErrClosed", err)
	}
}

// An index bound below zero is a mistake, not a bound: Open refuses it,
// and opens on zero (the default) and on one.
func TestOpenRefusesANegativeIndexBound(t *testing.T) {
	for _, bound := range []int{0, 1} {
		s, err := packstore.Open(ctx, packstore.Options{Blobs: mem.New(), Keys: keyring(t), Repo: repo, IndexInMemory: bound})
		if err != nil {
			t.Fatalf("positive control: opening with %d chunks indexed in memory: %v", bound, err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	_, err := packstore.Open(ctx, packstore.Options{Blobs: mem.New(), Keys: keyring(t), Repo: repo, IndexInMemory: -1})
	if err == nil || !bytes.Contains([]byte(err.Error()), []byte("negative")) {
		t.Fatalf("opening with -1 chunks indexed in memory = %v, want a refusal naming the negative bound", err)
	}
}

// Stats count the chunks of a pack being finished with the store's: a
// host reading Stats while its packs are on their way to the backend sees
// every chunk it put, and the same count once they have landed.
func TestStatsCountAPackBeingFinished(t *testing.T) {
	g := newGated(t, mem.New())
	g.release()
	s := gatedStore(t, g)
	held := make(chan struct{})
	release := make(chan struct{})
	var once, freed sync.Once
	free := func() { freed.Do(func() { close(release) }) }
	t.Cleanup(free)
	packstore.HoldFinish(s, func() { // both full packs' finishers hold; neither waits for a slot
		once.Do(func() { close(held) })
		<-release
	})
	chunks, _ := chunksOf("counted", 16) // two full packs and a pending one
	for _, c := range chunks {
		if _, err := s.Put(ctx, c); err != nil {
			t.Fatal(err)
		}
	}
	<-held
	st, err := s.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.Chunks != 16 {
		t.Fatalf("with two packs being finished Stats count %d chunks of the 16 put: a chunk on its way to the backend is stored", st.Chunks)
	}
	free()
	packstore.WaitUploads(s)
	if st, err = s.Stats(ctx); err != nil || st.Chunks != 16 {
		t.Fatalf("once the packs landed Stats count %d chunks, %v; want 16", st.Chunks, err)
	}
}

// What Prepare hands back knows the chunk's hash and length: the pipeline
// grows its index from them without the bytes.
func TestAPreparedChunkKnowsItsHashAndLength(t *testing.T) {
	s := smallPacks(t, mem.New())
	data := bytes.Repeat([]byte("prepared "), 300)
	p, err := s.Prepare(data)
	if err != nil {
		t.Fatal(err)
	}
	if p.Hash() != hash.Sum(data) || p.Len() != len(data) {
		t.Fatalf("a prepared chunk of %d bytes reports hash %s and %d bytes, want %s and %d", len(data), p.Hash().Short(), p.Len(), hash.Sum(data).Short(), len(data))
	}
}

// A pack uploaded beside the writer and not yet published stays in the
// index through a refresh, whether the refresh rebuilds the index in
// memory or spills it: here another store has published more than the
// in-memory bound since, so a read of a hash this store does not know
// rebuilds its index on disk from the newer manifest, which does not list
// the pack, and the pack's chunks read afterwards as before; a
// refresh that finds the manifest unchanged keeps them too. (Has of an
// unknown hash refreshes; Get of one does not.)
func TestUnpublishedPacksSurviveARefresh(t *testing.T) {
	bs := mem.New()
	kr := keyring(t)
	spilling := func() *packstore.Store {
		t.Helper()
		s, err := packstore.Open(ctx, packstore.WithBackoff(packstore.Options{Blobs: bs, Keys: kr, Repo: repo, PackSize: 32 << 10, IndexInMemory: 4, IndexDir: t.TempDir(), CacheBytes: -1}, time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	}
	mine := spilling()
	published, hs := chunksOf("published", 16) // four times the in-memory bound: the index spills
	if err := <-putAll(mine, published); err != nil {
		t.Fatal(err)
	}
	if err := mine.CompareAndSetRoot(ctx, hash.Hash{}, hs[0]); err != nil {
		t.Fatal(err)
	}
	theirs := spilling()
	others, os := chunksOf("theirs", 8) // twice the bound: loading their index object spills ours
	if err := <-putAll(theirs, others); err != nil {
		t.Fatal(err)
	}
	other, err := theirs.Put(ctx, []byte("theirs"))
	if err != nil {
		t.Fatal(err)
	}
	if err := theirs.CompareAndSetRoot(ctx, hs[0], other); err != nil {
		t.Fatal(err)
	}
	unpublished, us := chunksOf("unpublished", 16) // two packs upload, one stays pending
	if err := <-putAll(mine, unpublished); err != nil {
		t.Fatal(err)
	}
	packstore.WaitUploads(mine)
	for round, never := range []string{"never stored", "nor this"} { // the first refresh finds a newer manifest, the second the same
		have, err := mine.Has(ctx, []hash.Hash{hash.Sum([]byte(never))}) // a miss refreshes
		if err != nil || len(have) != 1 || have[hash.Sum([]byte(never))] {
			t.Fatalf("positive control: Has of a hash never stored = %v, %v; want false after a refresh", have, err)
		}
		for i, h := range us {
			if got, err := mine.Get(ctx, h); err != nil || !bytes.Equal(got, unpublished[i]) {
				t.Fatalf("after refresh %d, chunk %d of an uploaded, unpublished pack reads as %d bytes, %v; want its 4 KiB", round+1, i, len(got), err)
			}
		}
		if got, err := mine.Get(ctx, other); err != nil || string(got) != "theirs" {
			t.Fatalf("after refresh %d, the other store's chunk reads as %q, %v", round+1, got, err)
		}
		if got, err := mine.Get(ctx, os[7]); err != nil || !bytes.Equal(got, others[7]) {
			t.Fatalf("after refresh %d, the other store's last chunk reads as %d bytes, %v", round+1, len(got), err)
		}
	}
}

// Has and Get see what another store published since this one last read
// the manifest: a miss refreshes, once, and finds it. What no store has
// stays absent, beside what this store holds.
func TestAMissSeesAnotherStoresPublish(t *testing.T) {
	bs := mem.New()
	kr := keyring(t)
	mine, theirs := open(t, bs, kr), open(t, bs, kr)
	own, err := mine.Put(ctx, []byte("mine"))
	if err != nil {
		t.Fatal(err)
	}
	other, err := theirs.Put(ctx, []byte("theirs"))
	if err != nil {
		t.Fatal(err)
	}
	if err := theirs.CompareAndSetRoot(ctx, hash.Hash{}, other); err != nil {
		t.Fatal(err)
	}
	never := hash.Sum([]byte("nobody's"))
	have, err := mine.Has(ctx, []hash.Hash{own, other, never})
	if err != nil {
		t.Fatal(err)
	}
	if !have[own] || !have[other] || have[never] {
		t.Fatalf("Has = mine %t, theirs %t, nobody's %t; want true, true, false: a miss reads the manifest another store published", have[own], have[other], have[never])
	}
	if got, err := mine.Get(ctx, other); err != nil || string(got) != "theirs" {
		t.Fatalf("Get of the other store's chunk = %q, %v; want its content", got, err)
	}
}
