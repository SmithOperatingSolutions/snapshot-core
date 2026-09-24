package packstore_test

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/mem"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/packstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
)

// gatedPacks is a blob store whose pack puts wait at a gate until released,
// recording how many wait at once and the order of what lands.
type gatedPacks struct {
	blob.BlobStore
	mu      sync.Mutex
	gate    chan struct{} // closed to release every waiting put
	hold    int           // pack puts the gate holds; the rest pass (0: every one)
	held    int
	waiting int
	peak    int
	landed  []string // pack and root events in the order they completed
}

func newGated(bs blob.BlobStore) *gatedPacks {
	return &gatedPacks{BlobStore: bs, gate: make(chan struct{})}
}

func (g *gatedPacks) Put(ctx context.Context, name string, r io.Reader, size int64) error {
	g.mu.Lock()
	holdThis := strings.HasPrefix(name, "packs/") && (g.hold == 0 || g.held < g.hold)
	if holdThis {
		g.held++
	}
	g.mu.Unlock()
	if holdThis {
		g.mu.Lock()
		g.waiting++
		g.peak = max(g.peak, g.waiting)
		g.mu.Unlock()
		<-g.gate
		g.mu.Lock()
		g.waiting--
		g.mu.Unlock()
	}
	err := g.BlobStore.Put(ctx, name, r, size)
	g.mu.Lock()
	g.landed = append(g.landed, name)
	g.mu.Unlock()
	return err
}

func (g *gatedPacks) SwapRoot(ctx context.Context, expected blob.Version, next []byte) (blob.Version, error) {
	v, err := g.BlobStore.SwapRoot(ctx, expected, next)
	g.mu.Lock()
	g.landed = append(g.landed, "root")
	g.mu.Unlock()
	return v, err
}

// waitingPuts blocks until n pack puts wait at the gate.
func (g *gatedPacks) waitingPuts(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		g.mu.Lock()
		w := g.waiting
		g.mu.Unlock()
		if w >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("after ten seconds %d pack uploads wait at the gate, want %d", w, n)
		}
		time.Sleep(time.Millisecond)
	}
}

// smallPacks opens a store whose packs hold a few 4 KiB chunks.
func smallPacks(t *testing.T, bs blob.BlobStore) *packstore.Store {
	t.Helper()
	s, err := packstore.Open(ctx, packstore.WithBackoff(packstore.Options{Blobs: bs, Keys: keyring(t), Repo: repo, PackSize: 32 << 10}, time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// chunksOf is n distinct 4 KiB chunks and their hashes.
func chunksOf(seed string, n int) ([][]byte, []hash.Hash) {
	var data [][]byte
	var hs []hash.Hash
	for i := 0; i < n; i++ {
		c := payload(seed+"/"+string(rune('a'+i%26))+string(rune('a'+i/26)), 4<<10)
		data = append(data, c)
		hs = append(hs, hash.Sum(c))
	}
	return data, hs
}

// putAll stores chunks on a goroutine and returns a channel closed when
// every put has returned, or carrying the first error.
func putAll(s *packstore.Store, chunks [][]byte) <-chan error {
	done := make(chan error, 1)
	go func() {
		for _, c := range chunks {
			if _, err := s.Put(ctx, c); err != nil {
				done <- err
				return
			}
		}
		close(done)
	}()
	return done
}

// #10 (A): a full pack is finished and uploaded beside the writer, not in
// its way. With the first pack's upload held at the backend, the writer
// goes on storing chunks for the pack after it, and a chunk of the held
// pack still reads, from memory. Released, everything lands and publishes.
func TestAnUploadDoesNotHoldUpTheWriter(t *testing.T) {
	g := newGated(mem.New())
	s := smallPacks(t, g)
	chunks, hs := chunksOf("held", 16) // two 32 KiB packs' worth: the first is held
	done := putAll(s, chunks)
	g.waitingPuts(t, 1)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("with the first pack's upload held, storing the chunks after it did not return in five seconds: the writer waits on its uploads")
	}
	if got, err := s.Get(ctx, hs[0]); err != nil || len(got) != 4<<10 {
		t.Fatalf("a chunk of the pack being uploaded reads as %d bytes, %v; want it from memory", len(got), err)
	}
	close(g.gate)
	if err := s.CompareAndSetRoot(ctx, hash.Hash{}, hs[0]); err != nil {
		t.Fatalf("publishing once the uploads are released: %v", err)
	}
	fresh := open(t, g, keyring(t))
	for _, h := range hs {
		if _, err := fresh.Get(ctx, h); err != nil {
			t.Fatalf("after the publish a fresh store cannot read %s: %v", h.Short(), err)
		}
	}
}

// Memory stays bounded: with every upload held, at most two packs are in
// flight; a writer with a third full pack waits for one to land.
func TestAtMostTwoPacksAreInFlight(t *testing.T) {
	g := newGated(mem.New())
	s := smallPacks(t, g)
	chunks, _ := chunksOf("many", 48) // six packs' worth
	done := putAll(s, chunks)
	g.waitingPuts(t, 2)
	select {
	case <-done:
		t.Fatal("with every upload held the writer stored six packs' worth without waiting: nothing bounds the packs in flight")
	case <-time.After(200 * time.Millisecond):
	}
	close(g.gate)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	g.mu.Lock()
	peak := g.peak
	g.mu.Unlock()
	if peak != 2 {
		t.Fatalf("%d pack uploads were in flight at once, want two: the bound", peak)
	}
}

// A publish waits for every upload: the root is swapped only once each
// pack the chunks are in has landed.
func TestAPublishWaitsForItsUploads(t *testing.T) {
	g := newGated(mem.New())
	g.hold = 1 // the first pack alone is held; the publish's own upload passes
	s := smallPacks(t, g)
	chunks, hs := chunksOf("published", 16)
	if err := <-putAll(s, chunks); err != nil {
		t.Fatal(err)
	}
	go func() {
		g.waitingPuts(t, 1)
		time.Sleep(50 * time.Millisecond) // the swap must not come before the release
		close(g.gate)
	}()
	if err := s.CompareAndSetRoot(ctx, hash.Hash{}, hs[0]); err != nil {
		t.Fatal(err)
	}
	// Every pack lands eventually, held or not; the question is whether the
	// root came before one of them.
	landed := g.landedPacks(t, 3)
	root := -1
	for i, e := range landed {
		switch {
		case e == "root":
			root = i
		case strings.HasPrefix(e, "packs/") && root >= 0:
			t.Fatalf("pack %s landed after the root was swapped: the publish did not wait for its uploads (%v)", e, landed)
		}
	}
	if root < 0 {
		t.Fatalf("positive control: no root in %v", landed)
	}
}

// landedPacks waits until n packs have landed and returns every event so far.
func (g *gatedPacks) landedPacks(t *testing.T, n int) []string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		g.mu.Lock()
		packs := 0
		for _, e := range g.landed {
			if strings.HasPrefix(e, "packs/") {
				packs++
			}
		}
		landed := append([]string(nil), g.landed...)
		g.mu.Unlock()
		if packs >= n {
			return landed
		}
		if time.Now().After(deadline) {
			t.Fatalf("ten seconds on, %d packs have landed, want %d: %v", packs, n, landed)
		}
		time.Sleep(time.Millisecond)
	}
}

// A chunk of a pack being finished (named and built, before its upload
// even starts) reads from the unfinished pack: the finisher is held at
// that point and the read must not wait for it.
func TestAChunkOfAPackBeingFinishedReads(t *testing.T) {
	g := newGated(mem.New())
	close(g.gate) // uploads pass; the hold is on the finish
	s := smallPacks(t, g)
	held := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	packstore.HoldFinish(s, func() { // every finisher waits; the first one's arrival is the signal
		once.Do(func() { close(held) })
		<-release
	})
	chunks, hs := chunksOf("finishing", 16)
	done := putAll(s, chunks)
	<-held
	got, err := s.Get(ctx, hs[0])
	if err != nil || len(got) != 4<<10 {
		t.Fatalf("a chunk of a pack being finished reads as %d bytes, %v; want it from the unfinished pack", len(got), err)
	}
	if _, err := s.Put(ctx, chunks[0]); err != nil {
		t.Fatalf("storing a chunk of a pack being finished again: %v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := s.CompareAndSetRoot(ctx, hash.Hash{}, hs[0]); err != nil {
		t.Fatal(err)
	}
	if n := len(objects(t, g, "packs/")); n != 3 {
		t.Fatalf("after storing a chunk of a finishing pack again the store holds %d packs, want the three sixteen chunks make: the chunk must be found there, not stored twice", n)
	}
}

// A chunk is sealed for the pack pending when it is prepared (#10, B). If
// that pack has moved on by the time the chunk is stored, the frame is
// sealed again for the pack it lands in: every chunk reads back whatever
// pack it was prepared for.
func TestAChunkPreparedForOnePackStoresInTheNext(t *testing.T) {
	bs := mem.New()
	s := smallPacks(t, bs)
	chunks, hs := chunksOf("prepared ahead", 24) // three packs' worth, all sealed for the first
	for i := range chunks {                      // half compress, so the payload kept for the re-seal is the compressed one
		if i%2 == 0 {
			chunks[i] = bytes.Repeat([]byte{byte('a' + i)}, 4<<10)
			hs[i] = hash.Sum(chunks[i])
		}
	}
	var prepared []chunk.Prepared
	for _, c := range chunks {
		p, err := s.Prepare(c)
		if err != nil {
			t.Fatal(err)
		}
		prepared = append(prepared, p)
	}
	for i, p := range prepared {
		if h, err := s.PutPrepared(ctx, p); err != nil || h != hs[i] {
			t.Fatalf("storing chunk %d prepared ahead: %s, %v", i, h.Short(), err)
		}
	}
	if err := s.CompareAndSetRoot(ctx, hash.Hash{}, hs[0]); err != nil {
		t.Fatal(err)
	}
	if n := len(objects(t, bs, "packs/")); n < 2 {
		t.Fatalf("positive control: %d packs, want the chunks to have rolled over into another", n)
	}
	fresh := open(t, bs, keyring(t))
	for i, h := range hs {
		got, err := fresh.Get(ctx, h)
		if err != nil || !bytes.Equal(got, chunks[i]) {
			t.Fatalf("chunk %d, prepared for the first pack and stored in a later one, reads as %d bytes, %v", i, len(got), err)
		}
	}
}
