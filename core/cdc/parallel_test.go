package cdc_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"runtime"
	"testing"
	"testing/iotest"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/cdc"
)

// sampleBytes is n bytes of SHA-256 in counter mode.
func sampleBytes(n int) []byte {
	out := make([]byte, 0, n+sha256.Size)
	var ctr [8]byte
	for i := uint64(0); len(out) < n; i++ {
		binary.BigEndian.PutUint64(ctr[:], i)
		h := sha256.Sum256(append([]byte("cdc/parallel"), ctr[:]...))
		out = append(out, h[:]...)
	}
	return out[:n]
}

// chunksAndErr returns every chunk a chunker cuts and the error it stops with.
func chunksAndErr(next func() ([]byte, error)) ([][]byte, error) {
	var out [][]byte
	for {
		data, err := next()
		if err != nil {
			return out, err
		}
		out = append(out, data)
	}
}

// #10: the parallel chunker cuts where the serial one cuts, byte for byte,
// at one worker and at many, over random, zero, repeating and tiny
// streams, at both geometries, however the reader hands the bytes over,
// and streams that straddle its blocks; it stops with the same error at
// the same chunk when a read fails or makes no progress, and leaves no
// goroutine behind once closed.
func TestTheParallelChunkerCutsWhereTheChunkerCuts(t *testing.T) {
	random := sampleBytes(3 << 20)
	var zeros [1 << 20]byte
	repeating := bytes.Repeat([]byte("abcabcabd"), 100_000)
	streams := map[string][]byte{
		"random 3 MiB": random, "zeros 1 MiB": zeros[:], "repeating 900 kB": repeating,
		"empty": nil, "one byte": random[:1], "a window": random[:48], "a window and one": random[:49],
		"a block": random[:256<<10], "a block and one": random[:256<<10+1], "a block less one": random[:256<<10-1],
		"exactly Min": random[:16<<10], "exactly Max": random[:512<<10],
	}
	readers := map[string]func([]byte) io.Reader{
		"whole":         func(b []byte) io.Reader { return bytes.NewReader(b) },
		"halves":        func(b []byte) io.Reader { return iotest.HalfReader(bytes.NewReader(b)) },
		"data with EOF": func(b []byte) io.Reader { return iotest.DataErrReader(bytes.NewReader(b)) },
		"one byte":      func(b []byte) io.Reader { return iotest.OneByteReader(bytes.NewReader(b)) },
	}
	before := runtime.NumGoroutine()
	for gName, g := range map[string]cdc.Geometry{"repo": cdc.DefaultGeometry(), "fine": {Min: 4 << 10, Max: 64 << 10, Mask: 0x1FFF}} {
		for sName, data := range streams {
			for rName, mk := range readers {
				if rName == "one byte" && len(data) > 64<<10 {
					continue
				}
				c, err := cdc.New(mk(data), g)
				if err != nil {
					t.Fatal(err)
				}
				want, wantErr := chunksAndErr(c.Next)
				for _, workers := range []int{1, 0} {
					p, err := cdc.NewParallel(mk(data), g, workers)
					if err != nil {
						t.Fatal(err)
					}
					got, gotErr := chunksAndErr(p.Next)
					p.Close()
					if !errors.Is(gotErr, io.EOF) || !errors.Is(wantErr, io.EOF) {
						t.Fatalf("%s, %s, read %s, %d workers: stopped with %v, the chunker with %v", gName, sName, rName, workers, gotErr, wantErr)
					}
					if len(got) != len(want) {
						t.Fatalf("%s, %s, read %s, %d workers: %d chunks, the chunker's %d", gName, sName, rName, workers, len(got), len(want))
					}
					for i := range want {
						if !bytes.Equal(got[i], want[i]) {
							t.Fatalf("%s, %s, read %s, %d workers: chunk %d is %d bytes, the chunker's %d, or differs", gName, sName, rName, workers, i, len(got[i]), len(want[i]))
						}
					}
				}
			}
		}
	}
	g := cdc.DefaultGeometry()
	// The byte that makes a chunk exactly Min long is judged by the easy
	// mask: a stream is drawn in which the serial chunker cuts a chunk of
	// exactly Min, and the parallel one must cut it too.
	fine := cdc.Geometry{Min: 4 << 10, Max: 64 << 10, Mask: 0x1FFF}
	atMin := streamWithAChunkOfMin(t, sampleBytes(32<<20), fine)
	c, _ := cdc.New(bytes.NewReader(atMin), fine)
	want, wantErr := chunksAndErr(c.Next)
	p, _ := cdc.NewParallel(bytes.NewReader(atMin), fine, 0)
	got, gotErr := chunksAndErr(p.Next)
	p.Close()
	if !errors.Is(gotErr, io.EOF) || !errors.Is(wantErr, io.EOF) || len(got) != len(want) {
		t.Fatalf("a chunk of exactly Min: parallel %d chunks (%v), the chunker %d (%v)", len(got), gotErr, len(want), wantErr)
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("a chunk of exactly Min: chunk %d is %d bytes, the chunker's %d", i, len(got[i]), len(want[i]))
		}
	}
	// A read that fails after some bytes: the same chunks before it, the
	// same error, the chunk under way dropped.
	failing := func() io.Reader { return iotest.TimeoutReader(bytes.NewReader(random[:200<<10])) }
	c, _ = cdc.New(failing(), g)
	want, wantErr = chunksAndErr(c.Next)
	p, _ = cdc.NewParallel(failing(), g, 0)
	got, gotErr = chunksAndErr(p.Next)
	p.Close()
	if !errors.Is(gotErr, iotest.ErrTimeout) || !errors.Is(wantErr, iotest.ErrTimeout) || len(got) != len(want) {
		t.Fatalf("a failing read: parallel %d chunks and %v, the chunker %d and %v", len(got), gotErr, len(want), wantErr)
	}
	p, _ = cdc.NewParallel(noProgress{}, g, 0)
	if _, err := p.Next(); !errors.Is(err, io.ErrNoProgress) {
		t.Fatalf("a reader returning (0, nil) forever: %v, want io.ErrNoProgress", err)
	}
	p.Close()
	// A chunk is the caller's: cutting on does not alter one returned.
	p, _ = cdc.NewParallel(bytes.NewReader(random), g, 0)
	first, err := p.Next()
	if err != nil {
		t.Fatal(err)
	}
	keep := append([]byte(nil), first...)
	if _, err := chunksAndErr(p.Next); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	p.Close()
	if !bytes.Equal(first, keep) {
		t.Fatal("the first chunk changed while the chunker cut on")
	}
	// Abandoned midway and closed: its goroutines end.
	p, _ = cdc.NewParallel(bytes.NewReader(random), g, 0)
	if _, err := p.Next(); err != nil {
		t.Fatal(err)
	}
	p.Close()
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			t.Fatalf("%d goroutines two seconds after every parallel chunker was closed, %d before: they leak", runtime.NumGoroutine(), before)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

type noProgress struct{}

func (noProgress) Read([]byte) (int, error) { return 0, nil }

// streamWithAChunkOfMin is a prefix of data in which the serial chunker
// cuts a chunk of exactly g.Min bytes; the test fails if data has none.
func streamWithAChunkOfMin(t *testing.T, data []byte, g cdc.Geometry) []byte {
	t.Helper()
	c, err := cdc.New(bytes.NewReader(data), g)
	if err != nil {
		t.Fatal(err)
	}
	off := 0
	for {
		chunk, err := c.Next()
		if err != nil {
			t.Fatalf("positive control: %d MiB of corpus never cuts a chunk of exactly Min (%v)", len(data)>>20, err)
		}
		off += len(chunk)
		if len(chunk) == g.Min {
			return data[:min(off+1000, len(data))]
		}
	}
}

// firstSet finds the first set bit in [from, to) and none past to, even in
// the word to falls in.
func TestFirstSetStaysWithinItsBounds(t *testing.T) {
	bm := make([]uint64, 4)
	bm[1] |= 1 << 10 // bit 74
	bm[2] |= 1 << 0  // bit 128
	for _, tc := range []struct{ from, to, want int }{
		{0, 256, 74}, {74, 256, 74}, {75, 256, 128}, {0, 74, -1}, {0, 75, 74}, {70, 73, -1}, {129, 256, -1}, {128, 129, 128},
	} {
		if got := cdc.FirstSet(bm, tc.from, tc.to); got != tc.want {
			t.Fatalf("firstSet(bits 74 and 128, %d, %d) = %d, want %d", tc.from, tc.to, got, tc.want)
		}
	}
}

// Close stops a parallel chunker nobody is reading from, whose reader has
// filled every slot ahead and is waiting to hand over the next block: the
// wait ends, and every goroutine is gone.
func TestCloseStopsAParallelChunkerNobodyReads(t *testing.T) {
	before := runtime.NumGoroutine()
	data := sampleBytes(4 * 64 << 10) // four blocks: more than one worker's two slots ahead
	p, err := cdc.NewParallel(&thenBlocks{r: bytes.NewReader(data)}, cdc.DefaultGeometry(), 1)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // the reader reaches the block it cannot hand over; nothing here depends on it
	p.Close()
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			t.Fatalf("%d goroutines two seconds after Close, %d before: the reader, stuck on a slot nobody frees, did not stop", runtime.NumGoroutine(), before)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// thenBlocks serves r, then blocks forever instead of returning EOF.
type thenBlocks struct{ r io.Reader }

func (b *thenBlocks) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	if err == io.EOF {
		select {}
	}
	return n, err
}
