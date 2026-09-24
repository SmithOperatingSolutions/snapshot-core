//go:build slow

package cdc_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"runtime"
	"testing"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/cdc"
)

// corpusBytes is n bytes of SHA-256 in counter mode.
func corpusBytes(n int) []byte {
	out := make([]byte, 0, n+sha256.Size)
	var ctr [8]byte
	for i := uint64(0); len(out) < n; i++ {
		binary.BigEndian.PutUint64(ctr[:], i)
		h := sha256.Sum256(append([]byte("cdc/parallel"), ctr[:]...))
		out = append(out, h[:]...)
	}
	return out[:n]
}

// cutLengths returns every chunk's length and the error the chunker stops with.
func cutLengths(next func() ([]byte, error)) ([]int, error) {
	var lens []int
	for {
		data, err := next()
		if err != nil {
			return lens, err
		}
		lens = append(lens, len(data))
	}
}

// #10: the Buzhash at a byte depends on the 48 bytes before it alone, so
// the hashing spreads over cores while one goroutine places the cuts. On a
// machine with at least four threads the parallel chunker cuts 256 MiB into
// the same chunks as the serial one at least twice as fast, same machine,
// same run.
func TestSlowParallelCuttingOutrunsTheChunker(t *testing.T) {
	if n := runtime.GOMAXPROCS(0); n < 4 {
		t.Fatalf("this machine gives Go %d threads; the comparison needs at least 4", n)
	}
	data := corpusBytes(256 << 20)
	g := cdc.DefaultGeometry()
	c, err := cdc.New(bytes.NewReader(data), g)
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Now()
	serial, err := cutLengths(c.Next)
	if err != io.EOF {
		t.Fatal(err)
	}
	serialTime := time.Since(t0)
	p, err := cdc.NewParallel(bytes.NewReader(data), g, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	t0 = time.Now()
	parallel, err := cutLengths(p.Next)
	if err != io.EOF {
		t.Fatal(err)
	}
	parallelTime := time.Since(t0)
	t.Logf("256 MiB: serial %v (%.0f MB/s, %d chunks), parallel on %d %v (%.0f MB/s, %d chunks)",
		serialTime.Round(time.Millisecond), 256*1.048576e6/1e6/serialTime.Seconds(), len(serial),
		runtime.GOMAXPROCS(0), parallelTime.Round(time.Millisecond), 256*1.048576e6/1e6/parallelTime.Seconds(), len(parallel))
	if len(parallel) != len(serial) {
		t.Fatalf("the parallel chunker cut %d chunks, the serial one %d", len(parallel), len(serial))
	}
	for i := range serial {
		if parallel[i] != serial[i] {
			t.Fatalf("chunk %d is %d bytes from the parallel chunker, %d from the serial one", i, parallel[i], serial[i])
		}
	}
	if parallelTime*2 > serialTime {
		t.Fatalf("the parallel chunker took %v against the serial one's %v: under twice as fast", parallelTime.Round(time.Millisecond), serialTime.Round(time.Millisecond))
	}
}
