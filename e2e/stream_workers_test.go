package e2e_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"runtime"
	"testing"
	"testing/iotest"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/mem"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/packstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
	"github.com/SmithOperatingSolutions/snapshot-core/core/stream"
)

// packStore is a store that prepares chunks (chunk.Preparer), so a write
// over it takes the worker pipeline; memstore does not, and takes the
// serial path.
func packStore(t *testing.T) *packstore.Store {
	t.Helper()
	kr, err := seal.NewKeyring()
	if err != nil {
		t.Fatal(err)
	}
	s, err := packstore.Open(context.Background(), packstore.Options{Blobs: mem.New(), Keys: kr, Repo: seal.RepoID{0x57}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// #10: the stream is the same at any worker count: the same root over the
// same bytes on one worker (the serial path), two, and every core, so a
// host may pick any count and deduplicate against any other's writes.
func TestTheStreamIsTheSameAtAnyWorkerCount(t *testing.T) {
	data := sampleBytes(4 << 20)
	var roots []hash.Hash
	for _, workers := range []int{1, 2, 0} {
		cfg := stream.DefaultConfig()
		cfg.Workers = workers
		ref, err := stream.Write(context.Background(), packStore(t), bytes.NewReader(data), cfg)
		if err != nil {
			t.Fatalf("%d workers: %v", workers, err)
		}
		roots = append(roots, ref.Root)
		if ref.Size != uint64(len(data)) {
			t.Fatalf("%d workers: the stream is %d bytes, want %d", workers, ref.Size, len(data))
		}
	}
	if roots[1] != roots[0] || roots[2] != roots[0] {
		t.Fatalf("the same bytes gave roots %s (one worker), %s (two), %s (%d): the pipeline changed the stream", roots[0].Short(), roots[1].Short(), roots[2].Short(), runtime.GOMAXPROCS(0))
	}
}

// A source that fails midway fails the write with the source's error, on
// the pipeline as on the serial path, and every goroutine the pipeline
// started is gone once it returns.
func TestAWorkersWriteSurfacesTheSourcesError(t *testing.T) {
	data := sampleBytes(2 << 20)
	before := runtime.NumGoroutine()
	for _, workers := range []int{1, 0} {
		cfg := stream.DefaultConfig()
		cfg.Workers = workers
		_, err := stream.Write(context.Background(), packStore(t), iotest.TimeoutReader(bytes.NewReader(data)), cfg)
		if !errors.Is(err, iotest.ErrTimeout) {
			t.Fatalf("%d workers: a source failing after its first read gave %v, want the source's error", workers, err)
		}
	}
	waitForGoroutines(t, before)
}

// A store that fails a put midway fails the write with the store's error,
// and the pipeline winds down.
func TestAWorkersWriteSurfacesTheStoresError(t *testing.T) {
	data := sampleBytes(2 << 20)
	before := runtime.NumGoroutine()
	fs := &failingPuts{Store: packStore(t), failAt: 5}
	_, err := stream.Write(context.Background(), fs, bytes.NewReader(data), stream.DefaultConfig())
	if !errors.Is(err, errPut) {
		t.Fatalf("a store failing its fifth put gave %v, want the store's error", err)
	}
	if fs.puts.Load() != 5 {
		t.Fatalf("after the fifth put failed the pipeline went on to put %d chunks", fs.puts.Load())
	}
	waitForGoroutines(t, before)
}

var errPut = errors.New("injected: the store refuses")

// failingPuts is a Preparer whose failAt'th PutPrepared fails.
type failingPuts struct {
	*packstore.Store
	failAt int64
	puts   atomicCounter
}

func (f *failingPuts) PutPrepared(ctx context.Context, p chunk.Prepared) (hash.Hash, error) {
	if f.puts.Add(1) >= f.failAt {
		return hash.Hash{}, errPut
	}
	return f.Store.PutPrepared(ctx, p)
}

type atomicCounter struct{ n int64 }

func (c *atomicCounter) Add(d int64) int64 { c.n += d; return c.n } // PutPrepared is called from one goroutine
func (c *atomicCounter) Load() int64       { return c.n }

// waitForGoroutines waits for the goroutine count to fall back to before,
// as a pipeline winding down needs a moment after Write returns; it fails
// if it does not within two seconds.
func waitForGoroutines(t *testing.T, before int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			t.Fatalf("%d goroutines two seconds after the writes returned, %d before them: the pipeline leaks", runtime.NumGoroutine(), before)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// sampleBytes is n bytes of SHA-256 in counter mode: incompressible, and
// the same every run.
func sampleBytes(n int) []byte {
	out := make([]byte, 0, n+sha256.Size)
	var ctr [8]byte
	for i := uint64(0); len(out) < n; i++ {
		binary.BigEndian.PutUint64(ctr[:], i)
		h := sha256.Sum256(append([]byte("stream/workers"), ctr[:]...))
		out = append(out, h[:]...)
	}
	return out[:n]
}
