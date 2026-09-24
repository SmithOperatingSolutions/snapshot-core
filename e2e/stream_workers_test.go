package e2e_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"runtime"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/mem"
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
