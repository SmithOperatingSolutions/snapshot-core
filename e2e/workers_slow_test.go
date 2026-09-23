//go:build slow

package e2e_test

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/mem"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/packstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/repo"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
	"github.com/SmithOperatingSolutions/snapshot-core/model/blob"
)

// #10: writing hashes, compresses and seals every chunk, and on one
// goroutine that is what bounds a write, not the disk. With the store's
// cores put to work the same write goes at least one and a half times as
// fast as on one worker, and produces the same stream: the same root hash,
// so the same chunks, cut and hashed the same way whatever the worker
// count. Measured on the same machine in the same run, so the verdict
// does not depend on the machine.
func TestSlowWorkersOutrunOneWorker(t *testing.T) {
	if n := runtime.GOMAXPROCS(0); n < 4 {
		t.Fatalf("this machine gives Go %d threads; the comparison needs at least 4", n)
	}
	const size = 256 << 20
	ctx := context.Background()
	write := func(workers int) (time.Duration, string) {
		t.Helper()
		kr, err := seal.NewKeyring()
		if err != nil {
			t.Fatal(err)
		}
		s, err := packstore.Open(ctx, packstore.Options{Blobs: mem.New(), Keys: kr, Repo: seal.RepoID{0x10}})
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		cfg := repo.DefaultGeometry().Stream()
		cfg.Workers = workers
		t0 := time.Now()
		root, err := blob.Write(ctx, s, newRandom(3, size), cfg)
		if err != nil {
			t.Fatal(err)
		}
		return time.Since(t0), root.Hash.String()
	}
	one, rootOne := write(1)
	many, rootMany := write(0)
	t.Logf("256 MiB random: one worker %v (%.0f MB/s), %d workers %v (%.0f MB/s)", one.Round(time.Millisecond), mbs(size, one), runtime.GOMAXPROCS(0), many.Round(time.Millisecond), mbs(size, many))
	if rootMany != rootOne {
		t.Fatalf("the stream written on %d workers has root %s, on one worker %s: the chunks or their order changed", runtime.GOMAXPROCS(0), rootMany, rootOne)
	}
	if many*3 > one*2 {
		t.Fatalf("%d workers wrote 256 MiB in %v against %v on one: under one and a half times as fast, the cores are not at work", runtime.GOMAXPROCS(0), many.Round(time.Millisecond), one.Round(time.Millisecond))
	}
}
