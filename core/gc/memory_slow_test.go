//go:build slow

package gc_test

import (
	"context"
	"encoding/binary"
	"errors"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/auth"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/local"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/packstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/gc"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/object"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
	"github.com/SmithOperatingSolutions/snapshot-core/core/vcs"
)

// bundle (model 9) is an object of many chunks: its root lists the hashes
// of groups, each of which lists the hashes of leaves. It exists to put a
// million chunks under one ref without a million namespace edits.
type bundle struct{}

func (bundle) ID() model.ID                                             { return 9 }
func (bundle) FormatVersion() uint16                                    { return 1 }
func (bundle) Validate(context.Context, model.Root, chunk.Reader) error { return nil }
func (bundle) Diff(context.Context, model.Root, model.Root, chunk.Reader) (model.DiffIter, error) {
	return nil, errors.New("unused")
}
func (bundle) Merge(context.Context, model.Root, model.Root, model.Root, chunk.ReadWriter) (model.MergeResult, error) {
	return model.MergeResult{}, errors.New("unused")
}

// Walk visits the root and every group as nodes and every leaf as a leaf.
func (bundle) Walk(ctx context.Context, root model.Root, rd chunk.Reader, visit func(hash.Hash, bool) (bool, error)) error {
	if _, err := visit(root.Hash, false); err != nil {
		return err
	}
	groups, err := hashesIn(ctx, rd, root.Hash)
	if err != nil {
		return err
	}
	for _, g := range groups {
		if _, err := visit(g, false); err != nil {
			return err
		}
		leaves, err := hashesIn(ctx, rd, g)
		if err != nil {
			return err
		}
		for _, l := range leaves {
			if _, err := visit(l, true); err != nil {
				return err
			}
		}
	}
	return nil
}

func hashesIn(ctx context.Context, rd chunk.Reader, h hash.Hash) ([]hash.Hash, error) {
	b, err := rd.Get(ctx, h)
	if err != nil {
		return nil, err
	}
	if len(b)%hash.Size != 0 {
		return nil, errors.New("bundle: a list of hashes is a multiple of the hash size")
	}
	out := make([]hash.Hash, 0, len(b)/hash.Size)
	for i := 0; i+hash.Size <= len(b); i += hash.Size {
		var x hash.Hash
		copy(x[:], b[i:i+hash.Size])
		out = append(out, x)
	}
	return out, nil
}

// heap is the heap in use once the collector has run: twice, since what a
// sync.Pool held survives one cycle in its victim cache and would be counted
// against whatever ran between two measurements.
func heap() int64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int64(m.HeapAlloc)
}

// #6: the memory a repository costs to open (the chunk index) and to
// collect (the index again, plus the mark) does not grow with it. Under a
// bound of 64 Ki chunks in memory, a million chunks open in under 16 MiB
// and collect in a peak under 48 MiB (which a million at the old 117 and
// 371 bytes each would be far over; measured 30 MiB, most of it the
// codec's encoder), and two million peak no higher than a quarter over
// that: whatever grows per chunk is on disk. docs/DESIGN.md §6
// states the figures, and this is where they come from.
func TestSlowMemoryPerChunkOn1MChunks(t *testing.T) {
	const inMemory = 1 << 16
	const openBound, collectBound = 16 << 20, 48 << 20
	open1, peak1 := measureMemory(t, 1_000_000, inMemory)
	if open1 > openBound {
		t.Fatalf("opening a repository of a million chunks with %d indexed in memory costs %d bytes, want under %d: the index past the bound belongs on disk", inMemory, open1, openBound)
	}
	if peak1 > collectBound {
		t.Fatalf("collecting a repository of a million chunks peaks at %d bytes, want under %d: the mark and the round's index belong on disk", peak1, collectBound)
	}
	open2, peak2 := measureMemory(t, 2_000_000, inMemory)
	if open2 > openBound {
		t.Fatalf("opening a repository of two million chunks costs %d bytes, want under %d", open2, openBound)
	}
	if peak2 > peak1+peak1/4 {
		t.Fatalf("collecting two million chunks peaks at %d bytes against %d for one million: the collection's memory grows with the repository", peak2, peak1)
	}
}

// measureMemory writes a repository of n chunks and returns what a fresh
// store costs to open and the live heap's peak over a collection, in
// bytes. The repository is on disk (blob/local), so what is stored, and
// what a collection stores, is not on the heap.
func measureMemory(t *testing.T, n int, inMemory int) (open, peak int64) {
	t.Helper()
	const perGroup = 1000
	dir := t.TempDir()
	bs, err := local.Create(filepath.Join(t.TempDir(), "repo"), local.Options{})
	if err != nil {
		t.Fatal(err)
	}
	keys, err := seal.NewKeyring()
	if err != nil {
		t.Fatal(err)
	}
	reg, err := model.NewRegistry(bundle{})
	if err != nil {
		t.Fatal(err)
	}
	po := packstore.Options{Blobs: bs, Keys: keys, Repo: repo, IndexDir: dir, IndexInMemory: inMemory}
	s, err := packstore.Open(ctx, po)
	if err != nil {
		t.Fatal(err)
	}
	vo := vcs.Options{Config: prolly.DefaultConfig(), Registry: reg, Authorizer: auth.AllowAll{}}
	r, err := vcs.Init(ctx, s, alice, vo)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	rng := rand.New(rand.NewPCG(uint64(n), 6))
	leaf := make([]byte, 64)
	fill := func(b []byte) {
		for i := 0; i+8 <= len(b); i += 8 {
			binary.LittleEndian.PutUint64(b[i:], rng.Uint64())
		}
	}
	var groups []byte
	for g := 0; g < n/perGroup; g++ {
		var leaves []byte
		for range perGroup {
			fill(leaf)
			h, err := s.Put(ctx, leaf)
			if err != nil {
				t.Fatal(err)
			}
			leaves = append(leaves, h[:]...)
		}
		h, err := s.Put(ctx, leaves)
		if err != nil {
			t.Fatal(err)
		}
		groups = append(groups, h[:]...)
	}
	root, err := s.Put(ctx, groups)
	if err != nil {
		t.Fatal(err)
	}
	ws, err := r.WorkingSet(ctx, alice, vcs.MainBranch)
	if err != nil {
		t.Fatal(err)
	}
	ns, err := r.Namespace(ctx, ws.Working)
	if err != nil {
		t.Fatal(err)
	}
	e := ns.Editor()
	if err := e.Put("data", object.Ref{Model: 9, Root: model.Root{Hash: root, Size: uint64(len(groups)), Format: 1}}); err != nil {
		t.Fatal(err)
	}
	if ns, err = e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	next := ws
	next.Working, next.Staged = ns.Root(), ns.Root()
	if _, err := r.UpdateWorkingSet(ctx, alice, vcs.MainBranch, ws, next); err != nil {
		t.Fatal(err)
	}
	if _, err := r.CommitWorkingSet(ctx, alice, vcs.MainBranch, "many chunks"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("wrote %d chunks in %v", n, time.Since(start).Round(time.Millisecond))

	base := heap()
	opened, err := packstore.Open(ctx, po)
	if err != nil {
		t.Fatal(err)
	}
	open = heap() - base
	t.Logf("open: %d bytes", open)
	if err := opened.Close(); err != nil {
		t.Fatal(err)
	}
	opened = nil
	base = heap()

	var high atomic.Uint64
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		var m runtime.MemStats
		for {
			select {
			case <-stop:
				return
			case <-time.After(20 * time.Millisecond):
			}
			runtime.GC() // the live heap, not the garbage between cycles
			runtime.ReadMemStats(&m)
			if m.HeapAlloc > high.Load() {
				high.Store(m.HeapAlloc)
			}
		}
	}()
	start = time.Now()
	rep, err := gc.Run(ctx, gc.Options{Blobs: bs, Keys: keys, Repo: repo, Config: prolly.DefaultConfig(), Registry: reg, Grace: grace,
		WorkDir: dir, IndexInMemory: inMemory})
	close(stop)
	<-done
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if rep.Live < n {
		t.Fatalf("GC marked %d chunks live, want at least the %d leaves", rep.Live, n)
	}
	peak = int64(high.Load()) - base
	t.Logf("collect: %d live in %v, peak %d bytes", rep.Live, time.Since(start).Round(time.Millisecond), peak)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("after the collection the work directory holds %d files, want none", len(entries))
	}
	return open, peak
}
