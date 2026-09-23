//go:build slow

package gc_test

import (
	"context"
	"encoding/binary"
	"errors"
	"math/rand/v2"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/auth"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/mem"
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

// heap is the heap in use once the collector has run.
func heap() uint64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// #6: the memory a repository of a million chunks costs to open (the chunk
// index) and to collect (the index again, plus the mark), under a bound of
// 64 Ki chunks in memory: under 16 MiB to open and under 96 MiB at the
// collection's peak, which a million chunks at the old 117 and 371 bytes
// each would be far over. docs/DESIGN.md §6 states the figures, and this
// is where they come from.
func TestSlowMemoryPerChunkOn1MChunks(t *testing.T) {
	const n = 1_000_000
	const perGroup = 1000
	const inMemory = 1 << 16
	const openBound, collectBound = 16 << 20, 96 << 20
	dir := t.TempDir()
	bs := mem.New()
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
	rng := rand.New(rand.NewPCG(6, 6))
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
	if _, err := r.CommitWorkingSet(ctx, alice, vcs.MainBranch, "a million chunks"); err != nil {
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
	openCost := heap() - base
	t.Logf("open: %d bytes, %.0f per chunk", openCost, float64(openCost)/n)
	if openCost > openBound {
		t.Fatalf("opening a repository of %d chunks with %d indexed in memory costs %d bytes, want under %d: the index past the bound belongs on disk", n, inMemory, openCost, openBound)
	}
	if err := opened.Close(); err != nil {
		t.Fatal(err)
	}
	opened = nil
	base = heap()

	var peak atomic.Uint64
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
			if m.HeapAlloc > peak.Load() {
				peak.Store(m.HeapAlloc)
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
	gcCost := peak.Load() - base
	t.Logf("collect: %d live in %v, peak %d bytes, %.0f per chunk", rep.Live, time.Since(start).Round(time.Millisecond), gcCost, float64(gcCost)/n)
	if gcCost > collectBound {
		t.Fatalf("collecting a repository of %d chunks peaks at %d bytes, want under %d: the mark and the round's index belong on disk", n, gcCost, collectBound)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("after the collection the work directory holds %d files, want none", len(entries))
	}
}
