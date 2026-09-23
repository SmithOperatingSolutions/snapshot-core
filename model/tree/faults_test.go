package tree_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/memstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/model/tree"
)

var errInjected = errors.New("injected store failure")

// faulty fails exactly one read or one write, the failGet-th or failPut-th
// (0: none), and passes everything else through.
type faulty struct {
	*memstore.Store
	failGet, failPut int64
	gets, puts       atomic.Int64
}

func (f *faulty) Get(ctx context.Context, h hash.Hash) ([]byte, error) {
	if f.gets.Add(1) == f.failGet {
		return nil, errInjected
	}
	return f.Store.Get(ctx, h)
}

func (f *faulty) Put(ctx context.Context, b []byte) (hash.Hash, error) {
	if f.puts.Add(1) == f.failPut {
		return hash.Hash{}, errInjected
	}
	return f.Store.Put(ctx, b)
}

func (f *faulty) fail(get, put int64) {
	f.failGet, f.failPut = get, put
	f.gets.Store(0)
	f.puts.Store(0)
}

// everyFailurePoint runs op once with nothing failing, to count its reads
// and writes, then once for every read and every write it makes with that
// one failing: every run must return the store's error.
func everyFailurePoint(t *testing.T, f *faulty, name string, op func() error) {
	t.Helper()
	f.fail(0, 0)
	if err := op(); err != nil {
		t.Fatalf("%s: positive control: %v", name, err)
	}
	reads, writes := f.gets.Load(), f.puts.Load()
	if reads+writes == 0 {
		t.Fatalf("%s: fixture: the operation touched the store not at all", name)
	}
	for n := int64(1); n <= reads; n++ {
		f.fail(n, 0)
		if err := op(); !errors.Is(err, errInjected) {
			t.Fatalf("%s: with read %d of %d failing, err = %v; want the store's error", name, n, reads, err)
		}
	}
	for n := int64(1); n <= writes; n++ {
		f.fail(0, n)
		if err := op(); !errors.Is(err, errInjected) {
			t.Fatalf("%s: with write %d of %d failing, err = %v; want the store's error", name, n, writes, err)
		}
	}
	f.fail(0, 0)
}

// A failed read is never a missing entry, a short tree, the end of a diff
// or a merge with nothing more to apply; a failed write never leaves a
// tree looking written. Every store error, at every point the tree model
// touches the store, is its error.
func TestStoreErrorsSurfaceAtEveryPoint(t *testing.T) {
	f := &faulty{Store: memstore.New()}
	m := tree.Model{Config: cfg()}
	entries := map[string]tree.Entry{}
	for i := range 2000 {
		entries[fmt.Sprintf("dir%02d/file%04d", i%20, i)] = file(fmt.Sprint(i))
	}
	base := write(t, f, entries)
	ours, theirs := clone(entries), clone(entries)
	ours["dir03/file0003"] = file("ours")
	for _, i := range []int{7, 900, 1999} {
		theirs[fmt.Sprintf("dir%02d/file%04d", i%20, i)] = file(fmt.Sprint("theirs", i))
	}
	delete(theirs, "dir10/file1010")
	o, th := write(t, f, ours), write(t, f, theirs)

	everyFailurePoint(t, f, "Write", func() error { _, err := tree.Write(ctx, f, cfg(), theirs); return err })
	everyFailurePoint(t, f, "Read", func() error { _, err := tree.Read(ctx, f, cfg(), th); return err })
	everyFailurePoint(t, f, "Validate", func() error { return m.Validate(ctx, th, f) })
	everyFailurePoint(t, f, "Diff", func() error {
		d, err := m.Diff(ctx, base, th, f)
		if err != nil {
			return err
		}
		for {
			_, ok, err := d.Next(ctx)
			if err != nil || !ok {
				return err
			}
		}
	})
	everyFailurePoint(t, f, "Merge", func() error { _, err := m.Merge(ctx, base, o, th, f); return err })
}
