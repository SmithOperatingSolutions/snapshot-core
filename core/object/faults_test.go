package object_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/memstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/object"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
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

// A failed read is never a missing object or the end of a diff, and a
// failed write never leaves a namespace looking flushed: every store error,
// at every point a namespace touches the store, is its error.
func TestStoreErrorsSurfaceAtEveryPoint(t *testing.T) {
	f := &faulty{Store: memstore.New()}
	reg, _ := registry(t, 1)
	path := func(i int) string { return fmt.Sprintf("dir%02d/file%04d", i%30, i) }
	e := newNamespace(t, f, reg).Editor()
	for i := range 3000 {
		must(t, e.Put(path(i), ref(1, path(i))))
	}
	from := flush(t, e)
	edits := func(e *object.Editor) {
		must(t, e.Put(path(5), ref(1, "changed")))
		must(t, e.Put("added/one", ref(1, "added")))
		must(t, e.Delete(path(2999)))
	}
	e = from.Editor()
	edits(e)
	to := flush(t, e)

	everyFailurePoint(t, f, "Open", func() error {
		_, err := object.Open(ctx, f, prolly.DefaultConfig(), reg, to.Root())
		return err
	})
	everyFailurePoint(t, f, "Get", func() error { _, _, _, err := to.Get(ctx, path(1234)); return err })
	everyFailurePoint(t, f, "Flush", func() error {
		e := from.Editor()
		edits(e)
		_, err := e.Flush(ctx)
		return err
	})
	everyFailurePoint(t, f, "Diff", func() error {
		d, err := object.Diff(ctx, from, to)
		if err != nil {
			return err
		}
		for {
			_, ok, err := d.Next()
			if err != nil || !ok {
				return err
			}
		}
	})
}
