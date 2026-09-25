package prolly_test

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/boundary"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/memstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
	"github.com/SmithOperatingSolutions/snapshot-core/core/stream"
)

var errInjected = errors.New("injected store failure")

// faulty fails exactly one read or one write, the failGet-th or failPut-th
// (0: none), and passes everything else through. Failing only one means an
// operation that swallowed the error would go on to succeed, visibly.
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

// A failed read is never a missing key, the end of an iteration, or no
// difference; a failed write never leaves a flush looking done. Every store
// error, at every point an operation touches the store, is its error.
func TestStoreErrorsSurfaceAtEveryPoint(t *testing.T) {
	f := &faulty{Store: memstore.New()}
	c := prolly.DefaultConfig()
	c.InlineLimit = 100
	c.Stream.CDC.Min, c.Stream.CDC.Max = 256, 4<<10 // long values of several chunks
	e := mustEmpty(t, f, c).Editor()
	for i := 0; i < 20000; i++ {
		must(t, e.Put(key(i), val(i, "a")))
	}
	must(t, e.Put([]byte("long"), random("long", 20000)))
	m := mustFlush(t, e)
	if m.Height() < 2 {
		t.Fatalf("fixture: height %d, want at least 2", m.Height())
	}
	edits := func(e *prolly.Editor) {
		must(t, e.Put(key(7), val(7, "b")))
		must(t, e.Put(append(key(9000), 'x'), []byte("inserted")))
		must(t, e.Delete(key(15000)))
		must(t, e.Put([]byte("long"), random("longer", 30000)))
	}
	e2 := m.Editor()
	edits(e2)
	m2 := mustFlush(t, e2)

	everyFailurePoint(t, f, "Empty", func() error { _, err := prolly.Empty(ctx, f, c); return err })
	everyFailurePoint(t, f, "Open", func() error { _, err := prolly.Open(ctx, f, c, m.Root()); return err })
	everyFailurePoint(t, f, "Get", func() error { _, _, err := m.Get(ctx, key(12345)); return err })
	everyFailurePoint(t, f, "Get of a long value", func() error { _, _, err := m.Get(ctx, []byte("long")); return err })
	everyFailurePoint(t, f, "IterRange", func() error {
		it, err := m.IterRange(ctx, key(19000), nil)
		if err != nil {
			return err
		}
		for {
			_, _, ok, err := it.Next()
			if err != nil || !ok {
				return err
			}
		}
	})
	everyFailurePoint(t, f, "Flush", func() error {
		e := m.Editor()
		edits(e)
		_, err := e.Flush(ctx)
		return err
	})
	everyFailurePoint(t, f, "Flush that collapses the tree", func() error {
		e := m.Editor()
		for i := 1; i < 20000; i++ {
			must(t, e.Delete(key(i)))
		}
		_, err := e.Flush(ctx)
		return err
	})
	small := mustFlush(t, putAll(t, mustEmpty(t, f, c).Editor(), 0, 10))
	everyFailurePoint(t, f, "Flush that grows the tree", func() error {
		_, err := putAll(t, small.Editor(), 10, 5000).Flush(ctx)
		return err
	})
	everyFailurePoint(t, f, "Diff", func() error {
		d, err := prolly.Diff(ctx, m, m2)
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

	// A flush that failed can be retried with the same editor.
	e3 := m.Editor()
	edits(e3)
	f.fail(0, 1)
	if _, err := e3.Flush(ctx); !errors.Is(err, errInjected) {
		t.Fatalf("positive control: a flush whose first write fails = %v", err)
	}
	f.fail(0, 0)
	if again := mustFlush(t, e3); again.Root() != m2.Root() {
		t.Fatalf("retrying a failed flush made root %s, want %s", again.Root(), m2.Root())
	}
}

func mustEmpty(t *testing.T, s chunk.ReadWriter, c prolly.Config) *prolly.Map {
	t.Helper()
	m, err := prolly.Empty(ctx, s, c)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func mustFlush(t *testing.T, e *prolly.Editor) *prolly.Map {
	t.Helper()
	m, err := e.Flush(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func putAll(t *testing.T, e *prolly.Editor, from, to int) *prolly.Editor {
	t.Helper()
	for i := from; i < to; i++ {
		must(t, e.Put(key(i), val(i, "a")))
	}
	return e
}

// A diff reads long values back whole, and a long value that did not change
// is no change.
func TestDiffReadsLongValues(t *testing.T) {
	s := newStore()
	c := prolly.DefaultConfig()
	c.InlineLimit = 100
	e := empty(t, s, c).Editor()
	must(t, e.Put([]byte("same"), random("same", 5000)))
	must(t, e.Put([]byte("changes"), random("before", 5000)))
	from := flush(t, e)
	must(t, e.Put([]byte("changes"), random("after", 5000)))
	to := flush(t, e)
	got := diff(t, from, to)
	if len(got) != 1 || got[0].Kind != prolly.Modified || string(got[0].Key) != "changes" ||
		!bytes.Equal(got[0].From, random("before", 5000)) || !bytes.Equal(got[0].To, random("after", 5000)) {
		t.Fatalf("diffing a changed long value beside an unchanged one gave %d changes: %+v", len(got), got)
	}
}

// Configurations the map cannot work with are refused, not silently used.
func TestConfigIsValidated(t *testing.T) {
	s := newStore()
	good := prolly.DefaultConfig()
	m := empty(t, s, good)
	for name, c := range map[string]prolly.Config{
		"negative inline limit":     {Nodes: good.Nodes, InlineLimit: -1, Stream: good.Stream},
		"inline limit over 512 KiB": {Nodes: good.Nodes, InlineLimit: 512<<10 + 1, Stream: good.Stream},
		"unusable node geometry":    {Nodes: boundary.Geometry{Min: 1, Target: 2, Max: 3}, InlineLimit: 100, Stream: good.Stream},
		"negative value limit":      {Nodes: good.Nodes, InlineLimit: good.InlineLimit, Stream: good.Stream, MaxValue: -1},
	} {
		if _, err := prolly.Empty(ctx, s, c); err == nil {
			t.Errorf("Empty with %s succeeded", name)
		}
		if _, err := prolly.Open(ctx, s, c, m.Root()); err == nil {
			t.Errorf("Open with %s succeeded", name)
		}
	}
}

// Two more rules a node must keep: an entry counting leaves must not point
// at an empty node, and a value's tag is 0 or 1.
func TestMoreForgedTreesAreCorrupt(t *testing.T) {
	s := newStore()
	c := prolly.DefaultConfig()
	l1, emptyLeaf := store(t, s, leaf("a", "1", "b", "2")), store(t, s, leaf())
	badTag := encodeNode(leaf("k", "v"))
	badTag[5] = 0x02 // 01 00 01 · 01 'k' · tag
	h, err := s.Store.Put(ctx, badTag)
	if err != nil {
		t.Fatal(err)
	}
	for name, root := range map[string]hash.Hash{
		"an empty child under a count": store(t, s, internal(1, "b", l1, 2, "c", emptyLeaf, 1)),
		"a value tag of 2":             h,
	} {
		if err := readAll(s, c, root); !errors.Is(err, chunk.ErrCorrupt) {
			t.Errorf("%s: reading = %v, want ErrCorrupt", name, err)
		}
	}
}

// The stream reader refuses Refs it cannot serve and offsets before the start.
func TestStreamRefusesImpossibleRefsAndOffsets(t *testing.T) {
	s := newStore()
	ref, err := stream.Write(ctx, s, bytes.NewReader(random("s", 1000)), stream.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	for name, bad := range map[string]stream.Ref{
		"deeper than 63":       {Root: ref.Root, Size: ref.Size, Depth: 64},
		"longer than an int64": {Root: ref.Root, Size: 1 << 63, Depth: 0},
	} {
		if _, err := stream.Open(ctx, s, bad); !errors.Is(err, chunk.ErrCorrupt) {
			t.Errorf("%s: Open = %v, want ErrCorrupt", name, err)
		}
	}
	r, err := stream.Open(ctx, s, ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReadAt(make([]byte, 1), -1); err == nil {
		t.Error("ReadAt at a negative offset succeeded")
	}
	cfg := stream.DefaultConfig()
	cfg.Nodes.Min = 1
	if _, err := stream.Write(ctx, s, bytes.NewReader(nil), cfg); !errors.Is(err, boundary.ErrGeometry) {
		t.Errorf("Write with an unusable node geometry = %v, want ErrGeometry", err)
	}
	cfg = stream.DefaultConfig()
	cfg.CDC.Max = 0
	if _, err := stream.Write(ctx, s, bytes.NewReader(nil), cfg); err == nil {
		t.Error("Write with an unusable CDC geometry succeeded")
	}
}
