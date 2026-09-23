package stream_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/memstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/stream"
)

// recording is a memstore that remembers the chunks put into it and counts
// reads; failGet (0: none) makes that read fail.
type recording struct {
	*memstore.Store
	mu      sync.Mutex
	stored  map[hash.Hash]bool
	gets    atomic.Int64
	failGet int64
}

var errInjected = errors.New("injected store failure")

func newRecording() *recording {
	return &recording{Store: memstore.New(), stored: map[hash.Hash]bool{}}
}

func (r *recording) Put(ctx context.Context, b []byte) (hash.Hash, error) {
	h, err := r.Store.Put(ctx, b)
	if err == nil {
		r.mu.Lock()
		r.stored[h] = true
		r.mu.Unlock()
	}
	return h, err
}

func (r *recording) Get(ctx context.Context, h hash.Hash) ([]byte, error) {
	if r.gets.Add(1) == r.failGet {
		return nil, errInjected
	}
	return r.Store.Get(ctx, h)
}

// walkAll walks ref going into every chunk once, and returns what it was
// shown, in order.
func walkAll(s chunk.Reader, ref stream.Ref) ([]hash.Hash, error) {
	var order []hash.Hash
	seen := map[hash.Hash]bool{}
	err := stream.Walk(ctx, s, ref, func(h hash.Hash) (bool, error) {
		order = append(order, h)
		first := !seen[h]
		seen[h] = true
		return first, nil
	})
	return order, err
}

// Walk names every chunk a stream is made of, root first, reading its index
// nodes and never its data: GC marks a large blob without reading its bytes.
func TestWalkNamesEveryChunkAndReadsOnlyTheIndex(t *testing.T) {
	deepest := 0
	for _, n := range []int{0, 1, 5000, 1 << 20, 3 << 20} {
		s := newRecording()
		ref := write(t, s, random(fmt.Sprint("walk", n), n), small())
		nodes := map[int][]int{}
		if n > 0 {
			walk(t, s, ref.Root, int(ref.Depth), ref.Size, nodes)
		}
		index := 0
		for _, ls := range nodes {
			index += len(ls)
		}
		deepest = max(deepest, int(ref.Depth))
		s.gets.Store(0)
		order, err := walkAll(s, ref)
		if err != nil {
			t.Fatalf("%d bytes: Walk: %v", n, err)
		}
		named := map[hash.Hash]bool{}
		for _, h := range order {
			named[h] = true
		}
		if len(order) == 0 || order[0] != ref.Root {
			t.Fatalf("%d bytes: Walk named %d chunks, the root not first", n, len(order))
		}
		if len(named) != len(s.stored) {
			t.Fatalf("%d bytes: Walk named %d distinct chunks, the write stored %d", n, len(named), len(s.stored))
		}
		for h := range s.stored {
			if !named[h] {
				t.Fatalf("%d bytes: Walk never named stored chunk %s", n, h.Short())
			}
		}
		if reads := s.gets.Load(); reads != int64(index) {
			t.Fatalf("%d bytes: Walk read %d chunks, want only the %d index nodes", n, reads, index)
		}
	}
	if deepest < 2 {
		t.Fatalf("fixture: the deepest stream has depth %d, want at least 2", deepest)
	}
}

// Walk goes into a chunk only when visit says so, and a visit error ends it.
func TestWalkGoesNoFurtherThanVisitSays(t *testing.T) {
	s := newRecording()
	ref := write(t, s, random("stop", 3<<20), small())
	if ref.Depth < 2 {
		t.Fatalf("fixture: depth %d, want at least 2", ref.Depth)
	}
	s.gets.Store(0)
	var shown []hash.Hash
	if err := stream.Walk(ctx, s, ref, func(h hash.Hash) (bool, error) { shown = append(shown, h); return false, nil }); err != nil {
		t.Fatal(err)
	}
	if len(shown) != 1 || s.gets.Load() != 0 {
		t.Fatalf("told not to go into the root, Walk named %d chunks and read %d", len(shown), s.gets.Load())
	}
	all, err := walkAll(s, ref)
	if err != nil {
		t.Fatal(err)
	}
	// Refuse the root's first child: none of what only it reaches is named.
	top, err := s.Get(ctx, ref.Root)
	if err != nil {
		t.Fatal(err)
	}
	_, es := decodeIndex(t, top)
	sub, err := walkAll(s, stream.Ref{Root: es[0].child, Size: es[0].size, Depth: ref.Depth - 1})
	if err != nil {
		t.Fatal(err)
	}
	under := map[hash.Hash]bool{}
	for _, h := range sub[1:] {
		under[h] = true
	}
	shown = shown[:0]
	err = stream.Walk(ctx, s, ref, func(h hash.Hash) (bool, error) {
		shown = append(shown, h)
		return h != es[0].child, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range shown {
		if under[h] {
			t.Fatalf("told not to go into the first child, Walk named %s beneath it", h.Short())
		}
	}
	if len(shown) != len(all)-len(sub)+1 {
		t.Fatalf("Walk named %d chunks, want the %d of the whole stream less the %d beneath the first child", len(shown), len(all), len(sub)-1)
	}
	stop := errors.New("stop here")
	calls := 0
	err = stream.Walk(ctx, s, ref, func(hash.Hash) (bool, error) {
		if calls++; calls == 3 {
			return false, stop
		}
		return true, nil
	})
	if !errors.Is(err, stop) || calls != 3 {
		t.Fatalf("a visit error at the third chunk: Walk = %v after %d visits", err, calls)
	}
}

// Walk checks what it reads as a Reader does: every forged index node that a
// Reader refuses, Walk refuses, and so an impossible Ref.
func TestWalkRefusesForgedIndexNodes(t *testing.T) {
	s := newStore()
	good := handBuilt(t, s)
	if _, err := walkAll(s, good); err != nil {
		t.Fatalf("positive control: %v", err)
	}
	a, b, c := put(t, s, []byte("alpha")), put(t, s, []byte("beta")), put(t, s, []byte("gamma"))
	node := func(level int, es ...ientry) hash.Hash { return put(t, s, encodeIndex(level, es)) }
	ok := []ientry{{a, 5}, {b, 4}, {c, 5}}
	inner, other := node(1, ok...), node(1, ientry{c, 5}, ientry{a, 5})
	for name, ref := range map[string]stream.Ref{
		"a two-level tree under depth 1": {Root: node(2, ientry{inner, 14}, ientry{other, 10}), Size: 24, Depth: 1},
		"ref size disagrees":             {Root: good.Root, Size: 15, Depth: 1},
		"ref size smaller than the tree": {Root: good.Root, Size: 13, Depth: 1},
		"ref depth too deep":             {Root: good.Root, Size: 14, Depth: 2},
		"node claims level 2":            {Root: node(2, ok...), Size: 14, Depth: 2},
		"a single-child top":             {Root: node(2, ientry{inner, 14}), Size: 14, Depth: 2},
		"a node with no entries":         {Root: node(1), Size: 0, Depth: 1},
		"a zero-length entry":            {Root: node(1, ientry{a, 5}, ientry{b, 0}, ientry{b, 4}, ientry{c, 5}), Size: 14, Depth: 1},
		"wrong kind byte":                {Root: put(t, s, append([]byte{0x01}, encodeIndex(1, ok)[1:]...)), Size: 14, Depth: 1},
		"a byte past the entries":        {Root: put(t, s, append(encodeIndex(1, ok), 0)), Size: 14, Depth: 1},
		"truncated":                      {Root: put(t, s, encodeIndex(1, ok)[:40]), Size: 14, Depth: 1},
		"deeper than 63":                 {Root: good.Root, Size: 14, Depth: 64},
		"longer than an int64":           {Root: node(1, ientry{a, 1 << 62}, ientry{b, 1 << 62}), Size: 1 << 63, Depth: 1},
	} {
		if _, err := walkAll(s, ref); !errors.Is(err, chunk.ErrCorrupt) {
			t.Errorf("%s: Walk = %v, want ErrCorrupt", name, err)
		}
	}
}

// A failed read is never the end of a stream: every read Walk makes, failed
// in turn, is its error.
func TestWalkSurfacesStoreErrors(t *testing.T) {
	s := newRecording()
	ref := write(t, s, random("faults", 3<<20), small())
	s.gets.Store(0)
	if _, err := walkAll(s, ref); err != nil {
		t.Fatal(err)
	}
	reads := s.gets.Load()
	if reads < 3 {
		t.Fatalf("fixture: Walk read %d chunks, want several", reads)
	}
	for n := int64(1); n <= reads; n++ {
		s.gets.Store(0)
		s.failGet = n
		if _, err := walkAll(s, ref); !errors.Is(err, errInjected) {
			t.Fatalf("with read %d of %d failing, Walk = %v, want the store's error", n, reads, err)
		}
	}
}
