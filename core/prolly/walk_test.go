package prolly_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/boundary"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/memstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
	"github.com/SmithOperatingSolutions/snapshot-core/core/stream"
)

// recording is a memstore that remembers the chunks put into it and the
// chunks read from it; failGet (0: none) makes that read fail.
type recording struct {
	*memstore.Store
	mu      sync.Mutex
	stored  map[hash.Hash]bool
	read    map[hash.Hash]bool
	gets    atomic.Int64
	failGet int64
}

func newRecording() *recording {
	return &recording{Store: memstore.New(), stored: map[hash.Hash]bool{}, read: map[hash.Hash]bool{}}
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
	r.mu.Lock()
	r.read[h] = true
	r.mu.Unlock()
	return r.Store.Get(ctx, h)
}

func (r *recording) forget() {
	r.mu.Lock()
	r.read = map[hash.Hash]bool{}
	r.mu.Unlock()
	r.gets.Store(0)
}

// walkAll walks the map going into every chunk once; with values not nil it
// collects the values handed over, refusing one handed twice.
func walkAll(rd chunk.Reader, c prolly.Config, root hash.Hash, values map[string][]byte) ([]hash.Hash, error) {
	var order []hash.Hash
	seen := map[hash.Hash]bool{}
	visit := func(h hash.Hash) (bool, error) {
		order = append(order, h)
		first := !seen[h]
		seen[h] = true
		return first, nil
	}
	var value func(k, v []byte) error
	if values != nil {
		value = func(k, v []byte) error {
			if _, twice := values[string(k)]; twice {
				return fmt.Errorf("the value of %q handed over twice", k)
			}
			values[string(k)] = bytes.Clone(v)
			return nil
		}
	}
	err := prolly.Walk(ctx, rd, c, root, visit, value)
	return order, err
}

// high is content whose every byte has its top bit set, so no chunk of it
// starts like a node (0x01) or a stream index node (0x02).
func high(seed string, n int) []byte {
	b := random(seed, n)
	for i := range b {
		b[i] |= 0x80
	}
	return b
}

func walkConfig() prolly.Config {
	c := prolly.DefaultConfig()
	c.InlineLimit = 100
	c.Stream.CDC.Min, c.Stream.CDC.Max = 256, 4<<10 // long values of several chunks
	return c
}

// walkFixture is a map of 20,000 short values and five long ones, several
// levels deep, written into a fresh recording store.
func walkFixture(t *testing.T) (*recording, prolly.Config, *prolly.Map, map[string][]byte) {
	t.Helper()
	s, c := newRecording(), walkConfig()
	want := map[string][]byte{}
	for i := range 20000 {
		want[string(key(i))] = val(i, "a")
	}
	for i := range 5 {
		want[fmt.Sprintf("long-%d", i)] = high(fmt.Sprint("long", i), 30000)
	}
	m, err := prolly.Empty(ctx, s, c)
	if err != nil {
		t.Fatal(err)
	}
	e := m.Editor()
	for k, v := range want {
		must(t, e.Put([]byte(k), v))
	}
	if m, err = e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if m.Height() < 2 {
		t.Fatalf("fixture: height %d, want at least 2", m.Height())
	}
	return s, c, m, want
}

// Walk names every chunk a map is made of, root first (the empty map's node,
// stored by Empty, is not part of a map with entries), and hands over every
// value once, long ones whole; without a value callback it reads no long
// value's data.
func TestWalkNamesEveryChunkOfAMap(t *testing.T) {
	s, c, m, want := walkFixture(t)
	emptyNode := hash.Sum([]byte{0x01, 0x00, 0x00})
	values := map[string][]byte{}
	order, err := walkAll(s, c, m.Root(), values)
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	if order[0] != m.Root() {
		t.Fatalf("Walk named %s first, not the root", order[0].Short())
	}
	named := map[hash.Hash]bool{}
	for _, h := range order {
		named[h] = true
	}
	for h := range s.stored {
		if h != emptyNode && !named[h] {
			t.Fatalf("Walk never named stored chunk %s", h.Short())
		}
	}
	if len(named) != len(s.stored)-1 || named[emptyNode] {
		t.Fatalf("Walk named %d distinct chunks, want the %d stored less the empty map's node", len(named), len(s.stored))
	}
	if len(values) != len(want) {
		t.Fatalf("Walk handed over %d values, want %d", len(values), len(want))
	}
	for k, v := range want {
		if !bytes.Equal(values[k], v) {
			t.Fatalf("Walk handed over %q as %d bytes, want the %d stored", k, len(values[k]), len(v))
		}
	}
	s.forget()
	if _, err := walkAll(s, c, m.Root(), nil); err != nil {
		t.Fatal(err)
	}
	structure := 0
	for h := range named {
		b, err := s.Store.Get(ctx, h)
		if err != nil {
			t.Fatal(err)
		}
		isNode := len(b) > 0 && (b[0] == 0x01 || b[0] == 0x02)
		if isNode {
			structure++
		}
		if s.read[h] != isNode {
			t.Fatalf("with no value callback Walk read %s: %v; want exactly the nodes and stream index nodes read", h.Short(), s.read[h])
		}
	}
	if structure < 10 {
		t.Fatalf("fixture: %d nodes and stream index nodes, want many", structure)
	}
}

// Walk goes into a chunk only when visit says so, handing over no value
// beneath one it was told to leave, and visit's and value's errors end it.
func TestWalkGoesNoFurtherThanVisitSays(t *testing.T) {
	s, c, m, _ := walkFixture(t)
	s.forget()
	var shown []hash.Hash
	handed := 0
	err := prolly.Walk(ctx, s, c, m.Root(), func(h hash.Hash) (bool, error) { shown = append(shown, h); return false, nil },
		func([]byte, []byte) error { handed++; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if len(shown) != 1 || handed != 0 || s.gets.Load() != 0 {
		t.Fatalf("told not to go into the root, Walk named %d chunks, handed over %d values and read %d", len(shown), handed, s.gets.Load())
	}
	top, err := s.Store.Get(ctx, m.Root())
	if err != nil {
		t.Fatal(err)
	}
	first := decodeNode(t, top)
	values := map[string][]byte{}
	seen := map[hash.Hash]bool{}
	err = prolly.Walk(ctx, s, c, m.Root(), func(h hash.Hash) (bool, error) {
		if h == first.kids[0] {
			return false, nil
		}
		fresh := !seen[h]
		seen[h] = true
		return fresh, nil
	}, func(k, v []byte) error { values[string(k)] = v; return nil })
	if err != nil {
		t.Fatal(err)
	}
	for k := range values {
		if k <= string(first.keys[0]) {
			t.Fatalf("told not to go into the first child, Walk handed over %q beneath it", k)
		}
	}
	if len(values) == 0 {
		t.Fatal("told to leave only the first child, Walk handed over nothing")
	}
	stop := errors.New("stop here")
	calls := 0
	err = prolly.Walk(ctx, s, c, m.Root(), func(hash.Hash) (bool, error) {
		if calls++; calls == 3 {
			return false, stop
		}
		return true, nil
	}, nil)
	if !errors.Is(err, stop) || calls != 3 {
		t.Fatalf("a visit error at the third chunk: Walk = %v after %d visits", err, calls)
	}
	err = prolly.Walk(ctx, s, c, m.Root(), func(hash.Hash) (bool, error) { return true, nil },
		func([]byte, []byte) error { return stop })
	if !errors.Is(err, stop) {
		t.Fatalf("a value error: Walk = %v", err)
	}
}

// Walk checks what it reads as a read does: every forged tree a read
// refuses, Walk refuses; so does an unusable configuration.
func TestWalkRefusesForgedMaps(t *testing.T) {
	s := newStore()
	c := prolly.DefaultConfig()
	c.InlineLimit = 100
	l1, l2 := store(t, s, leaf("a", "1", "b", "2")), store(t, s, leaf("c", "3"))
	good := store(t, s, internal(1, "b", l1, 2, "c", l2, 1))
	if _, err := walkAll(s, c, good, map[string][]byte{}); err != nil {
		t.Fatalf("positive control: %v", err)
	}
	ac, bd := store(t, s, leaf("a", "1", "c", "3")), store(t, s, leaf("b", "2", "d", "4"))
	emptyLeaf := store(t, s, leaf())
	raw := func(b []byte) hash.Hash {
		h, err := s.Store.Put(ctx, b)
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	shortRef := tnode{keys: [][]byte{[]byte("k")}, vals: []tvalue{{isRef: true, ref: stream.Ref{Size: 50}}}}
	for name, root := range map[string]hash.Hash{
		"leaf keys out of order":            store(t, s, leaf("b", "2", "a", "1")),
		"entry key is not the child's last": store(t, s, internal(1, "bb", l1, 2, "c", l2, 1)),
		"entry count disagrees":             store(t, s, internal(1, "b", l1, 3, "c", l2, 1)),
		"child level is not one below":      store(t, s, internal(2, "b", l1, 2, "c", l2, 1)),
		"children overlap":                  store(t, s, internal(1, "c", ac, 2, "d", bd, 2)),
		"a child with no entries":           store(t, s, internal(1, "b", l1, 2, "c", emptyLeaf, 1)),
		"inline value over the limit":       store(t, s, leaf("k", string(bytes.Repeat([]byte("v"), 101)))),
		"stream ref within the limit":       store(t, s, shortRef),
		"an internal node with no entries":  store(t, s, internal(1)),
		"a single-child root":               store(t, s, internal(1, "b", l1, 2)),
		"bytes past the entries":            raw(append(encodeNode(leaf("a", "1")), 0)),
		"wrong kind byte":                   raw(append([]byte{0x02}, encodeNode(leaf("a", "1"))[1:]...)),
		"truncated":                         raw(encodeNode(leaf("a", "1"))[:5]),
	} {
		if _, err := walkAll(s, c, root, map[string][]byte{}); !errors.Is(err, chunk.ErrCorrupt) {
			t.Errorf("%s: Walk = %v, want ErrCorrupt", name, err)
		}
	}
	unusable := c
	unusable.Nodes = boundary.Geometry{Min: 1, Target: 2, Max: 3} // decoding never looks at it
	if _, err := walkAll(s, unusable, good, nil); !errors.Is(err, boundary.ErrGeometry) {
		t.Errorf("Walk with an unusable node geometry = %v, want ErrGeometry", err)
	}
}

// A failed read is never the end of a map or of a value: every read Walk
// makes, values included, failed in turn, is its error.
func TestWalkSurfacesStoreErrors(t *testing.T) {
	s, c, m, _ := walkFixture(t)
	s.forget()
	if _, err := walkAll(s, c, m.Root(), map[string][]byte{}); err != nil {
		t.Fatal(err)
	}
	reads := s.gets.Load()
	for n := int64(1); n <= reads; n++ {
		s.forget()
		s.failGet = n
		if _, err := walkAll(s, c, m.Root(), map[string][]byte{}); !errors.Is(err, errInjected) {
			t.Fatalf("with read %d of %d failing, Walk = %v, want the store's error", n, reads, err)
		}
	}
}
