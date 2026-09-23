package prolly_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/boundary"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/memstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
	"github.com/SmithOperatingSolutions/snapshot-core/core/stream"
)

var ctx = context.Background()

// tb is what the helpers need from a test: *testing.T and *rapid.T both have it.
type tb interface {
	Helper()
	Fatal(args ...any)
	Fatalf(format string, args ...any)
}

// counting is a memstore that counts reads, writes, and the chunks writes add.
type counting struct {
	*memstore.Store
	gets, puts, added atomic.Int64
}

func newStore() *counting { return &counting{Store: memstore.New()} }

func (c *counting) Get(ctx context.Context, h hash.Hash) ([]byte, error) {
	c.gets.Add(1)
	return c.Store.Get(ctx, h)
}

func (c *counting) Put(ctx context.Context, data []byte) (hash.Hash, error) {
	c.puts.Add(1)
	h := hash.Sum(data)
	if have, err := c.Has(ctx, []hash.Hash{h}); err == nil && !have[h] {
		c.added.Add(1)
	}
	return c.Store.Put(ctx, data)
}

func key(i int) []byte { return []byte(fmt.Sprintf("key-%08d", i)) }

// val's length depends only on gen's, so equal-length generations are
// same-length edits.
func val(i int, gen string) []byte { return []byte(fmt.Sprintf("value-%s-%08d", gen, i)) }

func empty(t tb, s *counting, c prolly.Config) *prolly.Map {
	t.Helper()
	m, err := prolly.Empty(ctx, s, c)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func flush(t tb, e *prolly.Editor) *prolly.Map {
	t.Helper()
	m, err := e.Flush(ctx)
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	return m
}

// build makes a map of keys 0..n-1 with generation gen values.
func build(t tb, s *counting, n int, gen string) *prolly.Map {
	t.Helper()
	e := empty(t, s, prolly.DefaultConfig()).Editor()
	for i := 0; i < n; i++ {
		if err := e.Put(key(i), val(i, gen)); err != nil {
			t.Fatal(err)
		}
	}
	return flush(t, e)
}

func get(t tb, m *prolly.Map, k []byte) ([]byte, bool) {
	t.Helper()
	v, ok, err := m.Get(ctx, k)
	if err != nil {
		t.Fatalf("Get(%q): %v", k, err)
	}
	return v, ok
}

type kv struct{ k, v string }

func all(t tb, m *prolly.Map, lo, hi []byte) []kv {
	t.Helper()
	it, err := m.IterRange(ctx, lo, hi)
	if err != nil {
		t.Fatal(err)
	}
	var out []kv
	for {
		k, v, ok, err := it.Next()
		if err != nil {
			t.Fatalf("iterating: %v", err)
		}
		if !ok {
			return out
		}
		out = append(out, kv{string(k), string(v)})
	}
}

// Engine Spec L1: "Empty map has a fixed, documented root hash".
func TestEmptyMapHasTheDocumentedRoot(t *testing.T) {
	s := newStore()
	m := empty(t, s, prolly.DefaultConfig())
	documented, _ := hex.DecodeString("fb50dc0717ff266cf9baf82b1ce7a1c2ef6d9247859680b11a19fb7077f5f222")
	if want := hash.Hash(sha256.Sum256([]byte{0x01, 0x00, 0x00})); !bytes.Equal(want[:], documented) || m.Root() != want {
		t.Fatalf("the empty map's root is %s, want %x (DESIGN §7: the leaf 01 00 00)", m.Root(), documented)
	}
	if m.Count() != 0 || m.Height() != 0 {
		t.Fatalf("the empty map has count %d, height %d", m.Count(), m.Height())
	}
	if _, ok := get(t, m, []byte("anything")); ok {
		t.Fatal("the empty map has a key")
	}
	re, err := prolly.Open(ctx, s, prolly.DefaultConfig(), m.Root())
	if err != nil || re.Count() != 0 || re.Root() != m.Root() {
		t.Fatalf("reopening the empty map: count %d, root %s, %v", re.Count(), re.Root(), err)
	}
}

// Engine Spec L1: "Put then Get returns the value; Get of a missing key returns ok=false".
func TestPutThenGet(t *testing.T) {
	s := newStore()
	m := build(t, s, 5000, "a")
	if m.Count() != 5000 {
		t.Fatalf("Count = %d, want 5000", m.Count())
	}
	re, err := prolly.Open(ctx, s, prolly.DefaultConfig(), m.Root())
	if err != nil {
		t.Fatal(err)
	}
	for _, mm := range []*prolly.Map{m, re} {
		for _, i := range []int{0, 1, 17, 2500, 4998, 4999} {
			if v, ok := get(t, mm, key(i)); !ok || !bytes.Equal(v, val(i, "a")) {
				t.Fatalf("Get(%s) = %q, %v; want %q", key(i), v, ok, val(i, "a"))
			}
		}
		for _, k := range [][]byte{key(5000), key(-1), []byte("key-"), []byte(""), []byte("key-00000017x"), []byte("zzz")} {
			if v, ok := get(t, mm, k); ok {
				t.Fatalf("Get(%q) of a missing key = %q, true", k, v)
			}
		}
	}
}

// Engine Spec L1: "IterRange yields keys in byte order and respects both bounds".
func TestIterRangeRespectsOrderAndBounds(t *testing.T) {
	s := newStore()
	m := build(t, s, 3000, "a")
	whole := all(t, m, nil, nil)
	if len(whole) != 3000 {
		t.Fatalf("IterRange(nil, nil) yielded %d entries, want 3000", len(whole))
	}
	for i, e := range whole {
		if e.k != string(key(i)) || e.v != string(val(i, "a")) {
			t.Fatalf("entry %d is %q=%q, want %q", i, e.k, e.v, key(i))
		}
	}
	cases := []struct {
		lo, hi     []byte
		first, end int // expected keys first..end-1
	}{
		{key(100), key(200), 100, 200},
		{append(key(100), 'a'), key(200), 101, 200}, // lo between keys
		{key(100), append(key(199), 'a'), 100, 200}, // hi between keys
		{nil, key(10), 0, 10},
		{key(2990), nil, 2990, 3000},
		{key(5), key(5), 5, 5},       // empty: lo == hi
		{key(9), key(5), 9, 9},       // empty: lo > hi
		{key(3000), nil, 3000, 3000}, // past the end
	}
	for _, c := range cases {
		got := all(t, m, c.lo, c.hi)
		if len(got) != c.end-c.first {
			t.Fatalf("IterRange(%q, %q) yielded %d entries, want %d", c.lo, c.hi, len(got), c.end-c.first)
		}
		for j, e := range got {
			if e.k != string(key(c.first+j)) {
				t.Fatalf("IterRange(%q, %q) entry %d = %q, want %q", c.lo, c.hi, j, e.k, key(c.first+j))
			}
		}
	}
	// Raw byte order, not string or numeric order.
	raw := [][]byte{{}, {0x00}, []byte("a"), []byte("a\x00"), []byte("b"), {0xff}, {0xff, 0x00}}
	e := empty(t, s, prolly.DefaultConfig()).Editor()
	for _, i := range rand.New(rand.NewPCG(1, 1)).Perm(len(raw)) {
		if err := e.Put(raw[i], []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	got := all(t, flush(t, e), nil, nil)
	if len(got) != len(raw) {
		t.Fatalf("a map of %d raw keys iterated %d", len(raw), len(got))
	}
	for i, e := range got {
		if e.k != string(raw[i]) {
			t.Fatalf("raw key %d = %x, want %x: not byte order", i, e.k, raw[i])
		}
	}
}

// Engine Spec L1: "Changing one value in a 1M-entry map rewrites at most
// tree-height + 1 nodes" (1M under -tags slow). A same-length edit changes
// no boundary, so exactly one node per level is new.
func TestOneValueChangeRewritesHeightPlusOneNodes(t *testing.T) {
	s := newStore()
	m := build(t, s, 20000, "a")
	if m.Height() < 2 {
		t.Fatalf("fixture: 20,000 entries made a tree of height %d, want at least 2", m.Height())
	}
	s.added.Store(0)
	e := m.Editor()
	if err := e.Put(key(12345), val(12345, "b")); err != nil {
		t.Fatal(err)
	}
	m2 := flush(t, e)
	if n := s.added.Load(); n != int64(m.Height()+1) {
		t.Fatalf("changing one value stored %d new nodes, want height+1 = %d", n, m.Height()+1)
	}
	if v, _ := get(t, m2, key(12345)); !bytes.Equal(v, val(12345, "b")) || m2.Count() != 20000 {
		t.Fatalf("after the edit Get = %q, Count = %d", v, m2.Count())
	}
	if v, _ := get(t, m, key(12345)); !bytes.Equal(v, val(12345, "a")) {
		t.Fatal("the old map changed: maps are immutable")
	}
}

// An edit that changes a value's length, an insert and a delete may move a
// boundary, but the tree resyncs at the next one: a few nodes per level.
func TestEditsThatMoveBoundariesRewriteAFewNodesPerLevel(t *testing.T) {
	s := newStore()
	m := build(t, s, 20000, "a")
	for name, edit := range map[string]func(e *prolly.Editor) error{
		"longer value": func(e *prolly.Editor) error { return e.Put(key(7777), val(7777, "a much longer generation")) },
		"insert":       func(e *prolly.Editor) error { return e.Put(append(key(7777), 'x'), []byte("inserted")) },
		"delete":       func(e *prolly.Editor) error { return e.Delete(key(7777)) },
	} {
		s.added.Store(0)
		e := m.Editor()
		if err := edit(e); err != nil {
			t.Fatal(err)
		}
		m2 := flush(t, e)
		if n, limit := s.added.Load(), int64(3*(m.Height()+1)); n == 0 || n > limit {
			t.Errorf("%s stored %d new nodes, want 1..%d", name, n, limit)
		}
		if got := all(t, m2, key(7770), key(7790)); len(got) < 19 || len(got) > 21 {
			t.Errorf("%s: the neighbourhood has %d entries", name, len(got))
		}
	}
}

// Engine Spec L1: "Diff of two maps differing by n entries returns exactly
// those n, and reads O(n log N) nodes (assert via a counting chunk store)".
func TestDiffReturnsExactlyTheChangesAndReadsLittle(t *testing.T) {
	s := newStore()
	from := build(t, s, 20000, "a")
	e := from.Editor()
	want := map[string]prolly.Change{}
	for _, i := range []int{3, 4000, 4001, 9999, 15000} { // modified
		must(t, e.Put(key(i), val(i, "b")))
		want[string(key(i))] = prolly.Change{Kind: prolly.Modified, Key: key(i), From: val(i, "a"), To: val(i, "b")}
	}
	for _, i := range []int{0, 12000, 19999} { // removed
		must(t, e.Delete(key(i)))
		want[string(key(i))] = prolly.Change{Kind: prolly.Removed, Key: key(i), From: val(i, "a")}
	}
	for _, i := range []int{500, 17000} { // added between existing keys
		k := append(key(i), 'x')
		must(t, e.Put(k, []byte("new")))
		want[string(k)] = prolly.Change{Kind: prolly.Added, Key: k, To: []byte("new")}
	}
	to := flush(t, e)

	s.gets.Store(0)
	got := diff(t, from, to)
	reads := s.gets.Load()
	if len(got) != len(want) {
		t.Fatalf("Diff returned %d changes, want %d", len(got), len(want))
	}
	for i, c := range got {
		w, ok := want[string(c.Key)]
		if !ok || c.Kind != w.Kind || !bytes.Equal(c.From, w.From) || !bytes.Equal(c.To, w.To) {
			t.Fatalf("change %d: %v %q (%q -> %q), want %v", i, c.Kind, c.Key, c.From, c.To, w)
		}
		if i > 0 && bytes.Compare(got[i-1].Key, c.Key) >= 0 {
			t.Fatal("Diff's changes are not in key order")
		}
	}
	if limit := int64(4 * len(want) * (from.Height() + 1)); reads > limit {
		t.Fatalf("a diff of %d changes read %d nodes, want at most %d (4 · n · (height+1))", len(want), reads, limit)
	}
	s.gets.Store(0)
	if same := diff(t, to, to); len(same) != 0 || s.gets.Load() != 0 {
		t.Fatalf("a map diffed with itself gave %d changes and read %d nodes, want 0 and 0", len(same), s.gets.Load())
	}
	back := diff(t, to, from)
	if len(back) != len(want) || back[0].Kind != prolly.Added || !bytes.Equal(back[0].Key, key(0)) {
		t.Fatalf("the reverse diff has %d changes, first %v %q", len(back), back[0].Kind, back[0].Key)
	}
}

func must(t tb, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func diff(t tb, from, to *prolly.Map) []prolly.Change {
	t.Helper()
	d, err := prolly.Diff(ctx, from, to)
	if err != nil {
		t.Fatal(err)
	}
	var out []prolly.Change
	for {
		c, ok, err := d.Next()
		if err != nil {
			t.Fatalf("Diff: %v", err)
		}
		if !ok {
			return out
		}
		out = append(out, c)
	}
}

// Engine Spec L1: "Oversized key returns ErrKeyTooLarge; no partial write".
func TestOversizedKeyIsRefusedWithoutAPartialWrite(t *testing.T) {
	s := newStore()
	m := build(t, s, 100, "a")
	big, limit := bytes.Repeat([]byte("k"), prolly.MaxKeySize+1), bytes.Repeat([]byte("k"), prolly.MaxKeySize)
	e := m.Editor()
	must(t, e.Put(key(1), val(1, "b")))
	if err := e.Put(big, []byte("v")); !errors.Is(err, prolly.ErrKeyTooLarge) {
		t.Fatalf("Put of a %d-byte key = %v, want ErrKeyTooLarge", len(big), err)
	}
	if err := e.Delete(big); !errors.Is(err, prolly.ErrKeyTooLarge) {
		t.Fatalf("Delete of a %d-byte key = %v, want ErrKeyTooLarge", len(big), err)
	}
	must(t, e.Put(limit, []byte("v"))) // positive control: exactly the limit
	got := flush(t, e)
	e2 := m.Editor()
	must(t, e2.Put(key(1), val(1, "b")))
	must(t, e2.Put(limit, []byte("v")))
	if want := flush(t, e2); got.Root() != want.Root() || got.Count() != 101 {
		t.Fatalf("the refused edits changed the map: root %s, want %s (count %d)", got.Root(), want.Root(), got.Count())
	}
}

// Storage Core Spec: "A prolly value over the inline limit is stored via cdc
// and reads back byte-identical". At the limit it is inline.
func TestAValueOverTheInlineLimitIsAStream(t *testing.T) {
	s := newStore()
	c := prolly.DefaultConfig()
	if c.InlineLimit != 256<<10 {
		t.Fatalf("DefaultConfig().InlineLimit = %d, want 256 KiB", c.InlineLimit)
	}
	at, over := random("at the limit", c.InlineLimit), random("over the limit", c.InlineLimit+1)
	e := empty(t, s, c).Editor()
	must(t, e.Put([]byte("at"), at))
	must(t, e.Put([]byte("over"), over))
	m := flush(t, e)
	for k, want := range map[string][]byte{"at": at, "over": over} {
		if v, ok := get(t, m, []byte(k)); !ok || !bytes.Equal(v, want) {
			t.Fatalf("Get(%s) returned %d bytes, want the %d written", k, len(v), len(want))
		}
	}
	values := leafValues(t, s, m.Root())
	if v := values["at"]; v.isRef || len(v.inline) != c.InlineLimit {
		t.Fatalf("a value of exactly the inline limit is stored %+v, want inline", v)
	}
	v := values["over"]
	if !v.isRef || v.ref.Size != uint64(c.InlineLimit+1) {
		t.Fatalf("a value over the inline limit is stored inline (%d bytes)", len(v.inline))
	}
	if b, err := stream.ReadAll(ctx, s, v.ref); err != nil || !bytes.Equal(b, over) {
		t.Fatalf("the long value's stream reads %d bytes (%v)", len(b), err)
	}
}

func random(seed string, n int) []byte {
	out := make([]byte, 0, n+32)
	for i := 0; len(out) < n; i++ {
		h := sha256.Sum256([]byte(fmt.Sprintf("%s/%d", seed, i)))
		out = append(out, h[:]...)
	}
	return out[:n]
}

// Engine Spec L1: "Node sizes stay within 512 B – 16 KiB across 1M random
// entries" (1M under -tags slow): every node but the last of its level.
func TestNodeSizesStayWithinBounds(t *testing.T) {
	s := newStore()
	checkNodeSizes(t, s, randomMap(t, s, 50000, 1))
}

func randomMap(t tb, s *counting, n int, seed uint64) *prolly.Map {
	t.Helper()
	rng := rand.New(rand.NewPCG(seed, seed))
	e := empty(t, s, prolly.DefaultConfig()).Editor()
	for i := 0; i < n; i++ {
		k := make([]byte, 8+rng.IntN(33))
		for j := range k {
			k[j] = byte(rng.Uint32())
		}
		must(t, e.Put(k, random(string(k), rng.IntN(101))))
	}
	return flush(t, e)
}

func checkNodeSizes(t tb, s *counting, m *prolly.Map) {
	t.Helper()
	if m.Count() < 1000 {
		t.Fatalf("fixture: the map holds %d entries, want many thousands", m.Count())
	}
	sizes := map[int][]int{}
	walk(t, s, m.Root(), m.Height(), nil, sizes)
	nodes := 0
	for level, ns := range sizes {
		nodes += len(ns)
		for i, n := range ns {
			if n > 16<<10 || (i < len(ns)-1 && n < 512) {
				t.Fatalf("level %d node %d of %d is %d bytes, outside 512 B .. 16 KiB", level, i, len(ns), n)
			}
		}
	}
	if nodes < 100 {
		t.Fatalf("fixture: the map has %d nodes, want hundreds", nodes)
	}
}

// The default split geometry is the one the tests assume.
func TestDefaultConfigIsTheRepoGeometry(t *testing.T) {
	c := prolly.DefaultConfig()
	if c.Nodes != boundary.DefaultGeometry() || c.Stream != stream.DefaultConfig() {
		t.Fatalf("DefaultConfig = %+v", c)
	}
}

// Flush re-chunks only around its edits and stops where the old and new
// trees agree again: one edit reads a few nodes per level, not the map.
func TestFlushReadsOnlyAroundItsEdits(t *testing.T) {
	s := newStore()
	m := build(t, s, 20000, "a")
	e := m.Editor()
	must(t, e.Put(key(10), val(10, "b")))
	s.gets.Store(0)
	flush(t, e)
	if reads, limit := s.gets.Load(), int64(3*(m.Height()+1)); reads > limit {
		t.Fatalf("flushing one edit into 20,000 entries read %d nodes, want at most %d", reads, limit)
	}
}

// Deleting every entry leaves the empty map, with its documented root.
func TestDeletingEverythingLeavesTheEmptyMap(t *testing.T) {
	s := newStore()
	m := build(t, s, 5000, "a")
	e := m.Editor()
	for i := 0; i < 5000; i++ {
		must(t, e.Delete(key(i)))
	}
	got := flush(t, e)
	if want := empty(t, s, prolly.DefaultConfig()); got.Root() != want.Root() || got.Count() != 0 || got.Height() != 0 {
		t.Fatalf("deleting every entry left root %s (count %d, height %d), want the empty map %s", got.Root(), got.Count(), got.Height(), want.Root())
	}
	if _, ok := get(t, got, key(1)); ok {
		t.Fatal("a deleted key is still there")
	}
}
