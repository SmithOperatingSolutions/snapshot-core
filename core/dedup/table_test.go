package dedup_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/dedup"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
)

// record is a key and the value it was added with.
type record struct {
	h hash.Hash
	v []byte
}

// records makes n distinct keys with distinct 17-byte values, in a random
// order that is not the sorted one.
func records(seed uint64, n int) []record {
	rng := rand.New(rand.NewPCG(seed, seed))
	out := make([]record, n)
	seen := map[hash.Hash]bool{}
	for i := range out {
		for {
			var h hash.Hash
			binary.LittleEndian.PutUint64(h[:], rng.Uint64())
			binary.LittleEndian.PutUint64(h[8:], rng.Uint64())
			binary.LittleEndian.PutUint64(h[16:], rng.Uint64())
			binary.LittleEndian.PutUint64(h[24:], rng.Uint64())
			if !seen[h] {
				seen[h] = true
				out[i].h = h
				break
			}
		}
		out[i].v = make([]byte, 17)
		binary.LittleEndian.PutUint64(out[i].v, uint64(i))
		out[i].v[16] = byte(i)
	}
	return out
}

// build makes a table of recs in dir, in runs of runSize records.
func build(t testing.TB, dir string, runSize int, recs []record) *dedup.Table {
	t.Helper()
	b, err := dedup.NewBuilder(dir, 17)
	if err != nil {
		t.Fatalf("NewBuilder: %v", err)
	}
	dedup.SetRunSize(b, runSize)
	for _, r := range recs {
		if err := b.Add(r.h, r.v); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	tb, err := b.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	t.Cleanup(func() { _ = tb.Close() })
	return tb
}

// #6: a table built from records spilled to disk in several runs answers
// every one of them with its value, misses every key it was not given,
// and reads back in hash order without repeats. Nothing of it but the file
// is needed once built: the directory holds the table alone.
func TestATableAnswersEveryRecordItWasBuiltFrom(t *testing.T) {
	dir := t.TempDir()
	recs := records(1, 20_000)
	tb := build(t, dir, 3_000, recs) // seven runs, the last one short
	if got := tb.Len(); got != int64(len(recs)) {
		t.Fatalf("the table holds %d records, want %d", got, len(recs))
	}
	for i, r := range recs {
		v, ok, err := tb.Lookup(r.h)
		if err != nil || !ok || !bytes.Equal(v, r.v) {
			t.Fatalf("record %d: found %v, value %x, %v; want %x", i, ok, v, err, r.v)
		}
	}
	for _, r := range records(2, 1_000) {
		if v, ok, err := tb.Lookup(r.h); ok || err != nil {
			t.Fatalf("a key never added is found with value %x, %v", v, err)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("the directory holds %v after the build, want the table alone: the runs must be removed", names)
	}
	sorted := slices.Clone(recs)
	slices.SortFunc(sorted, func(a, b record) int { return a.h.Compare(b.h) })
	c := tb.Cursor()
	for i, r := range sorted {
		h, v, ok, err := c.Next()
		if err != nil || !ok || h != r.h || !bytes.Equal(v, r.v) {
			t.Fatalf("the cursor's record %d is %s %x (%v, %v), want %s %x", i, h.Short(), v, ok, err, r.h.Short(), r.v)
		}
	}
	if _, _, ok, err := c.Next(); ok || err != nil {
		t.Fatalf("after the last record the cursor gives %v, %v; want the end", ok, err)
	}
}

// A key added twice keeps its first value, across runs and within one: the
// packs still in service are added first, and win.
func TestTheFirstRecordOfAKeyWins(t *testing.T) {
	recs := records(3, 10)
	var twice []record
	for _, r := range recs {
		twice = append(twice, r)
	}
	for _, r := range recs {
		later := slices.Clone(r.v)
		later[0] ^= 0xff
		twice = append(twice, record{r.h, later})
	}
	for name, runSize := range map[string]int{"across two runs": len(recs), "within one run": 2 * len(recs)} {
		tb := build(t, t.TempDir(), runSize, twice)
		for i, r := range recs {
			v, ok, err := tb.Lookup(r.h)
			if err != nil || !ok || !bytes.Equal(v, r.v) {
				t.Fatalf("%s: key %d added twice reads %x (%v, %v), want its first value %x", name, i, v, ok, err, r.v)
			}
		}
		if got := tb.Len(); got != int64(len(recs)) {
			t.Fatalf("%s: the table holds %d records, want %d: each key once", name, got, len(recs))
		}
	}
}

// An empty table is a table.
func TestAnEmptyTableMissesEverything(t *testing.T) {
	tb := build(t, t.TempDir(), 10, nil)
	if tb.Len() != 0 {
		t.Fatalf("an empty table holds %d records", tb.Len())
	}
	if _, ok, err := tb.Lookup(hash.Hash{1}); ok || err != nil {
		t.Fatalf("an empty table finds a key: %v, %v", ok, err)
	}
	if _, _, ok, err := tb.Cursor().Next(); ok || err != nil {
		t.Fatalf("an empty table's cursor gives %v, %v", ok, err)
	}
	path := dedup.TablePath(tb)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("positive control: the empty table has no file: %v", err)
	}
	if err := tb.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("after Close the table's file is still there (%v): a closed table leaves nothing behind", err)
	}
}

// The table holds almost nothing in memory: a million records cost the
// process under 64 KiB, the price of one key per 65,536 records and the
// buffers of a lookup, however large the table.
func TestATableHoldsAlmostNothingInMemory(t *testing.T) {
	recs := records(5, 1_000_000)
	heap := func() uint64 {
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.HeapAlloc
	}
	dir := t.TempDir()
	before := heap()
	tb := build(t, dir, 1<<18, recs)
	for i, r := range recs[:100] {
		if _, ok, err := tb.Lookup(r.h); !ok || err != nil {
			t.Fatalf("positive control: record %d of a million is not found (%v, %v)", i, ok, err)
		}
	}
	runtime.KeepAlive(recs)
	if cost := heap() - before; cost > 64<<10 {
		t.Fatalf("a table of a million records holds %d bytes in memory, want under %d", cost, 64<<10)
	}
	runtime.KeepAlive(tb)
}

// A table file that does not decode is refused, never read out of bounds:
// truncated, another magic, a count the file's size does not bear out.
func TestATableThatDoesNotDecodeIsRefused(t *testing.T) {
	dir := t.TempDir()
	tb := build(t, dir, 100, records(6, 500))
	whole, err := os.ReadFile(dedup.TablePath(tb))
	if err != nil {
		t.Fatal(err)
	}
	if len(whole) < 32+500*49 {
		t.Fatalf("positive control: the table file is %d bytes, want a header and 500 records of 49", len(whole))
	}
	if _, err := dedup.OpenTable(write(t, dir, "whole", whole)); err != nil {
		t.Fatalf("positive control: the table's own bytes do not open: %v", err)
	}
	forged := map[string][]byte{
		"truncated":      whole[:len(whole)-1],
		"one byte over":  append(slices.Clone(whole), 0),
		"another magic":  append([]byte("SCTX"), whole[4:]...),
		"count doubled":  withCount(whole, 1000),
		"count zero":     withCount(whole, 0),
		"header only":    whole[:32],
		"empty":          nil,
		"another value":  withValueLen(whole, 16),
		"another period": withEvery(whole, 128),
	}
	for name, b := range forged {
		tb, err := dedup.OpenTable(write(t, dir, name, b))
		if !errors.Is(err, dedup.ErrCorrupt) {
			t.Fatalf("%s: opened with %v, want ErrCorrupt", name, err)
		}
		if tb != nil {
			t.Fatalf("%s: a refused table was returned", name)
		}
	}
}

func write(t *testing.T, dir, name string, b []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func withCount(b []byte, n uint64) []byte {
	b = slices.Clone(b)
	binary.LittleEndian.PutUint64(b[8:], n)
	return b
}

func withValueLen(b []byte, n uint16) []byte {
	b = slices.Clone(b)
	binary.LittleEndian.PutUint16(b[6:], n)
	return b
}

func withEvery(b []byte, n uint32) []byte {
	b = slices.Clone(b)
	binary.LittleEndian.PutUint32(b[16:], n)
	return b
}

// FuzzOpenTable: no bytes open a table that then reads out of bounds.
func FuzzOpenTable(f *testing.F) {
	dir := f.TempDir()
	whole, err := os.ReadFile(dedup.TablePath(build(f, dir, 10, records(7, 30))))
	if err != nil {
		f.Fatal(err)
	}
	f.Add(whole)
	f.Add(whole[:32])
	f.Add([]byte("SCTB"))
	f.Fuzz(func(t *testing.T, b []byte) {
		p := filepath.Join(t.TempDir(), "fuzz")
		if err := os.WriteFile(p, b, 0o600); err != nil {
			t.Fatal(err)
		}
		tb, err := dedup.OpenTable(p)
		if err != nil {
			return
		}
		defer tb.Close()
		for _, h := range []hash.Hash{{}, {0xff}, {0x80}} {
			if _, _, err := tb.Lookup(h); err != nil {
				return
			}
		}
		c := tb.Cursor()
		for {
			_, _, ok, err := c.Next()
			if !ok || err != nil {
				return
			}
		}
	})
}
