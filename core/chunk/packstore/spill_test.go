package packstore_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/mem"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/packstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/dedup"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
)

// openSpilled opens a store that keeps at most one published chunk in
// memory: the rest of its index is a table in dir.
func openSpilled(t testing.TB, bs blob.BlobStore, kr *seal.Keyring, dir string) *packstore.Store {
	t.Helper()
	o := packstore.Options{Blobs: bs, Keys: kr, Repo: repo, IndexDir: dir, IndexInMemory: 1}
	s, err := packstore.Open(ctx, packstore.WithBackoff(o, time.Millisecond))
	if err != nil {
		t.Fatalf("Open with the index spilled: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// tables lists the index tables in dir.
func tables(t testing.TB, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "snapshot-table-") {
			out = append(out, e.Name())
		}
	}
	return out
}

// #6: a store whose published chunks are over its in-memory bound keeps
// their index in a table on disk. Every chunk reads through it, a writer
// deduplicates against it, chunks published by others are found after a
// refresh, and Close leaves nothing in the directory.
func TestAStoreSpillsItsIndexToDisk(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	dir := t.TempDir()
	a := openSpilled(t, bs, kr, dir)
	var hs []hash.Hash
	var contents [][]byte
	root := hash.Hash{}
	for session := 0; session < 3; session++ {
		var chunks [][]byte
		for i := 0; i < 50; i++ {
			chunks = append(chunks, payload("s"+string(rune('0'+session))+"/"+string(rune('a'+i%26))+string(rune('a'+i/26)), 200))
		}
		got := packed(t, a, root, chunks...)
		root = got[0]
		hs = append(hs, got...)
		contents = append(contents, chunks...)
	}
	b := openSpilled(t, bs, kr, dir)
	if got := tables(t, dir); len(got) != 1 {
		t.Fatalf("positive control: a fresh store over %d published chunks and a bound of one keeps %d tables in %s, want its index in one", len(hs), len(got), dir)
	}
	for i, h := range hs {
		if _, _, _, ok := b.Location(h); !ok {
			t.Fatalf("chunk %d is not located through the table", i)
		}
		got, err := b.Get(ctx, h)
		if err != nil || !bytes.Equal(got, contents[i]) {
			t.Fatalf("chunk %d reads as %d bytes, %v through the table", i, len(got), err)
		}
	}
	if st, err := b.Stats(ctx); err != nil || st.Chunks != int64(len(hs)) {
		t.Fatalf("a spilled store counts %d chunks, %v; want %d", st.Chunks, err, len(hs))
	}
	packs := objects(t, bs, "packs/")
	if _, err := b.Put(ctx, contents[7]); err != nil {
		t.Fatal(err)
	}
	if err := b.CompareAndSetRoot(ctx, root, root); err != nil {
		t.Fatal(err)
	}
	if n := len(newNames(packs, objects(t, bs, "packs/"))); n != 0 {
		t.Fatalf("a writer storing a chunk the table holds wrote %d new packs, want none: it must deduplicate through the table", n)
	}
	fresh := payload("fresh", 300)
	fh, err := b.Put(ctx, fresh)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CompareAndSetRoot(ctx, root, fh); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Root(ctx); err != nil {
		t.Fatal(err)
	}
	if got, err := a.Get(ctx, fh); err != nil || !bytes.Equal(got, fresh) {
		t.Fatalf("after a refresh the other store reads a chunk published meanwhile as %d bytes, %v", len(got), err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("after both stores closed the index directory holds %v, want nothing", names)
	}
}

// When GC moves gcGen the spilled index is rebuilt like the one in memory:
// a chunk in an expired pack is no longer located, and a live chunk a
// repacked pack still holds resolves to its new pack, the packs in service
// coming first in the table too.
func TestASpilledIndexIsRebuiltWhenGCMovesIt(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	dir := t.TempDir()
	w := openSpilled(t, bs, kr, dir)
	live := payload("live", 1<<10)
	hs := packed(t, w, hash.Hash{}, []byte("the root"), payload("dead one", 3<<10), payload("dead two", 3<<10), live)
	gone := packed(t, w, hs[0], []byte("the root"), []byte("unreachable"))
	reader := openSpilled(t, bs, kr, dir)
	if got := tables(t, dir); len(got) != 1 {
		t.Fatalf("positive control: the reader keeps %d tables, want its index in one", len(got))
	}
	old := objects(t, bs, "packs/")
	out := repackRound(t, bs, kr, liveSet(hs[0], hs[3]), t0, packstore.Repack{})
	if out.Repacked != 1 || out.Condemned != 1 {
		t.Fatalf("fixture: repacked %d, condemned %d; want the mixed pack repacked and the unreachable one condemned", out.Repacked, out.Condemned)
	}
	added := newNames(old, objects(t, bs, "packs/"))
	if _, err := reader.Root(ctx); err != nil { // the round moved gcGen: a rebuild
		t.Fatal(err)
	}
	if got, _, _, ok := reader.Location(hs[3]); !ok || got != added[0] {
		t.Fatalf("after the rebuild the reader locates the live chunk in %q (%v), want the new pack %s", got, ok, added[0])
	}
	out = repackRound(t, bs, kr, liveSet(hs[0], hs[3]), t0.Add(time.Hour), packstore.Repack{})
	remove(t, bs, out.Expired)
	if _, err := reader.Root(ctx); err != nil {
		t.Fatal(err)
	}
	if name, _, _, ok := reader.Location(gone[1]); ok {
		t.Fatalf("after its pack expired the unreachable chunk is still located in %s", name)
	}
	if got, err := reader.Get(ctx, hs[3]); err != nil || !bytes.Equal(got, live) {
		t.Fatalf("after the rebuild the live chunk reads as %d bytes, %v", len(got), err)
	}
}

// An index directory that cannot be written is Open's error, not a store
// that silently keeps everything in memory.
func TestASpilledIndexNeedsItsDirectory(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	w := openSpilled(t, bs, kr, t.TempDir())
	packed(t, w, hash.Hash{}, []byte("the root"), payload("more", 100))
	missing := filepath.Join(t.TempDir(), "not", "here")
	// A bound the repository is under: the directory is not needed yet,
	// and Open still refuses rather than fail at some later refresh.
	o := packstore.Options{Blobs: bs, Keys: kr, Repo: repo, IndexDir: missing, IndexInMemory: 1000}
	if s, err := packstore.Open(ctx, o); err == nil {
		_ = s.Close()
		t.Fatalf("a store opened with its index directory %s missing", missing)
	}
	file := filepath.Join(t.TempDir(), "a-file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	o.IndexDir = file
	if s, err := packstore.Open(ctx, o); err == nil {
		_ = s.Close()
		t.Fatalf("a store opened with its index directory %s a file", file)
	}
	o.IndexDir = t.TempDir()
	s, err := packstore.Open(ctx, o)
	if err != nil {
		t.Fatalf("positive control: the same store with a directory that exists: %v", err)
	}
	_ = s.Close()
	// A backend failing while the index is spilled at Open is Open's error:
	// the first read counts the chunks, the spill reads the objects again.
	reads := 0
	fb := &failingNamed{BlobStore: bs, fail: func(name string, get bool) bool {
		if get && strings.HasPrefix(name, "index/") {
			reads++
			return reads > 1
		}
		return false
	}}
	o.Blobs, o.IndexInMemory = fb, 1
	if s, err := packstore.Open(ctx, o); err == nil || !strings.Contains(err.Error(), "injected") {
		if s != nil {
			_ = s.Close()
		}
		t.Fatalf("with the backend failing on index objects, Open with the index spilled returned %v, want the injected failure", err)
	}
}

// A session's packs go into index objects of at most 8 MiB estimated, not
// one object whatever their number: an object is decoded whole by every
// store that opens it, and one over dedup's limit could not be written at
// all. A session over the bound publishes several, and reads whole.
func TestALargeSessionPublishesSeveralIndexObjects(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	o := packstore.WithBackoff(packstore.Options{Blobs: bs, Keys: kr, Repo: repo, PackSize: 64 << 10}, time.Millisecond)
	s, err := packstore.Open(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	const n = 140_000 // 64 bytes an entry estimated: over 8 MiB
	var root hash.Hash
	var last []byte
	for i := 0; i < n; i++ {
		last = fmt.Appendf(last[:0], "chunk %d", i)
		h, err := s.Put(ctx, last)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			root = h
		}
	}
	if err := s.CompareAndSetRoot(ctx, hash.Hash{}, root); err != nil {
		t.Fatal(err)
	}
	if got := len(objects(t, bs, "index/")); got < 2 {
		t.Fatalf("a session of %d chunks published %d index objects, want several of at most 8 MiB each", n, got)
	}
	fresh := open(t, bs, kr)
	for _, i := range []int{0, 1, n / 2, n - 1} {
		want := fmt.Appendf(nil, "chunk %d", i)
		if got, err := fresh.Get(ctx, hash.Sum(want)); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("chunk %d reads as %d bytes, %v from a fresh store", i, len(got), err)
		}
	}
}

// A spilled store whose repository shrinks under the bound (GC expired its
// packs) returns to memory at the rebuild: the table is removed, and an
// expired chunk is not located through a stale one.
func TestASpilledIndexReturnsToMemoryWhenTheRepositoryShrinks(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	dir := t.TempDir()
	o := packstore.Options{Blobs: bs, Keys: kr, Repo: repo, IndexDir: dir, IndexInMemory: 4}
	w, err := packstore.Open(ctx, packstore.WithBackoff(o, time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	live := packed(t, w, hash.Hash{}, []byte("the root"), payload("kept one", 200), payload("kept two", 200))
	dead := packed(t, w, live[0], []byte("the root"), payload("dead one", 200), payload("dead two", 200), payload("dead three", 200))
	reader, err := packstore.Open(ctx, packstore.WithBackoff(o, time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	if got := tables(t, dir); len(got) != 1 {
		t.Fatalf("positive control: over %d published chunks and a bound of 4 the reader keeps %d tables, want one", 6, len(got))
	}
	if out := round(t, bs, kr, liveSet(live...), t0); out.Condemned != 1 {
		t.Fatalf("fixture: condemned %d packs, want the dead one", out.Condemned)
	}
	out := round(t, bs, kr, liveSet(live...), t0.Add(time.Hour))
	if len(packsOf(out.Expired)) != 1 {
		t.Fatalf("fixture: expired %v, want the dead pack", out.Expired)
	}
	remove(t, bs, out.Expired)
	if _, err := reader.Root(ctx); err != nil { // gcGen moved: a rebuild, of 3 chunks under a bound of 4
		t.Fatal(err)
	}
	if got := tables(t, dir); len(got) != 0 {
		t.Fatalf("after the repository shrank to 3 chunks under a bound of 4 the reader still keeps %d tables in %s, want its index back in memory", len(got), dir)
	}
	if name, _, _, ok := reader.Location(dead[1]); ok {
		t.Fatalf("an expired chunk is still located in %s", name)
	}
	for _, h := range live {
		if _, _, _, ok := reader.Location(h); !ok {
			t.Fatalf("a live chunk %s is not located after the rebuild", h.Short())
		}
	}
}

// A round needs its index directory like a store does, and a backend that
// fails while a round reads its index objects or writes its new packs and
// index objects is the round's error: nothing is decided on a half-read
// index, and the round leaves no table behind.
func TestABackendFailureDuringARoundIsTheRoundsError(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	s := open(t, bs, kr)
	live := payload("live", 1<<10)
	hs := packed(t, s, hash.Hash{}, []byte("the root"), payload("dead one", 3<<10), payload("dead two", 3<<10), live)
	dir := t.TempDir()
	missing := filepath.Join(dir, "not", "here")
	if r, err := packstore.Begin(ctx, packstore.Options{Blobs: bs, Keys: kr, Repo: repo, IndexDir: missing}); err == nil {
		_ = r.Close()
		t.Fatalf("a round began with its index directory %s missing", missing)
	}
	isIndex := func(name string, get bool) bool { return get && strings.HasPrefix(name, "index/") }
	reads := 0
	for _, tc := range []struct {
		name string
		fail func(name string, get bool) bool
	}{
		{"reading an index object", isIndex},
		{"reading an index object again to rewrite it", func(name string, get bool) bool {
			if isIndex(name, get) {
				reads++
				return reads > 2 // Begin's read and the candidate's; the rewrite's fails
			}
			return false
		}},
		{"reading a pack to repack it", func(name string, get bool) bool { return get && strings.HasPrefix(name, "packs/") }},
		{"writing a new pack", func(name string, get bool) bool { return !get && strings.HasPrefix(name, "packs/") }},
		{"writing an index object", func(name string, get bool) bool { return !get && strings.HasPrefix(name, "index/") }},
	} {
		fb := &failingNamed{BlobStore: bs, fail: tc.fail}
		r, err := packstore.Begin(ctx, packstore.Options{Blobs: fb, Keys: kr, Repo: repo, IndexDir: dir})
		if err == nil {
			_, err = r.Apply(ctx, liveSet(hs[0], hs[3]), t0, time.Hour)
		}
		if err == nil || !strings.Contains(err.Error(), "injected") {
			t.Fatalf("%s failing, the round returned %v, want the injected failure", tc.name, err)
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Fatalf("%s failing, the round left %d files in %s, want none", tc.name, len(entries), dir)
		}
	}
	if out := repackRound(t, bs, kr, liveSet(hs[0], hs[3]), t0, packstore.Repack{}); out.Repacked != 1 {
		t.Fatalf("positive control: with the backend well the round repacked %d", out.Repacked)
	}
}

// failingNamed fails Get or Put for the objects fail picks.
type failingNamed struct {
	blob.BlobStore
	fail func(name string, get bool) bool
}

func (f *failingNamed) Get(ctx context.Context, name string, off, n int64) (io.ReadCloser, error) {
	if f.fail(name, true) {
		return nil, errors.New("injected: backend unavailable")
	}
	return f.BlobStore.Get(ctx, name, off, n)
}

func (f *failingNamed) Put(ctx context.Context, name string, r io.Reader, size int64) error {
	if f.fail(name, false) {
		return errors.New("injected: backend unavailable")
	}
	return f.BlobStore.Put(ctx, name, r, size)
}

// A record in an index table that is not a location, or that names a pack
// the table does not list, is corrupt: a store's lookup and a round's join
// both refuse it rather than read a pack that is not there.
func TestAForgedIndexTableRecordIsRefused(t *testing.T) {
	key := hash.Sum([]byte("a chunk"))
	table := func(valueLen int, value []byte) *dedup.Table {
		b, err := dedup.NewBuilder(t.TempDir(), valueLen)
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Add(key, value); err != nil {
			t.Fatal(err)
		}
		tb, err := b.Finish()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = tb.Close() })
		return tb
	}
	good := make([]byte, 17) // pack 0
	if _, ok, err := packstore.ForgedSpilled(table(17, good), 1).Lookup(key); !ok || err != nil {
		t.Fatalf("positive control: a well-formed record: %v %v", ok, err)
	}
	if err := packstore.JoinForged(table(17, good), 1, liveSet(key)); err != nil {
		t.Fatalf("positive control: a well-formed record joins: %v", err)
	}
	pack9 := make([]byte, 17)
	pack9[0] = 9
	for name, tc := range map[string]struct {
		valueLen int
		value    []byte
	}{
		"a record of another width":      {16, make([]byte, 16)},
		"a pack the table does not list": {17, pack9},
	} {
		if _, ok, err := packstore.ForgedSpilled(table(tc.valueLen, tc.value), 1).Lookup(key); !errors.Is(err, chunk.ErrCorrupt) {
			t.Fatalf("%s: a lookup answered %v, %v; want ErrCorrupt", name, ok, err)
		}
		if err := packstore.JoinForged(table(tc.valueLen, tc.value), 1, liveSet(key)); !errors.Is(err, chunk.ErrCorrupt) {
			t.Fatalf("%s: a join answered %v, want ErrCorrupt", name, err)
		}
	}
}
