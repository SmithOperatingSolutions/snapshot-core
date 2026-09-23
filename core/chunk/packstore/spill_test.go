package packstore_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/mem"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/packstore"
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
	o.IndexDir = t.TempDir()
	s, err := packstore.Open(ctx, o)
	if err != nil {
		t.Fatalf("positive control: the same store with a directory that exists: %v", err)
	}
	_ = s.Close()
}
