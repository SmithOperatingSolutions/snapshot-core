package cache_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/cache"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/contract"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/mem"
)

var ctx = context.Background()

// counting records how many reads reach the backend.
type counting struct {
	blob.BlobStore
	gets atomic.Int64
}

func (c *counting) Get(ctx context.Context, name string, off, n int64) (io.ReadCloser, error) {
	c.gets.Add(1)
	return c.BlobStore.Get(ctx, name, off, n)
}

func newCache(t *testing.T, inner blob.BlobStore, max int64) (*cache.Store, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "cache")
	c, err := cache.New(inner, cache.Options{Dir: dir, MaxBytes: max})
	if err != nil {
		t.Fatal(err)
	}
	return c, dir
}

func payload(seed string, n int) []byte {
	out := make([]byte, 0, n+32)
	for i := 0; len(out) < n; i++ {
		h := sha256.Sum256([]byte(fmt.Sprintf("%s/%d", seed, i)))
		out = append(out, h[:]...)
	}
	return out[:n]
}

func read(t *testing.T, s blob.BlobStore, name string, off, n int64) []byte {
	t.Helper()
	rc, err := s.Get(ctx, name, off, n)
	if err != nil {
		t.Fatalf("Get(%s, %d, %d): %v", name, off, n, err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// The cache is transparent: it passes the whole blob contract.
func TestContractThroughTheCache(t *testing.T) {
	contract.Run(t, func(t *testing.T) blob.BlobStore {
		c, _ := newCache(t, mem.New(), 64<<20)
		return c
	}, contract.Options{})
}

func TestRepeatedRangeReadsHitTheCache(t *testing.T) {
	inner := &counting{BlobStore: mem.New()}
	c, _ := newCache(t, inner, 64<<20)
	data := payload("pack", 100000)
	if err := c.Put(ctx, "packs/ab/x", bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if got := read(t, c, "packs/ab/x", 1000, 5000); !bytes.Equal(got, data[1000:6000]) {
			t.Fatal("a cached range read returned the wrong bytes")
		}
	}
	if n := inner.gets.Load(); n != 1 {
		t.Fatalf("5 reads of one range reached the backend %d times, want 1: nothing was cached", n)
	}
	if c.Used() == 0 {
		t.Fatal("Used() is 0 after caching a range")
	}
}

// Only immutable objects are cached: a pack is (the positive control), a
// probe object is not.
func TestOnlyImmutableObjectsAreCached(t *testing.T) {
	inner := &counting{BlobStore: mem.New()}
	c, _ := newCache(t, inner, 64<<20)
	for _, name := range []string{"packs/dd/p", "probe/x"} {
		if err := c.Put(ctx, name, bytes.NewReader([]byte("x")), 1); err != nil {
			t.Fatal(err)
		}
	}
	read(t, c, "packs/dd/p", 0, -1)
	read(t, c, "packs/dd/p", 0, -1)
	if n := inner.gets.Load(); n != 1 {
		t.Fatalf("two reads of a pack reached the backend %d times, want 1", n)
	}
	read(t, c, "probe/x", 0, -1)
	read(t, c, "probe/x", 0, -1)
	if n := inner.gets.Load(); n != 3 {
		t.Fatalf("an object outside packs/ and index/ was served from the cache (%d backend reads, want 3)", n)
	}
}

func TestSizeCapEvictsLeastRecentlyUsed(t *testing.T) {
	inner := &counting{BlobStore: mem.New()}
	const max = 50000
	c, dir := newCache(t, inner, max)
	for i := 0; i < 10; i++ {
		d := payload(fmt.Sprintf("p%d", i), 20000)
		name := fmt.Sprintf("packs/aa/%d", i)
		if err := c.Put(ctx, name, bytes.NewReader(d), int64(len(d))); err != nil {
			t.Fatal(err)
		}
		read(t, c, name, 0, -1)
		read(t, c, "packs/aa/0", 0, -1) // keep the first one hot
	}
	if c.Used() > max {
		t.Fatalf("the cache holds %d bytes, over its %d cap", c.Used(), max)
	}
	var onDisk int64
	_ = filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			if info, e := d.Info(); e == nil {
				onDisk += info.Size()
			}
		}
		return nil
	})
	if onDisk > max+4096 {
		t.Fatalf("%d bytes on disk for a %d-byte cache", onDisk, max)
	}
	before := inner.gets.Load()
	read(t, c, "packs/aa/0", 0, -1)
	if inner.gets.Load() != before {
		t.Fatal("the most recently used entry was evicted")
	}
}

// A hit makes an entry recent: with room for two, caching A and B, reading
// A, then caching C must evict B, not A. (Nothing here re-reads a victim, so
// a refetch cannot mask a wrong eviction.)
func TestHitsRefreshRecency(t *testing.T) {
	inner := &counting{BlobStore: mem.New()}
	c, _ := newCache(t, inner, 45000) // room for two 20 KB entries
	for _, n := range []string{"packs/ee/a", "packs/ee/b", "packs/ee/c"} {
		d := payload(n, 20000)
		if err := c.Put(ctx, n, bytes.NewReader(d), int64(len(d))); err != nil {
			t.Fatal(err)
		}
	}
	read(t, c, "packs/ee/a", 0, -1)
	read(t, c, "packs/ee/b", 0, -1)
	read(t, c, "packs/ee/a", 0, -1) // a hit: a is now the most recent
	read(t, c, "packs/ee/c", 0, -1) // over the cap: the least recent (b) goes
	before := inner.gets.Load()
	read(t, c, "packs/ee/a", 0, -1)
	if inner.gets.Load() != before {
		t.Fatal("the entry read most recently was evicted: hits do not refresh recency")
	}
	read(t, c, "packs/ee/b", 0, -1)
	if inner.gets.Load() != before+1 {
		t.Fatal("the least recently used entry survived the eviction")
	}
}

// A damaged cache file is refetched, never served.
func TestDamagedEntryIsRefetched(t *testing.T) {
	inner := &counting{BlobStore: mem.New()}
	c, dir := newCache(t, inner, 64<<20)
	data := payload("heal", 30000)
	if err := c.Put(ctx, "index/abc", bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	read(t, c, "index/abc", 0, -1)
	damaged := 0
	_ = filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			b, _ := os.ReadFile(p)
			if len(b) > 100 {
				b[len(b)-10] ^= 1
				_ = os.WriteFile(p, b, 0o600)
				damaged++
			}
		}
		return nil
	})
	if damaged == 0 {
		t.Fatal("no cache file to damage; the fixture tests nothing")
	}
	if got := read(t, c, "index/abc", 0, -1); !bytes.Equal(got, data) {
		t.Fatal("a damaged cache entry was served")
	}
	if n := inner.gets.Load(); n != 2 {
		t.Fatalf("the damaged entry reached the backend %d times in all, want 2 (fill, refetch)", n)
	}
}

func TestDeleteEvictsAndRestartsReuseTheCache(t *testing.T) {
	inner := &counting{BlobStore: mem.New()}
	c, dir := newCache(t, inner, 64<<20)
	data := payload("warm", 10000)
	if err := c.Put(ctx, "packs/bb/w", bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	read(t, c, "packs/bb/w", 0, -1)
	re, err := cache.New(inner, cache.Options{Dir: dir, MaxBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	before := inner.gets.Load()
	if got := read(t, re, "packs/bb/w", 0, -1); !bytes.Equal(got, data) || inner.gets.Load() != before {
		t.Fatal("a restarted cache did not reuse what the last process cached")
	}
	if err := re.Delete(ctx, "packs/bb/w"); err != nil {
		t.Fatal(err)
	}
	if _, err := re.Get(ctx, "packs/bb/w", 0, -1); err == nil {
		t.Fatal("a deleted object is still served from the cache")
	}
	if re.Used() != 0 {
		t.Fatalf("after Delete the cache still holds %d bytes", re.Used())
	}
}

func TestCacheFilesAreOwnerOnly(t *testing.T) {
	c, dir := newCache(t, mem.New(), 64<<20)
	if err := c.Put(ctx, "packs/cc/p", bytes.NewReader([]byte("abc")), 3); err != nil {
		t.Fatal(err)
	}
	read(t, c, "packs/cc/p", 0, -1)
	checked := 0
	_ = filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err == nil {
			info, _ := d.Info()
			checked++
			if info.Mode().Perm()&0o077 != 0 {
				t.Errorf("%s has mode %v", p, info.Mode().Perm())
			}
		}
		return nil
	})
	if checked < 2 {
		t.Fatal("the cache wrote nothing to check")
	}
}

// A restart keeps only whole entries: a crash's temp file (even one holding
// a complete entry, written but never renamed), a garbled or truncated file,
// and anything that is not a regular file are removed and never counted.
func TestRestartKeepsOnlyWholeEntries(t *testing.T) {
	inner := &counting{BlobStore: mem.New()}
	c, dir := newCache(t, inner, 64<<20)
	names := []string{"packs/aa/one", "packs/aa/two"}
	for _, n := range names {
		d := payload(n, 3000)
		if err := c.Put(ctx, n, bytes.NewReader(d), int64(len(d))); err != nil {
			t.Fatal(err)
		}
		read(t, c, n, 0, -1)
	}
	kept := c.Used()
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 2 {
		t.Fatalf("fixture: %d cache files (%v), want 2", len(files), err)
	}
	whole, err := os.ReadFile(filepath.Join(dir, files[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	for name, b := range map[string][]byte{
		".tmp-crashed": whole, // written, never renamed
		"garbled":      []byte("not a cache entry"),
		"truncated":    whole[:5],
	} {
		if err := os.WriteFile(filepath.Join(dir, name), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(filepath.Join(dir, files[0].Name()), filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}

	re, err := cache.New(inner, cache.Options{Dir: dir, MaxBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{".tmp-crashed", "garbled", "truncated", "link"} {
		if _, err := os.Lstat(filepath.Join(dir, name)); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("%s survived a restart (Lstat: %v)", name, err)
		}
	}
	if re.Used() != kept {
		t.Errorf("after a restart the cache counts %d bytes, want %d: something not a whole entry was indexed", re.Used(), kept)
	}
	before := inner.gets.Load()
	for _, n := range names {
		if got := read(t, re, n, 0, -1); !bytes.Equal(got, payload(n, 3000)) {
			t.Fatalf("%s read back wrong after a restart", n)
		}
	}
	if inner.gets.Load() != before {
		t.Error("the whole entries were not reused after the restart")
	}
}

// A read past the entry limit streams through whole and is not kept: the
// cache never buffers an unbounded object. Positive control: a read at the
// limit is kept.
func TestReadsPastTheEntryLimitPassThrough(t *testing.T) {
	inner := &counting{BlobStore: mem.New()}
	c, _ := newCache(t, inner, 64<<20)
	cache.SetEntryLimit(c, 1000)
	data := payload("big", 5000)
	if err := c.Put(ctx, "packs/ff/big", bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	read(t, c, "packs/ff/big", 0, 1000)
	read(t, c, "packs/ff/big", 0, 1000)
	if n := inner.gets.Load(); n != 1 {
		t.Fatalf("positive control: two reads at the entry limit reached the backend %d times, want 1", n)
	}
	kept := c.Used()
	for i := 0; i < 2; i++ {
		if got := read(t, c, "packs/ff/big", 0, -1); !bytes.Equal(got, data) {
			t.Fatalf("a read past the entry limit returned %d bytes, want the whole %d", len(got), len(data))
		}
	}
	if n := inner.gets.Load(); n != 3 {
		t.Errorf("reads past the entry limit reached the backend %d times in all, want 3: one was served from the cache", n)
	}
	if c.Used() != kept {
		t.Errorf("a read past the entry limit was kept: the cache grew from %d to %d bytes", kept, c.Used())
	}
}

// An object bigger than the whole cache is not kept, and does not flush the
// entries already there on its way through.
func TestAnObjectBiggerThanTheCacheDoesNotFlushIt(t *testing.T) {
	inner := &counting{BlobStore: mem.New()}
	c, _ := newCache(t, inner, 2000)
	small, big := payload("small", 500), payload("big", 3000)
	for name, d := range map[string][]byte{"packs/gg/small": small, "packs/gg/big": big} {
		if err := c.Put(ctx, name, bytes.NewReader(d), int64(len(d))); err != nil {
			t.Fatal(err)
		}
	}
	read(t, c, "packs/gg/small", 0, -1)
	if got := read(t, c, "packs/gg/big", 0, -1); !bytes.Equal(got, big) {
		t.Fatal("an object bigger than the cache read back wrong")
	}
	before := inner.gets.Load()
	read(t, c, "packs/gg/small", 0, -1)
	if inner.gets.Load() != before {
		t.Error("reading an object bigger than the cache evicted what the cache held")
	}
	read(t, c, "packs/gg/big", 0, -1)
	if inner.gets.Load() != before+1 {
		t.Error("an object bigger than the whole cache was served from it")
	}
}

var errMidRead = errors.New("connection reset mid-read")

// flaky's reads fail halfway through while fail is set; it counts the
// readers it hands out and the ones closed.
type flaky struct {
	blob.BlobStore
	fail           atomic.Bool
	opened, closed atomic.Int64
}

type body struct {
	r      io.Reader
	fail   bool
	closed *atomic.Int64
}

func (b *body) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	if err == io.EOF && b.fail {
		return n, errMidRead
	}
	return n, err
}

func (b *body) Close() error {
	b.closed.Add(1)
	return nil
}

func (f *flaky) Get(ctx context.Context, name string, off, n int64) (io.ReadCloser, error) {
	rc, err := f.BlobStore.Get(ctx, name, off, n)
	if err != nil {
		return nil, err
	}
	b, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		return nil, err
	}
	f.opened.Add(1)
	if f.fail.Load() {
		return &body{r: bytes.NewReader(b[:len(b)/2]), fail: true, closed: &f.closed}, nil
	}
	return &body{r: bytes.NewReader(b), closed: &f.closed}, nil
}

// A backend read that fails partway is the caller's error: the backend's
// reader (an S3 response body) is closed, and the half that arrived is not
// kept to be served later as the object.
func TestAFailedBackendReadIsReturnedAndNotKept(t *testing.T) {
	inner := &flaky{BlobStore: mem.New()}
	c, _ := newCache(t, inner, 64<<20)
	data := payload("flaky", 4000)
	if err := c.Put(ctx, "packs/hh/f", bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	inner.fail.Store(true)
	rc, err := c.Get(ctx, "packs/hh/f", 0, -1)
	if err == nil {
		_ = rc.Close()
	}
	if !errors.Is(err, errMidRead) {
		t.Fatalf("a read that failed mid-stream = %v, want the backend's error", err)
	}
	if o, cl := inner.opened.Load(), inner.closed.Load(); o != 1 || cl != 1 {
		t.Errorf("the backend handed out %d readers and %d were closed, want 1 and 1", o, cl)
	}
	if c.Used() != 0 {
		t.Errorf("a failed read left %d bytes in the cache", c.Used())
	}
	inner.fail.Store(false)
	if got := read(t, c, "packs/hh/f", 0, -1); !bytes.Equal(got, data) {
		t.Fatal("once the backend healed, the read returned the wrong bytes")
	}
}

// Options.MaxBytes 0 is the documented default, not a cache of size zero
// that keeps nothing.
func TestZeroMaxBytesIsTheDefault(t *testing.T) {
	inner := &counting{BlobStore: mem.New()}
	c, _ := newCache(t, inner, 0)
	d := payload("default", 1000)
	if err := c.Put(ctx, "packs/ii/d", bytes.NewReader(d), int64(len(d))); err != nil {
		t.Fatal(err)
	}
	read(t, c, "packs/ii/d", 0, -1)
	read(t, c, "packs/ii/d", 0, -1)
	if n := inner.gets.Load(); n != 1 {
		t.Fatalf("with MaxBytes 0, two reads reached the backend %d times, want 1: the default cap was not applied", n)
	}
}

// A cache that cannot write (a full or read-only disk) still serves every
// read, from the backend: failing to cache only means a later miss. Access
// is denied with chmod, which root ignores.
func TestACacheThatCannotWriteStillServesReads(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Fatal("this test denies writes with chmod, which root ignores: run the suite as an ordinary user")
	}
	inner := &counting{BlobStore: mem.New()}
	c, dir := newCache(t, inner, 64<<20)
	d := payload("read-only", 2000)
	if err := c.Put(ctx, "packs/jj/r", bytes.NewReader(d), int64(len(d))); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	for i := 0; i < 2; i++ {
		if got := read(t, c, "packs/jj/r", 0, -1); !bytes.Equal(got, d) {
			t.Fatal("a cache that cannot write returned the wrong bytes")
		}
	}
	if n := inner.gets.Load(); n != 2 {
		t.Errorf("%d backend reads, want 2: nothing could have been cached", n)
	}
	if c.Used() != 0 {
		t.Errorf("a cache that cannot write counts %d bytes", c.Used())
	}
}

// plain is a store that keeps no copy of the root.
type plain struct{ blob.BlobStore }

// The root's copy a split store keeps passes through the cache to the store
// behind it, never cached; a cache over a store that keeps none refuses it.
func TestTheRootsCopyPassesThroughTheCache(t *testing.T) {
	inner := mem.New()
	c, _ := newCache(t, inner, 1<<20)
	if err := c.WriteMirror(ctx, []byte("a root")); err != nil {
		t.Fatalf("WriteMirror through the cache = %v", err)
	}
	if got, err := inner.ReadMirror(ctx); err != nil || string(got) != "a root" {
		t.Fatalf("the store behind the cache holds the copy %q, %v; want %q", got, err, "a root")
	}
	if got, err := c.ReadMirror(ctx); err != nil || string(got) != "a root" {
		t.Fatalf("the copy read through the cache is %q, %v; want %q", got, err, "a root")
	}
	over, _ := newCache(t, plain{mem.New()}, 1<<20)
	if err := over.WriteMirror(ctx, []byte("a root")); err == nil {
		t.Fatal("a cache over a store that keeps no copy of the root took one")
	}
}
