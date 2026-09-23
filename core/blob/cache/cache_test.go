package cache_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/cache"
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
