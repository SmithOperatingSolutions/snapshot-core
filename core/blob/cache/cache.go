// Package cache is a size-capped local disk cache in front of a BlobStore,
// for the S3 read path (Engine Spec s3store: "range GETs ... through a
// size-capped local disk cache (default 10 GiB, dir 0700)"). Only immutable
// objects (packs/, index/) are cached; everything else passes through. Each
// cache file carries the SHA-256 of what it holds, so a damaged entry is
// refetched rather than served. Cached bytes are ciphertext frames: nothing
// in the cache is plaintext, and the chunk layer re-verifies every chunk anyway.
//
// A Delete through this store evicts the object; a delete by another process
// is not seen, which is harmless: cached objects are immutable and named by
// their content, so a stale entry is still the right bytes.
package cache

import (
	"bytes"
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/internal/wire"
)

// DefaultMaxBytes is the Engine Spec's default.
const DefaultMaxBytes = 10 << 30

const (
	entryMagic        = "SCCE"
	defaultEntryLimit = 64 << 20 // larger reads pass through uncached
	dirPerm           = 0o700
)

// Options configures a cache.
type Options struct {
	Dir      string
	MaxBytes int64 // 0: DefaultMaxBytes
}

type entry struct {
	key  string
	name string
	size int64 // bytes on disk
}

// Store is a caching BlobStore.
type Store struct {
	blob.BlobStore
	dir        string
	max        int64
	entryLimit int64

	mu     sync.Mutex
	used   int64
	lru    *list.List // of *entry; front is most recent
	items  map[string]*list.Element
	byName map[string]map[string]bool
}

var _ blob.BlobStore = (*Store)(nil)

// New wraps inner with a cache in o.Dir, reusing what a previous process left.
func New(inner blob.BlobStore, o Options) (*Store, error) {
	if o.Dir == "" {
		return nil, errors.New("cache: Dir is required")
	}
	if o.MaxBytes == 0 {
		o.MaxBytes = DefaultMaxBytes
	}
	if err := os.MkdirAll(o.Dir, dirPerm); err != nil {
		return nil, err
	}
	if err := os.Chmod(o.Dir, dirPerm); err != nil {
		return nil, err
	}
	s := &Store{BlobStore: inner, dir: o.Dir, max: o.MaxBytes, entryLimit: defaultEntryLimit, lru: list.New(),
		items: map[string]*list.Element{}, byName: map[string]map[string]bool{}}
	if err := s.reload(); err != nil {
		return nil, err
	}
	return s, nil
}

// reload indexes the files a previous process left, oldest first, dropping
// anything that is not a whole entry.
func (s *Store) reload() error {
	des, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	type found struct {
		e   *entry
		mod time.Time
	}
	var all []found
	for _, de := range des {
		p := filepath.Join(s.dir, de.Name())
		if !de.Type().IsRegular() || strings.HasPrefix(de.Name(), ".") {
			_ = os.Remove(p)
			continue
		}
		name, err := readHeaderName(p)
		info, ierr := de.Info()
		if err != nil || ierr != nil {
			_ = os.Remove(p)
			continue
		}
		all = append(all, found{e: &entry{key: de.Name(), name: name, size: info.Size()}, mod: info.ModTime()})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].mod.Before(all[j].mod) })
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, f := range all {
		s.insertLocked(f.e)
	}
	s.evictLocked()
	return nil
}

// Entry file: magic "SCCE" | object name (len-prefixed) | SHA-256 of content | content.
func readHeaderName(path string) (string, error) {
	f, err := os.Open(path) //nolint:gosec // G304: a file inside the cache directory
	if err != nil {
		return "", err
	}
	defer f.Close()
	head := make([]byte, 4+2+blob.MaxNameLen+32)
	n, _ := io.ReadFull(f, head)
	r := wire.NewReader(head[:n])
	magic, name := r.Fixed(4), r.LenBytes(blob.MaxNameLen)
	r.Fixed(32)
	if r.Err() != nil || string(magic) != entryMagic || blob.ValidName(string(name)) != nil {
		return "", fmt.Errorf("cache: bad entry %s", filepath.Base(path))
	}
	return string(name), nil
}

func encodeEntry(name string, content []byte) []byte {
	var w wire.Writer
	w.Raw([]byte(entryMagic))
	w.LenBytes([]byte(name))
	sum := sha256.Sum256(content)
	w.Raw(sum[:])
	w.Raw(content)
	return w.Bytes()
}

func decodeEntry(b []byte, name string) ([]byte, bool) {
	r := wire.NewReader(b)
	magic, n, sum := r.Fixed(4), r.LenBytes(blob.MaxNameLen), r.Fixed(32)
	if r.Err() != nil || string(magic) != entryMagic || string(n) != name {
		return nil, false
	}
	content := r.Fixed(r.Remaining())
	if got := sha256.Sum256(content); !bytes.Equal(got[:], sum) {
		return nil, false
	}
	return content, true
}

func cacheable(name string) bool {
	return strings.HasPrefix(name, "packs/") || strings.HasPrefix(name, "index/")
}

func keyFor(name string, off, n int64) string {
	sum := sha256.Sum256([]byte(name + "\x00" + strconv.FormatInt(off, 10) + "\x00" + strconv.FormatInt(n, 10)))
	return hex.EncodeToString(sum[:])
}

// Used reports the bytes the cache holds.
func (s *Store) Used() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.used
}

func (s *Store) insertLocked(e *entry) {
	if old, ok := s.items[e.key]; ok {
		s.removeLocked(old)
	}
	s.items[e.key] = s.lru.PushFront(e)
	s.used += e.size
	if s.byName[e.name] == nil {
		s.byName[e.name] = map[string]bool{}
	}
	s.byName[e.name][e.key] = true
}

func (s *Store) removeLocked(el *list.Element) {
	e := el.Value.(*entry)
	s.lru.Remove(el)
	delete(s.items, e.key)
	delete(s.byName[e.name], e.key)
	if len(s.byName[e.name]) == 0 {
		delete(s.byName, e.name)
	}
	s.used -= e.size
	_ = os.Remove(filepath.Join(s.dir, e.key))
}

func (s *Store) evictLocked() {
	for s.used > s.max && s.lru.Len() > 0 {
		s.removeLocked(s.lru.Back())
	}
}

// Get implements blob.BlobStore.
func (s *Store) Get(ctx context.Context, name string, off, n int64) (io.ReadCloser, error) {
	if !cacheable(name) {
		return s.BlobStore.Get(ctx, name, off, n)
	}
	key := keyFor(name, off, n)
	s.mu.Lock()
	el, hit := s.items[key]
	if hit {
		s.lru.MoveToFront(el)
	}
	s.mu.Unlock()
	if hit {
		b, err := os.ReadFile(filepath.Join(s.dir, key)) //nolint:gosec // G304: a file inside the cache directory
		if err == nil {
			if content, ok := decodeEntry(b, name); ok {
				return io.NopCloser(bytes.NewReader(content)), nil
			}
		}
		s.mu.Lock()
		if el, ok := s.items[key]; ok { // damaged: drop it and refetch
			s.removeLocked(el)
		}
		s.mu.Unlock()
	}
	rc, err := s.BlobStore.Get(ctx, name, off, n)
	if err != nil {
		return nil, err
	}
	head, err := io.ReadAll(io.LimitReader(rc, s.entryLimit+1))
	if err != nil {
		_ = rc.Close()
		return nil, err
	}
	if int64(len(head)) > s.entryLimit {
		return readCloser{Reader: io.MultiReader(bytes.NewReader(head), rc), c: rc}, nil // too big to cache
	}
	_ = rc.Close()
	s.store(key, name, head)
	return io.NopCloser(bytes.NewReader(head)), nil
}

type readCloser struct {
	io.Reader
	c io.Closer
}

func (r readCloser) Close() error { return r.c.Close() }

// store writes one entry atomically; a failure only means a later miss.
func (s *Store) store(key, name string, content []byte) {
	b := encodeEntry(name, content)
	if int64(len(b)) > s.max {
		return
	}
	tmp, err := os.CreateTemp(s.dir, ".tmp-*")
	if err != nil {
		return
	}
	_, werr := tmp.Write(b)
	cerr := tmp.Close()
	if werr != nil || cerr != nil || os.Rename(tmp.Name(), filepath.Join(s.dir, key)) != nil {
		_ = os.Remove(tmp.Name())
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.insertLocked(&entry{key: key, name: name, size: int64(len(b))})
	s.evictLocked()
}

// Delete implements blob.BlobStore, evicting the object's cached ranges.
func (s *Store) Delete(ctx context.Context, name string) error {
	if err := s.BlobStore.Delete(ctx, name); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for key := range s.byName[name] {
		if el, ok := s.items[key]; ok {
			s.removeLocked(el)
		}
	}
	return nil
}
