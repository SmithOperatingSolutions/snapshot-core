// Package mem is an in-memory blob.BlobStore, for tests. It defines the
// contract: every other backend must behave as it does.
package mem

import (
	"bytes"
	"context"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
)

type object struct {
	data []byte
	mod  time.Time
}

// Store is an in-memory BlobStore.
type Store struct {
	mu      sync.Mutex
	objects map[string]object
	root    []byte
	version blob.Version
	mirror  []byte // the root's copy a split store keeps here
}

// New returns an empty store.
func New() *Store { return &Store{objects: map[string]object{}} }

var _ blob.BlobStore = (*Store)(nil)

// Put implements blob.BlobStore. The body is read in full before the name is
// claimed, so an object is visible only once complete.
func (s *Store) Put(ctx context.Context, name string, r io.Reader, size int64) error {
	if err := blob.CheckPut(name, size); err != nil {
		return err
	}
	var buf bytes.Buffer
	if err := blob.CopyExact(&buf, r, size); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.objects[name]; ok {
		return blob.ErrExists
	}
	s.objects[name] = object{data: buf.Bytes(), mod: time.Now()}
	return nil
}

// Get implements blob.BlobStore.
func (s *Store) Get(ctx context.Context, name string, off, n int64) (io.ReadCloser, error) {
	if err := blob.ValidName(name); err != nil {
		return nil, err
	}
	s.mu.Lock()
	o, ok := s.objects[name]
	s.mu.Unlock()
	if !ok {
		return nil, blob.ErrNotFound
	}
	start, length, err := blob.Range(off, n, int64(len(o.data)))
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(o.data[start : start+length])), nil
}

// Stat implements blob.BlobStore.
func (s *Store) Stat(ctx context.Context, name string) (blob.Info, error) {
	if err := blob.ValidName(name); err != nil {
		return blob.Info{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.objects[name]
	if !ok {
		return blob.Info{}, blob.ErrNotFound
	}
	return blob.Info{Name: name, Size: int64(len(o.data)), ModTime: o.mod}, nil
}

// List implements blob.BlobStore.
func (s *Store) List(ctx context.Context, prefix, after string, limit int) ([]blob.Info, error) {
	if err := blob.CheckList(prefix, limit); err != nil {
		return nil, err
	}
	s.mu.Lock()
	var infos []blob.Info
	for name, o := range s.objects {
		if strings.HasPrefix(name, prefix) && name > after {
			infos = append(infos, blob.Info{Name: name, Size: int64(len(o.data)), ModTime: o.mod})
		}
	}
	s.mu.Unlock()
	sort.Slice(infos, func(i, j int) bool { return infos[i].Name < infos[j].Name })
	if len(infos) > limit {
		infos = infos[:limit]
	}
	return infos, nil
}

// Delete implements blob.BlobStore.
func (s *Store) Delete(ctx context.Context, name string) error {
	if err := blob.ValidName(name); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, name)
	return nil
}

// Root implements blob.BlobStore.
func (s *Store) Root(ctx context.Context) (blob.Root, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return blob.Root{Value: bytes.Clone(s.root), Version: s.version}, nil
}

// SwapRoot implements blob.BlobStore.
func (s *Store) SwapRoot(ctx context.Context, expected blob.Version, next []byte) (blob.Version, error) {
	if err := blob.CheckRootValue(next); err != nil {
		return blob.NoVersion, err
	}
	v, err := blob.NewVersion()
	if err != nil {
		return blob.NoVersion, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.version != expected {
		return blob.NoVersion, blob.ErrRootConflict
	}
	s.root, s.version = bytes.Clone(next), v
	return v, nil
}

// WriteMirror replaces the root's copy a split store keeps here (blob/split).
func (s *Store) WriteMirror(ctx context.Context, value []byte) error {
	if err := blob.CheckRootValue(value); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mirror = bytes.Clone(value)
	return nil
}

// ReadMirror returns the root's copy, or nothing when there is none.
func (s *Store) ReadMirror(ctx context.Context) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return bytes.Clone(s.mirror), nil
}
