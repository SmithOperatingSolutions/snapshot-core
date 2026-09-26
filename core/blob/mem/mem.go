// Package mem is an in-memory blob.BlobStore, for tests. It defines the
// contract: every other backend must behave as it does.
package mem

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
	journal []byte // blob.Journaler
	jopen   bool   // a writer has the journal open
	jholds  int    // HoldJournal calls not yet released
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

var _ blob.Journaler = (*Store)(nil)

// OpenJournal implements blob.Journaler. The journal is the store's memory,
// kept across every handle.
func (s *Store) OpenJournal(ctx context.Context) (blob.Journal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.jopen || s.jholds > 0 {
		return nil, blob.ErrJournalBusy
	}
	s.jopen = true
	return &journal{s: s}, nil
}

// HoldJournal implements blob.Journaler.
func (s *Store) HoldJournal(ctx context.Context) (int64, func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.jopen {
		return 0, nil, blob.ErrJournalBusy
	}
	s.jholds++
	var once sync.Once
	return int64(len(s.journal)), func() {
		once.Do(func() {
			s.mu.Lock()
			s.jholds--
			s.mu.Unlock()
		})
	}, nil
}

// journal is the open journal of a memory store.
type journal struct {
	s      *Store
	closed bool
}

func (j *journal) Read(ctx context.Context, limit int64) ([]byte, error) {
	j.s.mu.Lock()
	defer j.s.mu.Unlock()
	if j.closed {
		return nil, blob.ErrJournalClosed
	}
	if int64(len(j.s.journal)) > limit {
		return nil, fmt.Errorf("%w: the journal holds %d bytes, over %d", blob.ErrTooLarge, len(j.s.journal), limit)
	}
	return bytes.Clone(j.s.journal), nil
}

func (j *journal) Append(ctx context.Context, b []byte) error {
	j.s.mu.Lock()
	defer j.s.mu.Unlock()
	if j.closed {
		return blob.ErrJournalClosed
	}
	j.s.journal = append(j.s.journal, b...)
	return nil
}

func (j *journal) Reset(ctx context.Context) error {
	j.s.mu.Lock()
	defer j.s.mu.Unlock()
	if j.closed {
		return blob.ErrJournalClosed
	}
	j.s.journal = nil
	return nil
}

func (j *journal) Close() error {
	j.s.mu.Lock()
	defer j.s.mu.Unlock()
	if !j.closed {
		j.closed = true
		j.s.jopen = false
	}
	return nil
}

// JournalByDefault implements blob.Journaler: off in memory, where a
// commit is CPU and the journal measured worse with many writers (#34).
func (s *Store) JournalByDefault() bool { return false }

// Write implements blob.Journal: memory is as durable as it gets.
func (j *journal) Write(ctx context.Context, b []byte) error { return j.Append(ctx, b) }

// Sync implements blob.Journal.
func (j *journal) Sync(ctx context.Context) error {
	j.s.mu.Lock()
	defer j.s.mu.Unlock()
	if j.closed {
		return blob.ErrJournalClosed
	}
	return nil
}

// Rotate implements blob.Journal.
func (j *journal) Rotate(ctx context.Context) error { return errNoSegmentsYet }

// Drop implements blob.Journal.
func (j *journal) Drop(ctx context.Context) error { return errNoSegmentsYet }

var errNoSegmentsYet = errors.New("journal: no segments yet")
