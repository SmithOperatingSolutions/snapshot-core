// Package memstore is an in-memory chunk.Store, for tests. It defines the
// contract: chunk/packstore must behave as it does.
package memstore

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
)

// Store is an in-memory chunk store.
type Store struct {
	mu     sync.Mutex
	chunks map[hash.Hash][]byte
	root   hash.Hash
	closed bool
}

var _ chunk.Store = (*Store)(nil)

// New returns an empty store.
func New() *Store { return &Store{chunks: map[hash.Hash][]byte{}} }

// Tamper flips a byte of a stored chunk, as silent corruption would. It
// reports whether the chunk was there.
func (s *Store) Tamper(h hash.Hash) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.chunks[h]
	if !ok || len(b) == 0 {
		return false
	}
	b[len(b)/2] ^= 1
	return true
}

// Put implements chunk.Store.
func (s *Store) Put(ctx context.Context, data []byte) (hash.Hash, error) {
	if len(data) > chunk.MaxChunkSize {
		return hash.Hash{}, fmt.Errorf("%w: %d bytes", chunk.ErrTooLarge, len(data))
	}
	h := hash.Sum(data)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return hash.Hash{}, chunk.ErrClosed
	}
	if _, ok := s.chunks[h]; !ok {
		s.chunks[h] = bytes.Clone(data)
	}
	return h, nil
}

// Get implements chunk.Store.
func (s *Store) Get(ctx context.Context, h hash.Hash) ([]byte, error) {
	s.mu.Lock()
	b, ok := s.chunks[h]
	closed := s.closed
	s.mu.Unlock()
	switch {
	case closed:
		return nil, chunk.ErrClosed
	case !ok:
		return nil, chunk.ErrNotFound
	}
	out := bytes.Clone(b)
	if hash.Sum(out) != h {
		return nil, fmt.Errorf("%w: %s", chunk.ErrCorrupt, h.Short())
	}
	return out, nil
}

// Has implements chunk.Store.
func (s *Store) Has(ctx context.Context, hs []hash.Hash) (map[hash.Hash]bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, chunk.ErrClosed
	}
	out := make(map[hash.Hash]bool, len(hs))
	for _, h := range hs {
		_, out[h] = s.chunks[h]
	}
	return out, nil
}

// Root implements chunk.Store.
func (s *Store) Root(ctx context.Context) (hash.Hash, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return hash.Hash{}, chunk.ErrClosed
	}
	return s.root, nil
}

// CompareAndSetRoot implements chunk.Store.
func (s *Store) CompareAndSetRoot(ctx context.Context, expected, next hash.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return chunk.ErrClosed
	}
	if _, ok := s.chunks[next]; !ok || next.IsZero() {
		return fmt.Errorf("%w: %s", chunk.ErrRootMissing, next.Short())
	}
	if s.root != expected {
		return chunk.ErrRootConflict
	}
	s.root = next
	return nil
}

// Stats implements chunk.Store.
func (s *Store) Stats(ctx context.Context) (chunk.Stats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return chunk.Stats{}, chunk.ErrClosed
	}
	return chunk.Stats{Chunks: int64(len(s.chunks))}, nil
}

// Close implements chunk.Store.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}
