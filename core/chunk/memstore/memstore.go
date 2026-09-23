// Package memstore is an in-memory chunk.Store, for tests. It defines the
// contract: chunk/packstore must behave as it does.
package memstore

import (
	"context"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
)

// Store is an in-memory chunk store.
type Store struct{}

var _ chunk.Store = (*Store)(nil)

// New returns an empty store.
func New() *Store { return &Store{} }

// Tamper flips a byte of a stored chunk, as silent corruption would. It
// reports whether the chunk was there.
func (s *Store) Tamper(h hash.Hash) bool { return false }

// Put implements chunk.Store.
func (s *Store) Put(ctx context.Context, data []byte) (hash.Hash, error) { return hash.Hash{}, nil }

// Get implements chunk.Store.
func (s *Store) Get(ctx context.Context, h hash.Hash) ([]byte, error) { return nil, chunk.ErrNotFound }

// Has implements chunk.Store.
func (s *Store) Has(ctx context.Context, hs []hash.Hash) (map[hash.Hash]bool, error) { return nil, nil }

// Root implements chunk.Store.
func (s *Store) Root(ctx context.Context) (hash.Hash, error) { return hash.Hash{}, nil }

// CompareAndSetRoot implements chunk.Store.
func (s *Store) CompareAndSetRoot(ctx context.Context, expected, next hash.Hash) error { return nil }

// Stats implements chunk.Store.
func (s *Store) Stats(ctx context.Context) (chunk.Stats, error) { return chunk.Stats{}, nil }

// Close implements chunk.Store.
func (s *Store) Close() error { return nil }
