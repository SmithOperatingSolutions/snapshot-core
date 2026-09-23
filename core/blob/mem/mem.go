// Package mem is an in-memory blob.BlobStore, for tests. It defines the
// contract: every other backend must behave as it does.
package mem

import (
	"context"
	"io"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
)

// Store is an in-memory BlobStore.
type Store struct{}

// New returns an empty store.
func New() *Store { return &Store{} }

var _ blob.BlobStore = (*Store)(nil)

// Put implements blob.BlobStore.
func (s *Store) Put(ctx context.Context, name string, r io.Reader, size int64) error { return nil }

// Get implements blob.BlobStore.
func (s *Store) Get(ctx context.Context, name string, off, n int64) (io.ReadCloser, error) {
	return nil, blob.ErrNotFound
}

// Stat implements blob.BlobStore.
func (s *Store) Stat(ctx context.Context, name string) (blob.Info, error) { return blob.Info{}, nil }

// List implements blob.BlobStore.
func (s *Store) List(ctx context.Context, prefix, after string, limit int) ([]blob.Info, error) {
	return nil, nil
}

// Delete implements blob.BlobStore.
func (s *Store) Delete(ctx context.Context, name string) error { return nil }

// Root implements blob.BlobStore.
func (s *Store) Root(ctx context.Context) (blob.Root, error) { return blob.Root{}, nil }

// SwapRoot implements blob.BlobStore.
func (s *Store) SwapRoot(ctx context.Context, expected blob.Version, next []byte) (blob.Version, error) {
	return blob.NoVersion, nil
}
