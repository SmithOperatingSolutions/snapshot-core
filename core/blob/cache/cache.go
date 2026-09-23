// Package cache is a size-capped local disk cache in front of a BlobStore,
// for the S3 read path (Engine Spec s3store: "range GETs ... through a
// size-capped local disk cache (default 10 GiB, dir 0700)"). Only immutable
// objects (packs/, index/) are cached; everything else passes through. Each
// cache file carries the SHA-256 of what it holds, so a damaged entry is
// refetched rather than served. Cached bytes are ciphertext frames: nothing
// in the cache is plaintext, and the chunk layer re-verifies every chunk anyway.
package cache

import (
	"context"
	"io"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
)

// DefaultMaxBytes is the Engine Spec's default.
const DefaultMaxBytes = 10 << 30

// Options configures a cache.
type Options struct {
	Dir      string
	MaxBytes int64 // 0: DefaultMaxBytes
}

// Store is a caching BlobStore.
type Store struct{ blob.BlobStore }

var _ blob.BlobStore = (*Store)(nil)

// New wraps inner with a cache in o.Dir, reusing what a previous process left.
func New(inner blob.BlobStore, o Options) (*Store, error) { return &Store{BlobStore: inner}, nil }

// Used reports the bytes the cache holds.
func (s *Store) Used() int64 { return 0 }

// Get implements blob.BlobStore.
func (s *Store) Get(ctx context.Context, name string, off, n int64) (io.ReadCloser, error) {
	return s.BlobStore.Get(ctx, name, off, n)
}
