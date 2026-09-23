// Package packstore is the chunk store over any BlobStore: chunks are packed
// (core/pack), located through sealed index objects (core/dedup), and
// published by swapping a sealed manifest that names the root chunk and the
// live index objects (docs/DESIGN.md §4-5).
package packstore

import (
	"context"
	"errors"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
)

// ErrManifest is returned when the root object does not open: the wrong key
// or repository, or an altered manifest.
var ErrManifest = errors.New("packstore: manifest does not authenticate or decode")

// Defaults.
const (
	DefaultPackSize   = 32 << 20
	DefaultCacheBytes = 64 << 20
	// MaxSwapAttempts is the Engine Spec's bound on manifest retries.
	MaxSwapAttempts = 10
)

// Options configures a store.
type Options struct {
	Blobs      blob.BlobStore
	Keys       *seal.Keyring
	Repo       seal.RepoID
	PackSize   int // 0: DefaultPackSize
	CacheBytes int // 0: DefaultCacheBytes; negative: no cache
	backoff    time.Duration
}

// Store is a chunk.Store over a BlobStore.
type Store struct{}

var _ chunk.Store = (*Store)(nil)

// Open loads the manifest and its index objects.
func Open(ctx context.Context, o Options) (*Store, error) { return &Store{}, nil }

// Location reports where a published or flushed chunk lives, for tools and tests.
func (s *Store) Location(h hash.Hash) (pack string, off, n int64, ok bool) { return "", 0, 0, false }

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
