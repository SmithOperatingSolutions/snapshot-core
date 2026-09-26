// Package chunk is the chunk store port: immutable byte chunks keyed by the
// SHA-256 of their bytes, plus one mutable root (Engine Spec L0, which moved
// into the Storage Core as the layer above the BlobStore; docs/DESIGN.md §2).
// Nothing above this layer touches a backend. Implementations:
// chunk/memstore (tests) and chunk/packstore (packs on any BlobStore). Every
// implementation must pass chunk/contract.
//
// Beside the port, optional interfaces a store may offer (Preparer,
// Flusher, RawWriter): callers find them by type assertion, so a wrapper
// around a store that does not forward them silently takes the slow path
// for everything behind it (#45). A wrapper that narrows a store must
// implement every optional interface the store does, and a new optional
// interface is added to every wrapper the core ships (repo.Chunks()).
//
// Port version 1.
package chunk

import (
	"context"
	"errors"

	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
)

// Errors every implementation reports.
var (
	ErrNotFound     = errors.New("chunk: not found")
	ErrCorrupt      = errors.New("chunk: stored bytes do not verify")
	ErrTooLarge     = errors.New("chunk: larger than the chunk limit")
	ErrRootConflict = errors.New("chunk: root changed since it was read")
	ErrRootMissing  = errors.New("chunk: a root must name a stored chunk")
	ErrClosed       = errors.New("chunk: store is closed")
	// ErrSessionLost: GC deleted writes this store had not published, or a
	// chunk it counted on, because the host outlasted the grace window
	// (docs/DESIGN.md §9). Nothing was published, the store refuses every
	// later write, and the host must reopen the repository and write again.
	ErrSessionLost = errors.New("chunk: GC deleted writes this store had not published")
)

// MaxChunkSize is the Engine Spec's chunk limit.
const MaxChunkSize = 1 << 20

// Reader reads chunks. Every Get re-hashes what it returns: a chunk whose
// bytes do not hash to its name is ErrCorrupt, from every backend, cached or not.
type Reader interface {
	Get(ctx context.Context, h hash.Hash) ([]byte, error)
	Has(ctx context.Context, hs []hash.Hash) (map[hash.Hash]bool, error)
}

// Writer stores chunks. Put is idempotent: the same bytes are one chunk.
type Writer interface {
	Put(ctx context.Context, data []byte) (hash.Hash, error)
}

// ReadWriter is both.
type ReadWriter interface {
	Reader
	Writer
}

// Prepared is a chunk hashed and compressed, ready for a Preparer to store.
type Prepared interface {
	Hash() hash.Hash
	Len() int // the chunk's bytes
}

// Flusher is a Writer that can be told a stream of puts is over (#10): it
// may start storing what it holds unstored, beside the caller's next work
// rather than at the next publish. Flush never waits for the storing.
type Flusher interface {
	Writer
	Flush(ctx context.Context) error
}

// Preparer is a Writer whose per-chunk work (hashing, compression) can be
// done on any goroutine, apart from storing: Prepare on many goroutines
// at once, then PutPrepared in the order the caller needs (#10). Prepare
// and PutPrepared together are Put.
type Preparer interface {
	Writer
	Prepare(data []byte) (Prepared, error)
	PutPrepared(ctx context.Context, p Prepared) (hash.Hash, error)
}

// RawWriter is a Writer that can store a chunk without trying to compress
// it: for chunks its caller knows gain little from compression, such as a
// tree's own nodes (hashes and short keys). The chunk is the same chunk a
// Put of the same bytes stores: same hash, same bytes read back. Optional,
// beside the port; a caller holding a plain Writer calls Put.
type RawWriter interface {
	Writer
	PutRaw(ctx context.Context, data []byte) (hash.Hash, error)
}

// Stats describes what a store holds.
type Stats struct {
	Chunks int64 // distinct chunks stored (including ones not yet published)
}

// Store is the chunk store port.
type Store interface {
	ReadWriter
	// Root returns the current root; the zero Hash if none has been set.
	Root(ctx context.Context) (hash.Hash, error)
	// CompareAndSetRoot sets the root to next if it is still expected (the
	// zero Hash: only if none is set). next must be a stored chunk
	// (ErrRootMissing). On success the new root and every chunk it reaches
	// are durable. A stale expected is ErrRootConflict, and nothing changes.
	CompareAndSetRoot(ctx context.Context, expected, next hash.Hash) error
	Stats(ctx context.Context) (Stats, error)
	// Close releases the store. Afterwards every other method returns
	// ErrClosed, and Close again returns nil.
	Close() error
}
