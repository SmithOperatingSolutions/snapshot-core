// Package split is a BlobStore whose objects live in one store and whose
// root lives in another (docs/DESIGN.md §4, #8): objects on an S3-compatible
// provider that ignores conditional writes (blob/s3 opened objects only),
// the root on a store that compare-and-swaps (blob/local). A copy of the
// root is kept on the objects store, in the mode the host picks, so losing
// the root store's disk loses nothing but the swap in flight, or nothing.
package split

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
)

// Errors.
var (
	ErrOptions     = errors.New("split: invalid options")
	ErrNoMirror    = errors.New("split: the objects store holds no copy of the root")
	ErrRootPresent = errors.New("split: the root store already holds a root")
)

// MirrorMode is how the root's copy on the objects store is kept.
type MirrorMode int

// Mirror modes.
const (
	MirrorWait       MirrorMode = iota // a swap returns once the copy has landed (the default)
	MirrorBackground                   // one writer uploads the newest root after each swap
	MirrorPeriodic                     // the newest root, if it changed, every Mirror.Every
	MirrorOff                          // no copy; the host backs up the root store
)

// Mirror is the root copy's mode and, for MirrorPeriodic, its period.
type Mirror struct {
	Mode  MirrorMode
	Every time.Duration
}

// Mirrorer keeps the root's copy: the unconditional replacement of one
// object outside the object namespace (blob/s3, blob/mem).
type Mirrorer interface {
	WriteMirror(ctx context.Context, value []byte) error
	ReadMirror(ctx context.Context) ([]byte, error)
}

// Options configures a split store.
type Options struct {
	Objects blob.BlobStore // holds the objects, and the root's copy unless Mirror is off
	Roots   blob.BlobStore // holds the root; it must compare-and-swap
	Mirror  Mirror
}

// Status is the root copy's state.
type Status struct {
	Mirrored  bool      // the newest root is on the objects store
	LastWrite time.Time // when a copy last landed
	LastError error     // what last went wrong writing one, until one lands
}

// Store is a BlobStore over two.
type Store struct {
	o Options
}

var _ blob.BlobStore = (*Store)(nil)

var errStub = errors.New("split: not implemented")

// New opens a split store.
func New(o Options) (*Store, error) { return &Store{o: o}, nil }

// Put implements blob.BlobStore.
func (s *Store) Put(ctx context.Context, name string, r io.Reader, size int64) error {
	return errStub
}

// Get implements blob.BlobStore.
func (s *Store) Get(ctx context.Context, name string, off, n int64) (io.ReadCloser, error) {
	return nil, errStub
}

// Stat implements blob.BlobStore.
func (s *Store) Stat(ctx context.Context, name string) (blob.Info, error) {
	return blob.Info{}, errStub
}

// List implements blob.BlobStore.
func (s *Store) List(ctx context.Context, prefix, after string, limit int) ([]blob.Info, error) {
	return nil, errStub
}

// Delete implements blob.BlobStore.
func (s *Store) Delete(ctx context.Context, name string) error { return errStub }

// Root implements blob.BlobStore.
func (s *Store) Root(ctx context.Context) (blob.Root, error) { return blob.Root{}, errStub }

// SwapRoot implements blob.BlobStore.
func (s *Store) SwapRoot(ctx context.Context, expected blob.Version, next []byte) (blob.Version, error) {
	return blob.NoVersion, errStub
}

// MirrorStatus is the root copy's state.
func (s *Store) MirrorStatus() Status { return Status{} }

// Close flushes the root's copy and stops the store's writers.
func (s *Store) Close() error { return nil }

// Recover seeds a root store that has no root from the copy the objects
// store holds.
func Recover(ctx context.Context, objects Mirrorer, roots blob.BlobStore) error { return nil }
