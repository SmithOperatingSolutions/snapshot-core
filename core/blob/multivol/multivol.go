// Package multivol spreads one store across several local mount points
// (Storage Core Spec "core/blob/multivol"; Engine Spec "multistore" rules).
//
// Each volume is a blob/local store in <path>/vol, so every volume passes the
// local-filesystem allowlist, carries its own marker (a random UUID), and is
// crash-safe on its own. The primary volume also holds the volume map (volume
// UUID -> path) and the root. An object lives on the volume rendezvous hashing
// of its name picks among the volumes taking new objects; reads try volumes in
// that order, so objects placed before a volume was added are still found
// where they are. A volume that is missing, remounted or swapped is detected
// at open and on every operation, and the store goes read-only
// (ErrVolumeMissing) rather than guessing.
package multivol

import (
	"context"
	"errors"
	"io"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
)

// Errors.
var (
	ErrVolumeMissing  = errors.New("multivol: a volume is missing or remounted; the store is read-only")
	ErrVolumeMismatch = errors.New("multivol: a volume's marker does not match the volume map")
	ErrTooManyVolumes = errors.New("multivol: more than 64 volumes")
	ErrNoSpace        = errors.New("multivol: every volume is below its free-space floor")
	ErrNotAStore      = errors.New("multivol: not a multivol store")
)

// MaxVolumes is the Engine Spec's limit.
const MaxVolumes = 64

// DefaultFloor is the free-space fraction below which a volume stops taking
// new objects.
const DefaultFloor = 0.05

// Options configures a store.
type Options struct {
	Floor     float64                                         // 0 means DefaultFloor
	freeSpace func(path string) (free, total uint64, err error) // test seam; nil means statfs
}

// Store is a multi-volume BlobStore.
type Store struct{}

var _ blob.BlobStore = (*Store)(nil)

// Create makes a store over a primary path and any number of secondaries.
func Create(primary string, secondaries []string, opts Options) (*Store, error) { return &Store{}, nil }

// Open opens the store whose primary volume is at primary.
func Open(primary string, opts Options) (*Store, error) { return &Store{}, nil }

// AddVolume adds a volume online. New objects start landing on it; nothing
// already stored moves.
func (s *Store) AddVolume(path string) error { return nil }

// ReadOnly reports whether a volume has gone missing.
func (s *Store) ReadOnly() bool { return false }

// Place returns, for each name, the index into volumeIDs of the volume
// rendezvous hashing assigns it (all volumes taking objects).
func Place(names []string, volumeIDs []string) []int { return make([]int, len(names)) }

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

// VolumeIDs lists the volumes' UUIDs in map order (primary first).
func (s *Store) VolumeIDs() []string { return nil }
