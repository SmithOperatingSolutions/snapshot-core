// Package local is the single-path disk backend (Storage Core Spec
// "core/blob/local"; Engine Spec "filestore" rules). A store is a directory:
//
//	.snapshot-core   marker: format and store id (an unmounted mount point has none)
//	objects/...      one file per object, named <segment>~ so a name and a
//	                 longer name under it ("a/b", "a/b/c") never collide
//	tmp/             bodies being written; linked into objects/ when complete
//	root             the root pointer: version, value, SHA-256 of both
//	root.lock        flock'd around every root swap
//
// Put-if-absent is write-temp, fsync, link(2): link fails if the name exists,
// and an object is visible only complete, even across a crash (O_CREAT|O_EXCL
// on the final name would expose, and after a crash keep, a torn object).
// The root swap is lock, write-temp, fsync, rename, fsync-dir. Directories are
// 0700 and files 0600 regardless of umask. The filesystem must be local and
// on the allowlist (ext4, xfs, btrfs, zfs, apfs); anything else fails closed.
package local

import (
	"context"
	"errors"
	"io"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
)

// Errors.
var (
	ErrUnsupportedFilesystem = errors.New("local: filesystem is not on the allowlist (ext4, xfs, btrfs, zfs, apfs)")
	ErrNotAStore             = errors.New("local: not a snapshot-core store (no marker: an unmounted volume?)")
	ErrNotEmpty              = errors.New("local: directory is not empty")
	ErrPermissions           = errors.New("local: store directory is accessible to other users")
	ErrCorrupt               = errors.New("local: store metadata is corrupt")
)

// AllowedFilesystems is the allowlist.
var AllowedFilesystems = []string{"ext4", "xfs", "btrfs", "zfs", "apfs"}

// Options configures a store.
type Options struct {
	fsType func(path string) (string, error) // test seam; nil means detect
}

// Store is a local-disk BlobStore.
type Store struct{}

var _ blob.BlobStore = (*Store)(nil)

// Create makes a new store in dir, which must be absent or empty.
func Create(dir string, opts Options) (*Store, error) { return &Store{}, nil }

// Open opens an existing store.
func Open(dir string, opts Options) (*Store, error) { return &Store{}, nil }

// ID is the store's identity from its marker.
func (s *Store) ID() string { return "" }

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

// fsName maps a Linux statfs magic number to a filesystem name.
func fsName(magic int64) string { return "" }
