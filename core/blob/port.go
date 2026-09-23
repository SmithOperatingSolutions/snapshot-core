// Package blob is the backend port: a backend stores immutable objects by name
// and swaps one mutable root pointer atomically. That is the entire contract;
// chunks, packs and commits are built on top (Storage Core Spec, "Blob
// backends"). Implementations: blob/mem, blob/local, blob/multivol, blob/s3.
// Every implementation must pass blob/contract.
//
// Port version 1.
package blob

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

// Errors every backend reports (wrapped with detail where useful).
var (
	ErrNotFound        = errors.New("blob: not found")
	ErrExists          = errors.New("blob: already exists")
	ErrRootConflict    = errors.New("blob: root changed since it was read")
	ErrInvalidName     = errors.New("blob: invalid name")
	ErrInvalidRange    = errors.New("blob: invalid range")
	ErrInvalidLimit    = errors.New("blob: invalid list limit")
	ErrSizeMismatch    = errors.New("blob: object size does not match the declared size")
	ErrTooLarge        = errors.New("blob: too large")
	ErrEmptyRoot       = errors.New("blob: root value is empty")
	ErrReadOnly        = errors.New("blob: store is read-only")
	ErrDeleteForbidden = errors.New("blob: delete is reserved for the GC role")
)

// Limits every backend enforces.
const (
	MaxNameLen    = 512
	MaxSegmentLen = 128
	MaxDepth      = 8
	MaxListPage   = 1000
	MaxObjectSize = 5 << 30 // one S3 PUT; packs are far smaller
	MaxRootSize   = 4 << 20
)

// Info describes a stored object.
type Info struct {
	Name    string
	Size    int64
	ModTime time.Time // when the object was stored; GC uses it for grace windows
}

// Version is an opaque root version token. NoVersion means "no root yet".
// A backend never hands out the same token twice, so a stale token can never
// match again after the root has moved on and back (no ABA).
type Version string

// NoVersion is the version of a store that has no root.
const NoVersion Version = ""

// Root is the root pointer's value and its version.
type Root struct {
	Value   []byte
	Version Version
}

// BlobStore is the backend port.
type BlobStore interface {
	// Put stores exactly size bytes from r under name. It fails with
	// ErrExists if name exists, and with ErrSizeMismatch if r yields more or
	// fewer bytes; either way nothing becomes visible. An object is visible
	// only once complete.
	Put(ctx context.Context, name string, r io.Reader, size int64) error
	// Get reads n bytes at off (n == -1: to the end). A range running past the
	// end is cut at the end; off == size yields nothing; off > size, off < 0
	// or n < -1 is ErrInvalidRange.
	Get(ctx context.Context, name string, off, n int64) (io.ReadCloser, error)
	// Stat describes name.
	Stat(ctx context.Context, name string) (Info, error)
	// List returns up to limit objects whose names start with prefix and sort
	// strictly after after, in ascending byte order. limit must be
	// 1..MaxListPage. A page shorter than limit is the last.
	List(ctx context.Context, prefix, after string, limit int) ([]Info, error)
	// Delete removes name; deleting an absent name is not an error. Only the
	// GC role calls it (see NoDelete).
	Delete(ctx context.Context, name string) error
	// Root returns the root pointer; a store with none returns an empty Value
	// and NoVersion.
	Root(ctx context.Context) (Root, error)
	// SwapRoot replaces the root if its version is still expected (NoVersion:
	// only if there is no root) and returns the new version. Otherwise it
	// fails with ErrRootConflict and changes nothing.
	SwapRoot(ctx context.Context, expected Version, next []byte) (Version, error)
}

// ValidName reports whether name is a legal object name: 1..MaxNameLen bytes,
// '/'-separated segments of 1..MaxSegmentLen bytes from [a-z0-9._-], at most
// MaxDepth segments, no segment starting with '.' (so "." and ".." and every
// backend's own dot-files are unreachable).
func ValidName(name string) error {
	if len(name) == 0 || len(name) > MaxNameLen {
		return fmt.Errorf("%w: length %d outside 1..%d", ErrInvalidName, len(name), MaxNameLen)
	}
	segs := strings.Split(name, "/")
	if len(segs) > MaxDepth {
		return fmt.Errorf("%w: %d segments, more than %d", ErrInvalidName, len(segs), MaxDepth)
	}
	for _, seg := range segs {
		if err := validSegment(seg); err != nil {
			return err
		}
	}
	return nil
}

func validSegment(seg string) error {
	if len(seg) == 0 || len(seg) > MaxSegmentLen {
		return fmt.Errorf("%w: segment length %d outside 1..%d", ErrInvalidName, len(seg), MaxSegmentLen)
	}
	if seg[0] == '.' {
		return fmt.Errorf("%w: segment starts with '.'", ErrInvalidName)
	}
	for i := 0; i < len(seg); i++ {
		c := seg[i]
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '.' && c != '_' && c != '-' {
			return fmt.Errorf("%w: byte %#x", ErrInvalidName, c)
		}
	}
	return nil
}

// ValidPrefix reports whether prefix is a legal List prefix: empty, or a
// valid name, or a valid name followed by '/', or a valid name's leading part.
func ValidPrefix(prefix string) error {
	if prefix == "" {
		return nil
	}
	return ValidName(strings.TrimSuffix(prefix, "/"))
}

// NoDelete wraps a store so Delete fails with ErrDeleteForbidden. The
// repository runs on a NoDelete store; only core/gc holds the raw one.
func NoDelete(s BlobStore) BlobStore { return noDelete{s} }

type noDelete struct{ BlobStore }

func (noDelete) Delete(context.Context, string) error { return ErrDeleteForbidden }
