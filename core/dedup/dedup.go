// Package dedup answers "do we already have this chunk, and where?" (Storage
// Core Spec "core/dedup"; decision D4: new, because disknexus's index writes
// plaintext working copies to disk). It has two parts:
//
//   - Index objects: sealed records of which chunks live where, one per
//     batch of packs, named "index/<sha256 of the object>". They carry each
//     pack's salt, so any chunk can be read with one range read, and they
//     are sealed under the Index domain: no chunk hash is ever on disk in
//     the clear.
//   - Index: the in-memory map from chunk hash to location, built by
//     loading the index objects a repository's root lists.
package dedup

import (
	"errors"

	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/pack"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
)

// ErrCorrupt is returned for an index object that does not decode.
var ErrCorrupt = errors.New("dedup: corrupt index object")

// Limits on one index object.
const (
	MaxPacksPerObject = 1 << 16
	MaxObjectSize     = 64 << 20
)

// PackName is the object name of the pack whose bytes hash to sum.
func PackName(sum [32]byte) string { return "" }

// EncodeObject seals a batch of pack records into an index object.
func EncodeObject(kr *seal.Keyring, repo seal.RepoID, packs []pack.Info) (name string, blob []byte, err error) {
	return "", nil, nil
}

// DecodeObject opens an index object. The blob must hash to its name.
func DecodeObject(kr *seal.Keyring, repo seal.RepoID, name string, blob []byte) ([]pack.Info, error) {
	return nil, nil
}

// Location is where one chunk lives.
type Location struct {
	Pack  PackRef
	Entry pack.Entry
}

// PackRef is what a reader needs to fetch from a pack.
type PackRef struct {
	Name string
	Salt seal.Salt
	Size int64
}

// Index maps chunk hashes to locations. Not safe for concurrent use; the
// chunk store serializes access.
type Index struct{}

// New returns an empty index.
func New() *Index { return &Index{} }

// Add records every chunk of a pack. A chunk already located elsewhere keeps
// its first location.
func (x *Index) Add(info pack.Info) {}

// Lookup locates a chunk.
func (x *Index) Lookup(h hash.Hash) (Location, bool) { return Location{}, false }

// Has reports whether a chunk is located.
func (x *Index) Has(h hash.Hash) bool { return false }

// Len is the number of distinct chunks.
func (x *Index) Len() int { return 0 }

// Packs lists the packs added, in order.
func (x *Index) Packs() []PackRef { return nil }
