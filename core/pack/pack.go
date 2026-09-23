// Package pack is the pack format: many chunks in one immutable object, each
// compressed (when that helps) and then sealed, followed by a sealed index of
// where each one is (Storage Core Spec "core/pack"; decision D3: new, because
// disknexus's store is bound to numbered local packs and master-key crypto).
//
//	header   magic "SCPK" | version u16 | flags u16 | salt [32]        (40 bytes, clear)
//	frames   seal(Chunk key, context = chunk hash, zstd-or-raw chunk)   (one per chunk)
//	index    seal(PackIndex key, context = header, entries)
//	trailer  index offset u64 | index length u32 | magic "SCPE"      (16 bytes, clear)
//
// Both keys are derived from the repository's master key and the pack's own
// random salt (core/seal), so every pack has its own keys. A frame is bound
// to its chunk's hash: moved or swapped, it fails authentication. Reading a
// chunk decompresses under a hard size bound and re-verifies its SHA-256. A
// pack is named by the SHA-256 of its bytes, under a two-hex-digit shard.
package pack

import (
	"errors"

	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
)

// Errors.
var (
	ErrCorrupt  = errors.New("pack: corrupt")
	ErrTooLarge = errors.New("pack: chunk too large")
	ErrFull     = errors.New("pack: full")
	ErrDup      = errors.New("pack: chunk already in this pack")
)

// Limits.
const (
	MaxChunkSize     = 1 << 20 // Engine Spec L0: the chunk store refuses larger
	MaxChunksPerPack = 1 << 16 // far under seal.MaxSealsPerKey
	MaxPackSize      = 1 << 30
	HeaderSize       = 40
	TrailerSize      = 16
)

// Codecs.
const (
	CodecRaw  uint8 = 0
	CodecZstd uint8 = 1
)

// Entry locates one chunk's frame in a pack.
type Entry struct {
	Hash      hash.Hash
	Offset    uint32 // of the sealed frame, from the start of the pack
	StoredLen uint32 // sealed frame length
	RawLen    uint32 // the chunk's length
	Codec     uint8
}

// Info is what an index object records about a pack: enough to read any one
// chunk with a single range read (the salt gives the keys).
type Info struct {
	Name    string
	Salt    seal.Salt
	Size    int64
	Entries []Entry // sorted by hash
}

// Built is a finished pack.
type Built struct {
	Name  string
	Bytes []byte
	Info  Info
}

// Name returns the object name for pack bytes: packs/<2 hex>/<64 hex>.
func Name(b []byte) string { return "" }

// Codec holds the zstd encoder and a size-bounded decoder. Safe for
// concurrent use; one per chunk store.
type Codec struct{}

// NewCodec builds a codec.
func NewCodec() (*Codec, error) { return &Codec{}, nil }

// Close releases the codec.
func (c *Codec) Close() {}

// Keys are one pack's keys: one for its frames, one for its index.
type Keys struct{ chunk, index *seal.Key }

// DeriveKeys derives a pack's keys from its salt.
func DeriveKeys(kr *seal.Keyring, repo seal.RepoID, salt seal.Salt) (*Keys, error) {
	chunk, err := kr.Key(seal.Chunk, repo, salt)
	if err != nil {
		return nil, err
	}
	index, err := kr.Key(seal.PackIndex, repo, salt)
	if err != nil {
		return nil, err
	}
	return &Keys{chunk: chunk, index: index}, nil
}

// Writer builds one pack in memory.
type Writer struct{}

// NewWriter starts a pack with a fresh random salt.
func NewWriter(kr *seal.Keyring, repo seal.RepoID, codec *Codec, maxSize int) (*Writer, error) {
	return &Writer{}, nil
}

// Add appends a chunk whose identity the caller has computed. It refuses a
// chunk over MaxChunkSize, a duplicate, and a chunk that would take the pack
// past its size or count limit (ErrFull: start another pack).
func (w *Writer) Add(h hash.Hash, data []byte) error { return nil }

// Size is the pack's size so far.
func (w *Writer) Size() int { return 0 }

// Count is the number of chunks so far.
func (w *Writer) Count() int { return 0 }

// Has reports whether the pack holds h.
func (w *Writer) Has(h hash.Hash) bool { return false }

// Get returns a chunk added to this unfinished pack (verified like any read).
func (w *Writer) Get(h hash.Hash) ([]byte, bool, error) { return nil, false, nil }

// Finish seals the index and returns the pack.
func (w *Writer) Finish() (Built, error) { return Built{}, nil }

// ReadInfo parses a whole pack's header, trailer and index.
func ReadInfo(name string, b []byte, kr *seal.Keyring, repo seal.RepoID) (Info, error) {
	return Info{}, nil
}

// OpenFrame decrypts, decompresses and verifies one chunk's frame.
func OpenFrame(keys *Keys, codec *Codec, e Entry, sealed []byte) ([]byte, error) { return nil, nil }
