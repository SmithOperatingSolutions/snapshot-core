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
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"

	"github.com/klauspost/compress/zstd"

	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/internal/wire"
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

const (
	headerMagic  = "SCPK"
	trailerMagic = "SCPE"
	indexMagic   = "SCPI"
	version      = 1
	sealOverhead = 28 // nonce + tag
	// maxEntryLen bounds one encoded index entry: hash, three uvarints of at
	// most 5 bytes each (values < 2^32), the codec byte.
	maxEntryLen = hash.Size + 3*5 + 1
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
func Name(b []byte) string {
	sum := sha256.Sum256(b)
	h := hex.EncodeToString(sum[:])
	return "packs/" + h[:2] + "/" + h
}

// Codec holds the zstd encoder and a size-bounded decoder. Safe for
// concurrent use; one per chunk store.
type Codec struct {
	enc *zstd.Encoder
	dec *zstd.Decoder
}

// NewCodec builds a codec.
func NewCodec() (*Codec, error) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, err
	}
	// The decoder refuses to produce more than one chunk's worth: a frame
	// that expands further is a decompression bomb, not a chunk.
	dec, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(MaxChunkSize), zstd.WithDecoderConcurrency(0))
	if err != nil {
		_ = enc.Close()
		return nil, err
	}
	return &Codec{enc: enc, dec: dec}, nil
}

// Close releases the codec.
func (c *Codec) Close() {
	_ = c.enc.Close()
	c.dec.Close()
}

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
type Writer struct {
	codec    *Codec
	keys     *Keys
	salt     seal.Salt
	maxSize  int
	buf      []byte
	entries  map[hash.Hash]Entry
	finished bool
}

// NewWriter starts a pack with a fresh random salt.
func NewWriter(kr *seal.Keyring, repo seal.RepoID, codec *Codec, maxSize int) (*Writer, error) {
	if maxSize < HeaderSize+TrailerSize || maxSize > MaxPackSize {
		return nil, fmt.Errorf("pack: size limit %d outside %d..%d", maxSize, HeaderSize+TrailerSize, MaxPackSize)
	}
	salt, err := seal.NewSalt()
	if err != nil {
		return nil, err
	}
	keys, err := DeriveKeys(kr, repo, salt)
	if err != nil {
		return nil, err
	}
	var w wire.Writer
	w.Raw([]byte(headerMagic))
	w.U16(version)
	w.U16(0) // flags
	w.Raw(salt[:])
	return &Writer{codec: codec, keys: keys, salt: salt, maxSize: maxSize, buf: w.Bytes(),
		entries: map[hash.Hash]Entry{}}, nil
}

// indexBound is the most the sealed index for n entries can take.
func indexBound(n int) int { return len(indexMagic) + 2 + 5 + n*maxEntryLen + sealOverhead }

// Add appends a chunk whose identity the caller has computed. It refuses a
// chunk over MaxChunkSize, a duplicate, and a chunk that would take the pack
// past its size or count limit (ErrFull: start another pack). A pack's first
// chunk is always accepted, so one oversized chunk still gets a pack.
func (w *Writer) Add(h hash.Hash, data []byte) error {
	switch {
	case w.finished:
		return errors.New("pack: writer is finished")
	case len(data) > MaxChunkSize:
		return fmt.Errorf("%w: %d bytes, limit %d", ErrTooLarge, len(data), MaxChunkSize)
	case len(w.entries) >= MaxChunksPerPack:
		return ErrFull
	}
	if _, ok := w.entries[h]; ok {
		return ErrDup
	}
	payload, codec := data, CodecRaw
	if len(data) > 0 {
		if z := w.codec.enc.EncodeAll(data, nil); len(z) < len(data) {
			payload, codec = z, CodecZstd
		}
	}
	sealed, err := w.keys.chunk.Seal(h[:], payload)
	if err != nil {
		return err
	}
	if len(w.entries) > 0 && len(w.buf)+len(sealed)+indexBound(len(w.entries)+1)+TrailerSize > w.maxSize {
		return ErrFull
	}
	w.entries[h] = Entry{Hash: h, Offset: uint32(len(w.buf)), StoredLen: uint32(len(sealed)),
		RawLen: uint32(len(data)), Codec: codec}
	w.buf = append(w.buf, sealed...)
	return nil
}

// Size is the most the pack can take if finished now.
func (w *Writer) Size() int { return len(w.buf) + indexBound(len(w.entries)) + TrailerSize }

// Count is the number of chunks so far.
func (w *Writer) Count() int { return len(w.entries) }

// Has reports whether the pack holds h.
func (w *Writer) Has(h hash.Hash) bool {
	_, ok := w.entries[h]
	return ok
}

// Get returns a chunk added to this unfinished pack (verified like any read).
func (w *Writer) Get(h hash.Hash) ([]byte, bool, error) {
	e, ok := w.entries[h]
	if !ok {
		return nil, false, nil
	}
	data, err := OpenFrame(w.keys, w.codec, e, w.buf[e.Offset:e.Offset+e.StoredLen])
	return data, true, err
}

// Finish seals the index and returns the pack.
func (w *Writer) Finish() (Built, error) {
	if w.finished {
		return Built{}, errors.New("pack: writer is finished")
	}
	w.finished = true
	entries := make([]Entry, 0, len(w.entries))
	for _, e := range w.entries {
		entries = append(entries, e)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Hash.Compare(entries[j].Hash) < 0 })
	sealed, err := w.keys.index.Seal(w.buf[:HeaderSize], encodeIndex(entries))
	if err != nil {
		return Built{}, err
	}
	indexOffset := len(w.buf)
	b := make([]byte, 0, len(w.buf)+len(sealed)+TrailerSize)
	b = append(append(b, w.buf...), sealed...)
	b = binary.LittleEndian.AppendUint64(b, uint64(indexOffset))
	b = binary.LittleEndian.AppendUint32(b, uint32(len(sealed)))
	b = append(b, trailerMagic...)
	name := Name(b)
	return Built{Name: name, Bytes: b, Info: Info{Name: name, Salt: w.salt, Size: int64(len(b)), Entries: entries}}, nil
}

// Index plaintext: magic "SCPI" | version u16 | count uvarint | count x
// (hash [32] | offset uvarint | stored uvarint | raw uvarint | codec u8),
// strictly sorted by hash.
func encodeIndex(entries []Entry) []byte {
	var w wire.Writer
	w.Raw([]byte(indexMagic))
	w.U16(version)
	w.Uvarint(uint64(len(entries)))
	for _, e := range entries {
		w.Raw(e.Hash[:])
		w.Uvarint(uint64(e.Offset))
		w.Uvarint(uint64(e.StoredLen))
		w.Uvarint(uint64(e.RawLen))
		w.U8(e.Codec)
	}
	return w.Bytes()
}

// decodeIndex parses and validates an index against the pack it came from:
// every frame lies between the header and the index, no two overlap, every
// length is consistent with its codec, and hashes are strictly increasing.
func decodeIndex(b []byte, indexOffset uint64) ([]Entry, error) {
	r := wire.NewReader(b)
	magic := r.Fixed(4)
	v := r.U16()
	n := r.Uvarint()
	if r.Err() != nil || string(magic) != indexMagic || v != version || n > MaxChunksPerPack {
		return nil, fmt.Errorf("%w: index header", ErrCorrupt)
	}
	entries := make([]Entry, 0, n)
	for i := uint64(0); i < n; i++ {
		var e Entry
		copy(e.Hash[:], r.Fixed(hash.Size))
		off, stored, raw := r.Uvarint(), r.Uvarint(), r.Uvarint()
		e.Codec = r.U8()
		if r.Err() != nil {
			return nil, fmt.Errorf("%w: index entry %d: %w", ErrCorrupt, i, r.Err())
		}
		switch {
		case stored < sealOverhead || off+stored > indexOffset:
			return nil, fmt.Errorf("%w: entry %d frame [%d,+%d) outside the frames region", ErrCorrupt, i, off, stored)
		case raw > MaxChunkSize:
			return nil, fmt.Errorf("%w: entry %d claims %d bytes", ErrCorrupt, i, raw)
		case e.Codec == CodecRaw && stored != raw+sealOverhead:
			return nil, fmt.Errorf("%w: entry %d raw length mismatch", ErrCorrupt, i)
		case e.Codec == CodecZstd && stored-sealOverhead >= raw:
			return nil, fmt.Errorf("%w: entry %d is not smaller compressed", ErrCorrupt, i)
		case e.Codec != CodecRaw && e.Codec != CodecZstd:
			return nil, fmt.Errorf("%w: entry %d codec %d", ErrCorrupt, i, e.Codec)
		case i > 0 && entries[i-1].Hash.Compare(e.Hash) >= 0:
			return nil, fmt.Errorf("%w: entries not strictly sorted", ErrCorrupt)
		}
		e.Offset, e.StoredLen, e.RawLen = uint32(off), uint32(stored), uint32(raw)
		entries = append(entries, e)
	}
	if err := r.Done(); err != nil {
		return nil, fmt.Errorf("%w: index: %w", ErrCorrupt, err)
	}
	// Walking frames in offset order from the end of the header refuses both
	// overlapping frames and a frame reaching back into the header.
	byOffset := append([]Entry(nil), entries...)
	sort.Slice(byOffset, func(i, j int) bool { return byOffset[i].Offset < byOffset[j].Offset })
	end := uint64(HeaderSize)
	for _, e := range byOffset {
		if uint64(e.Offset) < end {
			return nil, fmt.Errorf("%w: frames overlap each other or the header", ErrCorrupt)
		}
		end = uint64(e.Offset) + uint64(e.StoredLen)
	}
	return entries, nil
}

// ReadInfo parses a whole pack's header, trailer and index.
func ReadInfo(name string, b []byte, kr *seal.Keyring, repo seal.RepoID) (Info, error) {
	if len(b) < HeaderSize+TrailerSize {
		return Info{}, fmt.Errorf("%w: %d bytes", ErrCorrupt, len(b))
	}
	if Name(b) != name {
		return Info{}, fmt.Errorf("%w: bytes do not hash to the pack's name", ErrCorrupt)
	}
	h := wire.NewReader(b[:HeaderSize])
	magic, v, flags := h.Fixed(4), h.U16(), h.U16()
	var salt seal.Salt
	copy(salt[:], h.Fixed(len(salt)))
	if h.Done() != nil || string(magic) != headerMagic || v != version || flags != 0 {
		return Info{}, fmt.Errorf("%w: header", ErrCorrupt)
	}
	t := wire.NewReader(b[len(b)-TrailerSize:])
	indexOffset, indexLen, tmagic := t.U64(), uint64(t.U32()), t.Fixed(4)
	if t.Done() != nil || string(tmagic) != trailerMagic || indexOffset < HeaderSize ||
		indexOffset+indexLen+TrailerSize != uint64(len(b)) {
		return Info{}, fmt.Errorf("%w: trailer", ErrCorrupt)
	}
	keys, err := DeriveKeys(kr, repo, salt)
	if err != nil {
		return Info{}, err
	}
	plain, err := keys.index.Open(b[:HeaderSize], b[indexOffset:indexOffset+indexLen])
	if err != nil {
		return Info{}, fmt.Errorf("%w: index does not authenticate (wrong key or repository, or altered)", ErrCorrupt)
	}
	entries, err := decodeIndex(plain, indexOffset)
	if err != nil {
		return Info{}, err
	}
	return Info{Name: name, Salt: salt, Size: int64(len(b)), Entries: entries}, nil
}

// OpenFrame decrypts, decompresses and verifies one chunk's frame.
func OpenFrame(keys *Keys, codec *Codec, e Entry, sealed []byte) ([]byte, error) {
	if len(sealed) != int(e.StoredLen) {
		return nil, fmt.Errorf("%w: frame is %d bytes, index says %d", ErrCorrupt, len(sealed), e.StoredLen)
	}
	payload, err := keys.chunk.Open(e.Hash[:], sealed)
	if err != nil {
		return nil, fmt.Errorf("%w: frame for %s does not authenticate", ErrCorrupt, e.Hash.Short())
	}
	var data []byte
	switch e.Codec {
	case CodecRaw:
		data = payload
	case CodecZstd:
		if e.RawLen > MaxChunkSize {
			return nil, fmt.Errorf("%w: frame claims %d bytes", ErrCorrupt, e.RawLen)
		}
		data, err = codec.dec.DecodeAll(payload, make([]byte, 0, e.RawLen))
		if err != nil {
			return nil, fmt.Errorf("%w: frame for %s does not decompress within the chunk limit", ErrCorrupt, e.Hash.Short())
		}
	default:
		return nil, fmt.Errorf("%w: codec %d", ErrCorrupt, e.Codec)
	}
	if len(data) != int(e.RawLen) {
		return nil, fmt.Errorf("%w: frame for %s is %d bytes, index says %d", ErrCorrupt, e.Hash.Short(), len(data), e.RawLen)
	}
	if hash.Sum(data) != e.Hash {
		return nil, fmt.Errorf("%w: chunk bytes do not hash to %s", ErrCorrupt, e.Hash.Short())
	}
	return data, nil
}
