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
	"sync"

	"github.com/klauspost/compress/zstd"

	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
	"github.com/SmithOperatingSolutions/snapshot-core/core/wire"
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
	FrameOverhead    = sealOverhead // what sealing adds to a frame's payload
)

// Codecs.
const (
	CodecRaw  uint8 = 0
	CodecZstd uint8 = 1
)

// RawBelow is the size under which a chunk is stored raw without trying
// zstd (D13): on a chunk this small the encoder's setup and match-table
// cache misses cost more than the few bytes it could save.
const RawBelow = 256

const (
	headerMagic  = "SCPK"
	trailerMagic = "SCPE"
	indexMagic   = "SCPI"
	version      = 1
	sealOverhead = 28 // nonce + tag
	// minIndexEntry is the shortest encoded index entry: hash, three
	// one-byte uvarints and the codec.
	minIndexEntry = hash.Size + 4
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
	enc     *zstd.Encoder
	dec     *zstd.Decoder
	scratch sync.Pool // []byte the encoder writes into
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
	c := &Codec{enc: enc, dec: dec}
	c.scratch.New = func() any { return make([]byte, 0, MaxChunkSize+MaxChunkSize/8) }
	return c, nil
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

// NewWriter starts a pack with a fresh random salt, its buffer at the
// pack's size: appending frames then never grows and copies the pack,
// which was a third of what a store did per chunk (#10).
func NewWriter(kr *seal.Keyring, repo seal.RepoID, codec *Codec, maxSize int) (*Writer, error) {
	return NewWriterSized(kr, repo, codec, maxSize, maxSize)
}

// NewWriterSized is NewWriter for a pack expected to hold about expect
// bytes of frames: its buffer starts at that, not at the pack's size, and
// grows if the pack goes past it. A repack of a few KiB of live chunks
// costs those KiB, not a pack (#14).
func NewWriterSized(kr *seal.Keyring, repo seal.RepoID, codec *Codec, maxSize, expect int) (*Writer, error) {
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
	size := min(max(expect+HeaderSize+TrailerSize, HeaderSize+TrailerSize), maxSize)
	buf := make([]byte, 0, size)
	return &Writer{codec: codec, keys: keys, salt: salt, maxSize: maxSize, buf: append(buf, w.Bytes()...),
		entries: map[hash.Hash]Entry{}}, nil
}

// indexBound is the most the sealed index for n entries can take.
// SealedIndexLen is the length of the sealed index a pack of these entries
// carries, exactly as the writer encodes it: a record listing a pack's
// entries places the index, and so where its frames must end.
func SealedIndexLen(entries []Entry) uint64 {
	n := uint64(len(indexMagic)+2+uvarintLen(uint64(len(entries)))) + sealOverhead
	for _, e := range entries {
		n += uint64(hash.Size + uvarintLen(uint64(e.Offset)) + uvarintLen(uint64(e.StoredLen)) + uvarintLen(uint64(e.RawLen)) + 1)
	}
	return n
}

func uvarintLen(v uint64) int {
	n := 1
	for ; v >= 0x80; v >>= 7 {
		n++
	}
	return n
}

func indexBound(n int) int { return len(indexMagic) + 2 + 5 + n*maxEntryLen + sealOverhead }

// Add appends a chunk whose identity the caller has computed. It refuses a
// chunk over MaxChunkSize, a duplicate, and a chunk that would take the pack
// past its size or count limit (ErrFull: start another pack). A pack's first
// chunk is always accepted, so one oversized chunk still gets a pack.
func (w *Writer) Add(h hash.Hash, data []byte) error {
	if w.finished || len(data) > MaxChunkSize || len(w.entries) >= MaxChunksPerPack {
		return w.AddCompressed(h, len(data), nil, CodecRaw) // the refusal, without compressing first
	}
	payload, codec := w.codec.Compress(data)
	return w.AddCompressed(h, len(data), payload, codec)
}

// Compress is the payload a chunk is stored as: zstd when that is shorter,
// else the bytes themselves; a chunk under RawBelow is not tried (D13). Safe to call from many goroutines at once.
// The encoder writes into a scratch buffer kept from call to call, so a
// chunk that does not compress costs no allocation, and one that does
// costs its compressed size.
func (c *Codec) Compress(data []byte) (payload []byte, codec uint8) {
	if len(data) < RawBelow {
		return data, CodecRaw
	}
	scratch := c.scratch.Get().([]byte)
	z := c.enc.EncodeAll(data, scratch[:0])
	if len(z) < len(data) {
		payload = make([]byte, len(z))
		copy(payload, z)
		codec = CodecZstd
	} else {
		payload, codec = data, CodecRaw
	}
	c.scratch.Put(z[:0]) //nolint:staticcheck // SA6002: the slice is what the pool holds
	return payload, codec
}

// AddCompressed is Add for a chunk already compressed by this writer's
// codec (Compress): rawLen is the chunk's length, payload and codec what
// Compress returned. The seal, which needs this pack's keys, happens here.
func (w *Writer) AddCompressed(h hash.Hash, rawLen int, payload []byte, codec uint8) error {
	if w.finished || rawLen > MaxChunkSize || len(w.entries) >= MaxChunksPerPack {
		return w.AddSealed(h, rawLen, nil, codec) // the refusal, without sealing first
	}
	sealed, err := w.Seal(h, payload) // a duplicate is refused by AddSealed, after a seal it did not need

	if err != nil {
		return err
	}
	return w.AddSealed(h, rawLen, sealed, codec)
}

// Salt identifies this pack's keys: a frame sealed by one writer's Seal
// belongs in no other pack.
func (w *Writer) Salt() seal.Salt { return w.salt }

// Seal seals a compressed payload for this pack, on any goroutine (#10):
// the chunk's hash is the frame's associated data.
func (w *Writer) Seal(h hash.Hash, payload []byte) ([]byte, error) {
	return w.keys.chunk.Seal(h[:], payload)
}

// AddSealed appends a frame this writer's Seal produced: rawLen is the
// chunk's length, codec what Compress returned. It refuses what Add does.
func (w *Writer) AddSealed(h hash.Hash, rawLen int, sealed []byte, codec uint8) error {
	switch {
	case w.finished:
		return errors.New("pack: writer is finished")
	case rawLen > MaxChunkSize:
		return fmt.Errorf("%w: %d bytes, limit %d", ErrTooLarge, rawLen, MaxChunkSize)
	case len(w.entries) >= MaxChunksPerPack:
		return ErrFull
	}
	if _, ok := w.entries[h]; ok {
		return ErrDup
	}
	if len(w.entries) > 0 && len(w.buf)+len(sealed)+indexBound(len(w.entries)+1)+TrailerSize > w.maxSize {
		return ErrFull
	}
	if need := len(w.buf) + len(sealed); need > cap(w.buf) {
		w.grow(need)
	}
	w.entries[h] = Entry{Hash: h, Offset: uint32(len(w.buf)), StoredLen: uint32(len(sealed)),
		RawLen: uint32(rawLen), Codec: codec}
	w.buf = append(w.buf, sealed...)
	return nil
}

// grow moves the frames to a buffer of at least need bytes: twice the
// current one, with room for the index and trailer, up to the pack's size.
// Doubling copies each byte about once on the way to a full pack, where
// append's growth past a few hundred KiB copied a large pack four times
// over.
func (w *Writer) grow(need int) {
	size := max(2*cap(w.buf), need+indexBound(len(w.entries)+1)+TrailerSize)
	size = max(min(size, w.maxSize), need) // a first chunk over the size limit still gets its pack
	buf := make([]byte, len(w.buf), size)
	copy(buf, w.buf)
	w.buf = buf
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
	// The pack is built in the writer's own buffer, allocated at the pack's
	// size, so finishing copies nothing; the writer is done with it.
	indexOffset := len(w.buf)
	b := w.buf
	b = append(b, sealed...)
	b = binary.LittleEndian.AppendUint64(b, uint64(indexOffset))
	b = binary.LittleEndian.AppendUint32(b, uint32(len(sealed)))
	b = append(b, trailerMagic...) // w.buf's array, appended beyond its length: a Get on the frames meanwhile reads bytes this never touches
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
	// n is a claim until the entries decode: room for no more than the
	// bytes left could hold (#24).
	entries := make([]Entry, 0, min(n, uint64(len(b))/minIndexEntry))
	for i := uint64(0); i < n; i++ {
		var e Entry
		copy(e.Hash[:], r.Fixed(hash.Size))
		off, stored, raw := r.Uvarint(), r.Uvarint(), r.Uvarint()
		e.Codec = r.U8()
		if r.Err() != nil {
			return nil, fmt.Errorf("%w: index entry %d: %w", ErrCorrupt, i, r.Err())
		}
		switch {
		case stored < sealOverhead || off > indexOffset: // an offset past 32 bits would truncate into place
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
	// Walking frames in offset order from the end of the header refuses
	// overlapping frames, a frame reaching back into the header, and (the
	// last) one reaching into the index.
	byOffset := append([]Entry(nil), entries...)
	sort.Slice(byOffset, func(i, j int) bool { return byOffset[i].Offset < byOffset[j].Offset })
	end := uint64(HeaderSize)
	for _, e := range byOffset {
		if uint64(e.Offset) < end {
			return nil, fmt.Errorf("%w: frames overlap each other or the header", ErrCorrupt)
		}
		end = uint64(e.Offset) + uint64(e.StoredLen)
	}
	if end > indexOffset {
		return nil, fmt.Errorf("%w: the last frame ends at %d, past the index at %d", ErrCorrupt, end, indexOffset)
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
