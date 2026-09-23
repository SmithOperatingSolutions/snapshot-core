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
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"

	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/internal/wire"
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

const (
	objectMagic = "SCIX"
	plainMagic  = "SCIP"
	version     = 1
	headerSize  = 4 + 2 + 32 // magic, version, salt
)

// PackName is the object name of the pack whose bytes hash to sum.
func PackName(sum [32]byte) string {
	h := hex.EncodeToString(sum[:])
	return "packs/" + h[:2] + "/" + h
}

// packSum recovers a pack name's hash, refusing anything PackName would not write.
func packSum(name string) ([32]byte, error) {
	var sum [32]byte
	if len(name) != len("packs/xx/")+64 {
		return sum, fmt.Errorf("%w: pack name %q", ErrCorrupt, name)
	}
	b, err := hex.DecodeString(name[len("packs/xx/"):])
	if err != nil || len(b) != 32 {
		return sum, fmt.Errorf("%w: pack name %q", ErrCorrupt, name)
	}
	copy(sum[:], b)
	if PackName(sum) != name {
		return sum, fmt.Errorf("%w: pack name %q is not canonical", ErrCorrupt, name)
	}
	return sum, nil
}

// Object plaintext: magic "SCIP" | version u16 | packs uvarint | packs x
// (pack hash [32] | salt [32] | size uvarint | entries uvarint | entries x
// (hash [32] | offset uvarint | stored uvarint | raw uvarint | codec u8)).
// Packs are strictly increasing by hash; entries likewise within a pack.
func encodePlain(packs []pack.Info) ([]byte, error) {
	type record struct {
		sum  [32]byte
		info pack.Info
	}
	recs := make([]record, len(packs))
	for i, p := range packs {
		sum, err := packSum(p.Name)
		if err != nil {
			return nil, err
		}
		recs[i] = record{sum: sum, info: p}
	}
	sort.Slice(recs, func(i, j int) bool { return bytes.Compare(recs[i].sum[:], recs[j].sum[:]) < 0 })
	var w wire.Writer
	w.Raw([]byte(plainMagic))
	w.U16(version)
	w.Uvarint(uint64(len(recs)))
	for _, rec := range recs {
		p := rec.info
		w.Raw(rec.sum[:])
		w.Raw(p.Salt[:])
		w.Uvarint(uint64(p.Size))
		w.Uvarint(uint64(len(p.Entries)))
		for _, e := range p.Entries {
			w.Raw(e.Hash[:])
			w.Uvarint(uint64(e.Offset))
			w.Uvarint(uint64(e.StoredLen))
			w.Uvarint(uint64(e.RawLen))
			w.U8(e.Codec)
		}
	}
	return w.Bytes(), nil
}

func decodePlain(b []byte) ([]pack.Info, error) {
	r := wire.NewReader(b)
	magic, v, n := r.Fixed(4), r.U16(), r.Uvarint()
	if r.Err() != nil || string(magic) != plainMagic || v != version || n > MaxPacksPerObject {
		return nil, fmt.Errorf("%w: header", ErrCorrupt)
	}
	packs := make([]pack.Info, 0, n)
	var prev [32]byte
	for i := uint64(0); i < n; i++ {
		var sum [32]byte
		copy(sum[:], r.Fixed(32))
		var p pack.Info
		copy(p.Salt[:], r.Fixed(32))
		size, count := r.Uvarint(), r.Uvarint()
		if r.Err() != nil || size < pack.HeaderSize+pack.TrailerSize || size > pack.MaxPackSize ||
			count > pack.MaxChunksPerPack || (i > 0 && bytes.Compare(prev[:], sum[:]) >= 0) {
			return nil, fmt.Errorf("%w: pack record %d", ErrCorrupt, i)
		}
		prev = sum
		p.Name, p.Size = PackName(sum), int64(size)
		framesEnd := size - pack.TrailerSize
		p.Entries = make([]pack.Entry, 0, count)
		for j := uint64(0); j < count; j++ {
			var e pack.Entry
			copy(e.Hash[:], r.Fixed(hash.Size))
			off, stored, raw := r.Uvarint(), r.Uvarint(), r.Uvarint()
			e.Codec = r.U8()
			if r.Err() != nil || off < pack.HeaderSize || off+stored > framesEnd || raw > pack.MaxChunkSize ||
				(e.Codec != pack.CodecRaw && e.Codec != pack.CodecZstd) ||
				(j > 0 && p.Entries[j-1].Hash.Compare(e.Hash) >= 0) {
				return nil, fmt.Errorf("%w: pack %d entry %d", ErrCorrupt, i, j)
			}
			e.Offset, e.StoredLen, e.RawLen = uint32(off), uint32(stored), uint32(raw)
			p.Entries = append(p.Entries, e)
		}
		packs = append(packs, p)
	}
	if err := r.Done(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrCorrupt, err)
	}
	return packs, nil
}

// EncodeObject seals a batch of pack records into an index object.
func EncodeObject(kr *seal.Keyring, repo seal.RepoID, packs []pack.Info) (name string, blob []byte, err error) {
	if len(packs) > MaxPacksPerObject {
		return "", nil, fmt.Errorf("dedup: %d packs in one object, limit %d", len(packs), MaxPacksPerObject)
	}
	plain, err := encodePlain(packs)
	if err != nil {
		return "", nil, err
	}
	salt, err := seal.NewSalt()
	if err != nil {
		return "", nil, err
	}
	key, err := kr.Key(seal.Index, repo, salt)
	if err != nil {
		return "", nil, err
	}
	defer key.Destroy()
	var w wire.Writer
	w.Raw([]byte(objectMagic))
	w.U16(version)
	w.Raw(salt[:])
	sealed, err := key.Seal(w.Bytes(), plain)
	if err != nil {
		return "", nil, err
	}
	w.Raw(sealed)
	blob = w.Bytes()
	if len(blob) > MaxObjectSize {
		return "", nil, fmt.Errorf("dedup: index object of %d bytes, limit %d", len(blob), MaxObjectSize)
	}
	sum := sha256.Sum256(blob)
	return "index/" + hex.EncodeToString(sum[:]), blob, nil
}

// DecodeObject opens an index object. The blob must hash to its name.
func DecodeObject(kr *seal.Keyring, repo seal.RepoID, name string, blob []byte) ([]pack.Info, error) {
	sum := sha256.Sum256(blob)
	if name != "index/"+hex.EncodeToString(sum[:]) {
		return nil, fmt.Errorf("%w: bytes do not hash to %s", ErrCorrupt, name)
	}
	if len(blob) < headerSize || len(blob) > MaxObjectSize {
		return nil, fmt.Errorf("%w: %d bytes", ErrCorrupt, len(blob))
	}
	h := wire.NewReader(blob[:headerSize])
	magic, v := h.Fixed(4), h.U16()
	var salt seal.Salt
	copy(salt[:], h.Fixed(len(salt)))
	if h.Done() != nil || string(magic) != objectMagic || v != version {
		return nil, fmt.Errorf("%w: header", ErrCorrupt)
	}
	key, err := kr.Key(seal.Index, repo, salt)
	if err != nil {
		return nil, err
	}
	defer key.Destroy()
	plain, err := key.Open(blob[:headerSize], blob[headerSize:])
	if err != nil {
		return nil, fmt.Errorf("%w: does not authenticate (wrong key or repository, or altered)", ErrCorrupt)
	}
	return decodePlain(plain)
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

type loc struct {
	pack   int32
	offset uint32
	stored uint32
	raw    uint32
	codec  uint8
}

// Index maps chunk hashes to locations. Not safe for concurrent use; the
// chunk store serializes access.
type Index struct {
	packs  []PackRef
	byName map[string]int32
	chunks map[hash.Hash]loc
}

// New returns an empty index.
func New() *Index {
	return &Index{byName: map[string]int32{}, chunks: map[hash.Hash]loc{}}
}

// Add records every chunk of a pack. A chunk already located elsewhere keeps
// its first location.
func (x *Index) Add(info pack.Info) {
	pi, ok := x.byName[info.Name]
	if !ok {
		pi = int32(len(x.packs))
		x.packs = append(x.packs, PackRef{Name: info.Name, Salt: info.Salt, Size: info.Size})
		x.byName[info.Name] = pi
	}
	for _, e := range info.Entries {
		if _, dup := x.chunks[e.Hash]; dup {
			continue
		}
		x.chunks[e.Hash] = loc{pack: pi, offset: e.Offset, stored: e.StoredLen, raw: e.RawLen, codec: e.Codec}
	}
}

// Lookup locates a chunk.
func (x *Index) Lookup(h hash.Hash) (Location, bool) {
	l, ok := x.chunks[h]
	if !ok {
		return Location{}, false
	}
	return Location{Pack: x.packs[l.pack], Entry: pack.Entry{Hash: h, Offset: l.offset, StoredLen: l.stored,
		RawLen: l.raw, Codec: l.codec}}, true
}

// Has reports whether a chunk is located.
func (x *Index) Has(h hash.Hash) bool {
	_, ok := x.chunks[h]
	return ok
}

// Len is the number of distinct chunks.
func (x *Index) Len() int { return len(x.chunks) }

// Packs lists the packs added, in order.
func (x *Index) Packs() []PackRef { return append([]PackRef(nil), x.packs...) }
