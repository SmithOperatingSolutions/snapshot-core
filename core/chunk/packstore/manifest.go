package packstore

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/internal/wire"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
)

// The manifest is the BlobStore's root value: which chunk is the root, which
// index objects locate the chunks it reaches, the GC generation, and the
// objects GC has condemned (C4). Sealed under the Refs domain:
//
//	blob       magic "SCMF" | version u16 | salt [32] | seal(Refs key, context = header, plaintext)
//	plaintext  magic "SCMP" | version u16 | seq u64 | gcGen u64 | root [32] |
//	           indexes uvarint | indexes x hash [32] (strictly increasing) |
//	           condemned uvarint | condemned x (kind u8 (1-4) | hash [32] | at i64)
type manifest struct {
	seq       uint64
	gcGen     uint64
	root      hash.Hash
	indexes   [][32]byte // index objects, by the SHA-256 in their names
	condemned []condemned
}

type condemned struct {
	kind uint8 // condemnedPack, condemnedIndex, deletedPack or deletedIndex
	sum  [32]byte
	at   int64 // unix nanoseconds
}

// Condemned kinds.
const (
	condemnedPack  = 1
	condemnedIndex = 2
	deletedPack    = 3 // an orphan pack GC deleted
	deletedIndex   = 4 // an orphan index object GC deleted
)

const (
	manifestMagic = "SCMF"
	plainMagic    = "SCMP"
	manifestV1    = 1
	headerLen     = 4 + 2 + 32
	// maxIndexes keeps the manifest well inside the backend's root size limit.
	maxIndexes   = 100_000
	maxCondemned = 100_000
)

func indexName(sum [32]byte) string { return "index/" + hex.EncodeToString(sum[:]) }

func indexSum(name string) ([32]byte, error) {
	var sum [32]byte
	b, err := hex.DecodeString(strings.TrimPrefix(name, "index/"))
	if err != nil || len(b) != 32 || indexName([32]byte(b)) != name {
		return sum, fmt.Errorf("packstore: index object name %q", name)
	}
	copy(sum[:], b)
	return sum, nil
}

func (m manifest) encodePlain() []byte {
	idx := append([][32]byte(nil), m.indexes...)
	sort.Slice(idx, func(i, j int) bool { return bytes.Compare(idx[i][:], idx[j][:]) < 0 })
	var w wire.Writer
	w.Raw([]byte(plainMagic))
	w.U16(manifestV1)
	w.U64(m.seq)
	w.U64(m.gcGen)
	w.Raw(m.root[:])
	w.Uvarint(uint64(len(idx)))
	for _, s := range idx {
		w.Raw(s[:])
	}
	w.Uvarint(uint64(len(m.condemned)))
	for _, c := range m.condemned {
		w.U8(c.kind)
		w.Raw(c.sum[:])
		w.U64(uint64(c.at))
	}
	return w.Bytes()
}

func decodePlain(b []byte) (manifest, error) {
	var m manifest
	r := wire.NewReader(b)
	magic, v := r.Fixed(4), r.U16()
	m.seq, m.gcGen = r.U64(), r.U64()
	copy(m.root[:], r.Fixed(hash.Size))
	n := r.Uvarint()
	if r.Err() != nil || string(magic) != plainMagic || v != manifestV1 || n > maxIndexes {
		return manifest{}, fmt.Errorf("%w: header", ErrManifest)
	}
	for i := uint64(0); i < n; i++ {
		var s [32]byte
		copy(s[:], r.Fixed(32))
		if r.Err() != nil || (i > 0 && bytes.Compare(m.indexes[i-1][:], s[:]) >= 0) {
			return manifest{}, fmt.Errorf("%w: index list", ErrManifest)
		}
		m.indexes = append(m.indexes, s)
	}
	c := r.Uvarint()
	if r.Err() != nil || c > maxCondemned {
		return manifest{}, fmt.Errorf("%w: condemned list", ErrManifest)
	}
	for i := uint64(0); i < c; i++ {
		var e condemned
		e.kind = r.U8()
		copy(e.sum[:], r.Fixed(32))
		e.at = int64(r.U64())
		if r.Err() != nil || e.kind < condemnedPack || e.kind > deletedIndex {
			return manifest{}, fmt.Errorf("%w: condemned entry", ErrManifest)
		}
		m.condemned = append(m.condemned, e)
	}
	if err := r.Done(); err != nil {
		return manifest{}, fmt.Errorf("%w: %w", ErrManifest, err)
	}
	return m, nil
}

func (m manifest) seal(kr *seal.Keyring, repo seal.RepoID) ([]byte, error) {
	salt, err := seal.NewSalt()
	if err != nil {
		return nil, err
	}
	key, err := kr.Key(seal.Refs, repo, salt)
	if err != nil {
		return nil, err
	}
	defer key.Destroy()
	var w wire.Writer
	w.Raw([]byte(manifestMagic))
	w.U16(manifestV1)
	w.Raw(salt[:])
	sealed, err := key.Seal(w.Bytes(), m.encodePlain())
	if err != nil {
		return nil, err
	}
	w.Raw(sealed)
	return w.Bytes(), nil
}

func openManifest(b []byte, kr *seal.Keyring, repo seal.RepoID) (manifest, error) {
	if len(b) < headerLen {
		return manifest{}, fmt.Errorf("%w: %d bytes", ErrManifest, len(b))
	}
	h := wire.NewReader(b[:headerLen])
	magic, v := h.Fixed(4), h.U16()
	var salt seal.Salt
	copy(salt[:], h.Fixed(len(salt)))
	if h.Done() != nil || string(magic) != manifestMagic || v != manifestV1 {
		return manifest{}, fmt.Errorf("%w: header", ErrManifest)
	}
	key, err := kr.Key(seal.Refs, repo, salt)
	if err != nil {
		return manifest{}, err
	}
	defer key.Destroy()
	plain, err := key.Open(b[:headerLen], b[headerLen:])
	if err != nil {
		return manifest{}, fmt.Errorf("%w: wrong key or repository, or altered", ErrManifest)
	}
	return decodePlain(plain)
}
