package dedup

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/internal/wire"
	"github.com/SmithOperatingSolutions/snapshot-core/core/pack"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
)

// Whatever decodes re-encodes to the same bytes: one encoding per value.
func FuzzDecodePlain(f *testing.F) {
	seed, _ := encodePlain([]pack.Info{{Name: PackName([32]byte{1}), Size: 1000,
		Entries: []pack.Entry{{Hash: hash.Sum([]byte("a")), Offset: pack.HeaderSize, StoredLen: 40, RawLen: 12}}}})
	f.Add(seed)
	f.Add([]byte("SCIP"))
	f.Fuzz(func(t *testing.T, b []byte) {
		packs, err := decodePlain(b)
		if err != nil {
			return
		}
		again, err := encodePlain(packs)
		if err != nil || !bytes.Equal(again, b) {
			t.Fatalf("decoded an index object that does not re-encode to itself (%v)", err)
		}
	})
}

// sealPlain seals hand-built plaintext as a real index object: it
// authenticates, so only the decoder's own checks can refuse it.
func sealPlain(t *testing.T, kr *seal.Keyring, repo seal.RepoID, plain []byte) (string, []byte) {
	t.Helper()
	salt, _ := seal.NewSalt()
	key, err := kr.Key(seal.Index, repo, salt)
	if err != nil {
		t.Fatal(err)
	}
	var w wire.Writer
	w.Raw([]byte(objectMagic))
	w.U16(version)
	w.Raw(salt[:])
	sealed, err := key.Seal(w.Bytes(), plain)
	if err != nil {
		t.Fatal(err)
	}
	w.Raw(sealed)
	sum := sha256.Sum256(w.Bytes())
	return "index/" + hex.EncodeToString(sum[:]), w.Bytes()
}

type rec struct {
	sum     byte // first byte of the pack hash
	size    uint64
	entries []ent
}
type ent struct {
	hash             byte
	off, stored, raw uint64
	codec            uint8
}

func plain(recs ...rec) []byte {
	var w wire.Writer
	w.Raw([]byte(plainMagic))
	w.U16(version)
	w.Uvarint(uint64(len(recs)))
	for _, r := range recs {
		w.Raw(append([]byte{r.sum}, make([]byte, 31)...))
		w.Raw(make([]byte, 32)) // salt
		w.Uvarint(r.size)
		w.Uvarint(uint64(len(r.entries)))
		for _, e := range r.entries {
			w.Raw(append([]byte{e.hash}, make([]byte, 31)...))
			w.Uvarint(e.off)
			w.Uvarint(e.stored)
			w.Uvarint(e.raw)
			w.U8(e.codec)
		}
	}
	return w.Bytes()
}

func TestForgedObjectsAreRefused(t *testing.T) {
	kr, _ := seal.NewKeyring()
	repo := seal.RepoID{4}
	e1 := ent{hash: 1, off: pack.HeaderSize, stored: 40, raw: 12}
	e2 := ent{hash: 2, off: pack.HeaderSize + 40, stored: 40, raw: 12}
	honest := plain(rec{sum: 1, size: 1000, entries: []ent{e1, e2}}, rec{sum: 2, size: 1000})
	if name, blob := sealPlain(t, kr, repo, honest); true {
		if _, err := DecodeObject(kr, repo, name, blob); err != nil {
			t.Fatalf("positive control: an honest hand-built object is refused: %v", err)
		}
	}
	for label, p := range map[string][]byte{
		"packs out of order":         plain(rec{sum: 2, size: 1000}, rec{sum: 1, size: 1000}),
		"pack listed twice":          plain(rec{sum: 1, size: 1000}, rec{sum: 1, size: 1000}),
		"pack smaller than a header": plain(rec{sum: 1, size: 10}),
		"pack over the size limit":   plain(rec{sum: 1, size: pack.MaxPackSize + 1}),
		"entries out of order":       plain(rec{sum: 1, size: 1000, entries: []ent{e2, e1}}),
		"entry listed twice": plain(rec{sum: 1, size: 1000, entries: []ent{e1,
			{hash: 1, off: pack.HeaderSize + 40, stored: 40, raw: 12}}}),
		"frame inside the header": plain(rec{sum: 1, size: 1000, entries: []ent{{hash: 1, off: 4, stored: 30, raw: 2}}}),
		"frame past the pack":     plain(rec{sum: 1, size: 1000, entries: []ent{{hash: 1, off: 990, stored: 40, raw: 12}}}),
		"chunk over the limit":    plain(rec{sum: 1, size: 1000, entries: []ent{{hash: 1, off: 40, stored: 40, raw: pack.MaxChunkSize + 1}}}),
		"unknown codec":           plain(rec{sum: 1, size: 1000, entries: []ent{{hash: 1, off: 40, stored: 40, raw: 12, codec: 5}}}),
	} {
		name, blob := sealPlain(t, kr, repo, p)
		if _, err := DecodeObject(kr, repo, name, blob); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: DecodeObject accepted a forged object (err=%v)", label, err)
		}
	}
}
