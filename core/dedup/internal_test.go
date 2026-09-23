package dedup

import (
	"bytes"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/pack"
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
