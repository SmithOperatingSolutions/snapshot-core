package dedup

import (
	"errors"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/pack"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
)

// frameOverhead is what sealing adds to a frame: the AES-GCM nonce and tag.
const frameOverhead = 28

// #26: an index entry's stored length was checked only against its pack's
// size (up to a gibibyte), never against its raw length, the way pack's own
// index decoder does: a raw frame is the chunk plus the seal, a compressed
// one is smaller than that. The claim drives GC repack's make([]byte,
// StoredLen) before any read, and the size of every chunk read's range. An
// entry of about 50 bytes could make GC allocate a gibibyte.
func TestRegression_SC26_AnEntryWhoseStoredLengthDisagreesWithItsRawIsRefused(t *testing.T) {
	kr, _ := seal.NewKeyring()
	repo := seal.RepoID{26}
	const size = 8 << 20 // room for any honest frame; the lies below fit in it too
	decodes := func(e ent) error {
		name, blob := sealPlain(t, kr, repo, plain(rec{sum: 1, size: size, entries: []ent{e}}))
		_, err := DecodeObject(kr, repo, name, blob)
		return err
	}
	const most = pack.MaxChunkSize
	// Positive controls, at the limits: the largest raw frame, and the
	// largest compressed frame (one byte smaller than raw).
	for label, e := range map[string]ent{
		"a raw frame of the largest chunk":    {hash: 1, off: pack.HeaderSize, stored: most + frameOverhead, raw: most, codec: pack.CodecRaw},
		"a compressed frame one byte smaller": {hash: 1, off: pack.HeaderSize, stored: most - 1 + frameOverhead, raw: most, codec: pack.CodecZstd},
		"an empty raw chunk":                  {hash: 1, off: pack.HeaderSize, stored: frameOverhead, raw: 0, codec: pack.CodecRaw},
	} {
		if err := decodes(e); err != nil {
			t.Fatalf("positive control: %s (stored %d, raw %d) is refused: %v", label, e.stored, e.raw, err)
		}
	}
	for label, e := range map[string]ent{
		"a raw frame a byte long":                {hash: 1, off: pack.HeaderSize, stored: 12 + frameOverhead + 1, raw: 12, codec: pack.CodecRaw},
		"a raw frame a byte short":               {hash: 1, off: pack.HeaderSize, stored: 12 + frameOverhead - 1, raw: 12, codec: pack.CodecRaw},
		"a raw frame of 7 MiB for a 1 KiB chunk": {hash: 1, off: pack.HeaderSize, stored: 7 << 20, raw: 1024, codec: pack.CodecRaw},
		"a compressed frame no smaller":          {hash: 1, off: pack.HeaderSize, stored: 12 + frameOverhead, raw: 12, codec: pack.CodecZstd},
		"a compressed frame of 7 MiB":            {hash: 1, off: pack.HeaderSize, stored: 7 << 20, raw: most, codec: pack.CodecZstd},
		"a compressed frame shorter than a seal": {hash: 1, off: pack.HeaderSize, stored: frameOverhead - 1, raw: 12, codec: pack.CodecZstd},
	} {
		if err := decodes(e); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s decoded (stored %d bytes for a %d-byte chunk; err=%v): a stored length not tied to the "+
				"raw one lets an index entry make GC repack allocate up to a gibibyte before reading a byte, "+
				"and a chunk read ask the backend for all of it", label, e.stored, e.raw, err)
		}
	}
}
