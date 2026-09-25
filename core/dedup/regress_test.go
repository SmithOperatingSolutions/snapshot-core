package dedup

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"

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

// #26: pack's own index decoder lays a pack's frames out: none overlaps
// another or the header, and none reaches past the index offset into the
// index. dedup checked each frame alone against the pack's end, less the
// trailer, with an offset sum that could wrap. An index object could list
// two chunks in one frame, or a chunk in the pack's index: a put
// deduplicates against it and stores nothing, and the chunk never reads
// back (at most one frame at an offset authenticates).
func TestRegression_SC26_IndexFramesLieWhereTheirPackHoldsThem(t *testing.T) {
	kr, _ := seal.NewKeyring()
	repo := seal.RepoID{27}
	decodes := func(p []byte) error {
		name, blob := sealPlain(t, kr, repo, p)
		_, err := DecodeObject(kr, repo, name, blob)
		return err
	}

	// Positive control, a real pack: its frames are adjacent from the
	// header right up to its index, so its size is the smallest that
	// holds them. One byte less and the last frame reaches into the index.
	c, err := pack.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	w, err := pack.NewWriterSized(kr, repo, c, 1<<20, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 40; i++ {
		d := []byte(fmt.Sprintf("chunk %d %s", i, strings.Repeat("z", 10*i)))
		if err := w.Add(hash.Sum(d), d); err != nil {
			t.Fatal(err)
		}
	}
	built, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	real, err := encodePlain([]pack.Info{built.Info})
	if err != nil {
		t.Fatal(err)
	}
	if err := decodes(real); err != nil {
		t.Fatalf("positive control: a real pack's record (%d chunks, %d bytes) is refused: %v", len(built.Info.Entries), built.Info.Size, err)
	}
	short := built.Info
	short.Size--
	shortPlain, _ := encodePlain([]pack.Info{short})

	// Positive control by hand: two adjacent frames, [40,80) and [80,120),
	// then the sealed index (magic 4, version 2, count 1, two entries of
	// hash 32 + three one-byte uvarints + codec 1, seal 28: 107 bytes),
	// then the 16-byte trailer. 243 bytes; 242 is one too few.
	e1 := ent{hash: 1, off: pack.HeaderSize, stored: 40, raw: 12}
	e2 := ent{hash: 2, off: pack.HeaderSize + 40, stored: 40, raw: 12}
	if err := decodes(plain(rec{sum: 1, size: 243, entries: []ent{e1, e2}})); err != nil {
		t.Fatalf("positive control: adjacent frames right up to the index offset are refused: %v", err)
	}

	overlapping := ent{hash: 2, off: pack.HeaderSize + 39, stored: 40, raw: 12}
	for label, p := range map[string][]byte{
		"a frame overlapping the one before it":       plain(rec{sum: 1, size: 1000, entries: []ent{e1, overlapping}}),
		"a frame overlapping the one after it":        plain(rec{sum: 1, size: 1000, entries: []ent{{hash: 1, off: pack.HeaderSize + 39, stored: 40, raw: 12}, {hash: 2, off: pack.HeaderSize, stored: 40, raw: 12}}}),
		"two chunks in one frame":                     plain(rec{sum: 1, size: 1000, entries: []ent{e1, {hash: 2, off: pack.HeaderSize, stored: 40, raw: 12}}}),
		"a frame reaching a byte into the index":      plain(rec{sum: 1, size: 242, entries: []ent{e1, e2}}),
		"a real pack's frames, a byte into its index": shortPlain,
		"an offset whose sum with its length wraps":   plain(rec{sum: 1, size: 1000, entries: []ent{{hash: 1, off: 1<<64 - 20, stored: 40, raw: 12}}}),
	} {
		if err := decodes(p); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s decoded (err=%v): a put deduplicates against a chunk listed where its pack cannot hold "+
				"it, stores nothing, and the chunk never reads back", label, err)
		}
	}
}

// #26: two guards the layout walk cannot stand in for, since both act
// before it sees an entry. An offset past 32 bits is refused, not
// truncated into a place in the pack where it would fit (2^32 + 40 reads
// as 40); and an index longer than its whole pack is refused rather than
// wrapping the index offset around to the top of the range, where every
// frame would end before it. A frame at the same place with a true offset,
// in a pack that holds it and its index, is the positive control.
func TestRegression_SC26_AnEntryCannotWrapIntoPlace(t *testing.T) {
	kr, _ := seal.NewKeyring()
	repo := seal.RepoID{28}
	decodes := func(p []byte) error {
		name, blob := sealPlain(t, kr, repo, p)
		_, err := DecodeObject(kr, repo, name, blob)
		return err
	}
	empty := ent{hash: 1, off: pack.HeaderSize, stored: frameOverhead, raw: 0}
	// 40 header + 28 frame + 71 index (4 + 2 + 1 + 32 + 1 + 1 + 1 + 1, sealed) + 16 trailer.
	if err := decodes(plain(rec{sum: 1, size: 155, entries: []ent{empty}})); err != nil {
		t.Fatalf("positive control: an empty chunk's frame in a pack of 155 bytes is refused: %v", err)
	}
	for label, p := range map[string][]byte{
		"an offset of 2^32 + 40":                          plain(rec{sum: 1, size: 1000, entries: []ent{{hash: 1, off: 1<<32 + pack.HeaderSize, stored: 40, raw: 12}}}),
		"an index longer than the pack after its trailer": plain(rec{sum: 1, size: 84, entries: []ent{empty}}),
	} {
		if err := decodes(p); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s decoded (err=%v): a put deduplicates against a chunk listed where its pack cannot hold "+
				"it, stores nothing, and the chunk never reads back", label, err)
		}
	}
}
