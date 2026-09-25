package pack

import (
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
	"github.com/SmithOperatingSolutions/snapshot-core/core/wire"
)

// #24: a pack index's header claims how many entries follow. An index of
// nine bytes claiming 65,536 must be refused without first allocating room
// for 65,536 entries; an honest one-entry index decodes within the same
// budget.
func TestRegression_SC24_AnIndexIsNotSizedByItsClaimedCount(t *testing.T) {
	alloc := func(f func()) uint64 {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		f()
		runtime.ReadMemStats(&after)
		return after.TotalAlloc - before.TotalAlloc
	}
	const budget = 64 << 10
	honest := encodeIndex([]Entry{{Offset: HeaderSize, StoredLen: sealOverhead + 5, RawLen: 5, Codec: CodecRaw}})
	var es []Entry
	var err error
	if used := alloc(func() { es, err = decodeIndex(honest, 1<<20) }); err != nil || len(es) != 1 || used > budget {
		t.Fatalf("positive control: an honest one-entry index decoded as %d entries (%v) after allocating %d bytes, want 1 within %d", len(es), err, used, budget)
	}
	forged := binary.AppendUvarint(binary.LittleEndian.AppendUint16([]byte(indexMagic), version), MaxChunksPerPack)
	used := alloc(func() { es, err = decodeIndex(forged, 1<<20) })
	if !errors.Is(err, ErrCorrupt) || used > budget {
		t.Fatalf("a %d-byte index claiming %d entries: %d entries, %v, after allocating %d bytes; want ErrCorrupt within %d",
			len(forged), MaxChunksPerPack, len(es), err, used, budget)
	}
}

// indexWithOffsets encodes entries as the writer does, except that entry i
// carries the 64-bit offset offs[i] where one is given: an offset the Entry
// type (32 bits) cannot hold, as a forged index can.
func indexWithOffsets(entries []Entry, offs map[int]uint64) []byte {
	var w wire.Writer
	w.Raw([]byte(indexMagic))
	w.U16(version)
	w.Uvarint(uint64(len(entries)))
	for i, e := range entries {
		off, ok := offs[i]
		if !ok {
			off = uint64(e.Offset)
		}
		w.Raw(e.Hash[:])
		w.Uvarint(off)
		w.Uvarint(uint64(e.StoredLen))
		w.Uvarint(uint64(e.RawLen))
		w.U8(e.Codec)
	}
	return w.Bytes()
}

// #26: decodeIndex checked each frame with off+stored > indexOffset, a sum
// that wraps: an offset near 2^64 passed, then truncated to 32 bits, a
// frame about 4 GiB into a pack of at most one. And its walk refused
// overlaps but never checked where the last frame ends. ReadInfo accepted
// such a pack: the chunk is listed, a put deduplicates against it and
// stores nothing, and it never reads back.
func TestRegression_SC26_APacksFramesEndAtItsIndex(t *testing.T) {
	kr, _ := seal.NewKeyring()
	c, _ := NewCodec()
	defer c.Close()
	repo := seal.RepoID{26}
	w, err := NewWriterSized(kr, repo, c, 1<<20, 64<<10)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		d := []byte(fmt.Sprintf("chunk %d %s", i, strings.Repeat("q", 12*i)))
		if err := w.Add(hash.Sum(d), d); err != nil {
			t.Fatal(err)
		}
	}
	b, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	es := b.Info.Entries
	indexOffset := binary.LittleEndian.Uint64(b.Bytes[len(b.Bytes)-TrailerSize:])
	byOffset := make([]int, len(es))
	for i := range byOffset {
		byOffset[i] = i
	}
	sort.Slice(byOffset, func(i, j int) bool { return es[byOffset[i]].Offset < es[byOffset[j]].Offset })
	first, last := byOffset[0], byOffset[len(byOffset)-1]

	// Positive control: the writer's frames run adjacent from the header
	// right up to the index; resealed as they are, they read.
	if es[first].Offset != HeaderSize || uint64(es[last].Offset)+uint64(es[last].StoredLen) != indexOffset {
		t.Fatalf("fixture: frames span %d..%d, want %d..%d (the index offset): the control would not test the boundary",
			es[first].Offset, uint64(es[last].Offset)+uint64(es[last].StoredLen), HeaderSize, indexOffset)
	}
	honest := resealPlain(t, kr, repo, b, indexWithOffsets(es, nil))
	if _, err := ReadInfo(Name(honest), honest, kr, repo); err != nil {
		t.Fatalf("positive control: a real pack whose frames end at its index is refused: %v", err)
	}

	intoIndex := append([]Entry(nil), es...)
	intoIndex[last].StoredLen++ // and the raw length with it: consistent for either codec
	intoIndex[last].RawLen++
	for label, plain := range map[string][]byte{
		"an offset of 2^64 - 20, whose sum with its length wraps":      indexWithOffsets(es, map[int]uint64{last: 1<<64 - 20}),
		"an offset of 2^32 + 40, which truncates to the first frame's": indexWithOffsets(es, map[int]uint64{first: 1<<32 + HeaderSize}),
		"a last frame ending a byte into the index":                    indexWithOffsets(intoIndex, nil),
	} {
		bad := resealPlain(t, kr, repo, b, plain)
		if _, err := ReadInfo(Name(bad), bad, kr, repo); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: ReadInfo = %v, want ErrCorrupt: a put deduplicates against a chunk the pack does not "+
				"hold where its index says, stores nothing, and the chunk never reads back", label, err)
		}
	}
}
