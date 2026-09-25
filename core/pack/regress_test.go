package pack

import (
	"encoding/binary"
	"errors"
	"runtime"
	"testing"
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
