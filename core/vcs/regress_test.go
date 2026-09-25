package vcs

import (
	"errors"
	"runtime"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/merge"
	"github.com/SmithOperatingSolutions/snapshot-core/core/wire"
)

// allocated reports the bytes f allocates on the heap, averaged over runs.
func allocated(runs int, f func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < runs; i++ {
		f()
	}
	runtime.ReadMemStats(&after)
	return (after.TotalAlloc - before.TotalAlloc) / uint64(runs)
}

// #26: decodeConflict kept looping over the model conflicts a record
// claims after its reader had failed, appending an empty one each time: a
// 6-byte record claiming 10,000 made 10,000 appends before the refusal. A
// truncated record must be refused for what its bytes hold.
func TestRegression_SC26_ATruncatedConflictIsRefusedWithoutFillingItsClaim(t *testing.T) {
	record := func(claimed, present int) []byte {
		var w wire.Writer
		w.U8(uint8(merge.BothChanged))
		w.U8(0)
		w.U8(0)
		w.U8(0) // no base, ours or theirs
		w.Uvarint(uint64(claimed))
		for i := 0; i < present; i++ {
			w.LenBytes(nil) // location
			w.LenBytes(nil) // reason
		}
		return w.Bytes()
	}
	// Positive control, at the limit: a record holding every model
	// conflict it claims decodes them all.
	full := record(maxModelConflicts, maxModelConflicts)
	if c, err := decodeConflict("p", full); err != nil || len(c.Model) != maxModelConflicts {
		t.Fatalf("positive control: a conflict record with %d model conflicts decoded %d, %v",
			maxModelConflicts, len(c.Model), err)
	}

	truncated := record(maxModelConflicts, 0)
	if _, err := decodeConflict("p", truncated); !errors.Is(err, chunk.ErrCorrupt) {
		t.Fatalf("a %d-byte conflict record claiming %d model conflicts: %v, want ErrCorrupt", len(truncated), maxModelConflicts, err)
	}
	const budget = 4 << 10
	if got := allocated(5, func() { _, _ = decodeConflict("p", truncated) }); got > budget {
		t.Errorf("refusing a %d-byte conflict record claiming %d model conflicts allocated %d bytes (budget %d): "+
			"every stored conflict a working set lists costs its claim, not its bytes, to read or walk",
			len(truncated), maxModelConflicts, got, budget)
	}
}
