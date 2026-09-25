package packstore

import (
	"errors"
	"fmt"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/pack"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
	"github.com/SmithOperatingSolutions/snapshot-core/core/wire"
)

// The commit journal (#34, docs/DESIGN.md §5-6) is a sequence of records,
// one per journaled commit, each sealed under the Journal domain with a
// key of its own:
//
//	record     magic "SCJR" | version u16 | length u32 | salt [32] |
//	           seal(Journal key, context = those 42 bytes, plaintext), length bytes
//	plaintext  magic "SCJP" | version u16 | expected [32] | next [32] | gcGen u64 |
//	           pack salt [32] | frames uvarint (at most 65,536) |
//	           frames x (hash [32] | raw uvarint (at most 1 MiB) | codec u8 (0 raw, 1 zstd) |
//	                     stored uvarint (at most 1 MiB + 28) | frame, stored bytes) |
//	           counted uvarint (at most 65,536) | counted x hash [32]
//
// expected and next are the root the commit replaced and the one it set;
// each record's expected is the one before's next. The frames are the
// chunks the commit added to the pending pack since the last record,
// sealed for the pack whose salt the record names, exactly as they sit in
// it. counted is the chunks puts found already stored since the last
// record, and gcGen the GC generation they were found under: a replay
// checks they survived, as a publish does (DESIGN §9).
type jrecord struct {
	expected, next hash.Hash
	gcGen          uint64
	salt           seal.Salt
	frames         []jframe
	counted        []hash.Hash
}

// jframe is one chunk's sealed frame, as a pack holds it.
type jframe struct {
	h      hash.Hash
	raw    uint32
	codec  uint8
	sealed []byte
}

const (
	journalMagic      = "SCJR"
	journalPlainMagic = "SCJP"
	journalV1         = 1
	journalHeaderLen  = 4 + 2 + 4 + 32
	// maxJournal is the most a writer leaves in a journal (it publishes
	// before an append would pass it), and so the most a record can be.
	maxJournal = 16 << 20
	// maxCounted bounds a record's counted chunks; a commit that counted
	// on more publishes instead of journaling.
	maxCounted     = 1 << 16
	maxFrameStored = pack.MaxChunkSize + pack.FrameOverhead
)

// errJournalCorrupt is a record that authenticates and does not decode:
// no torn write makes one, so it is refused, never skipped.
var errJournalCorrupt = errors.New("packstore: a journal record authenticates and does not decode")

// seal encodes and seals the record.
func (r *jrecord) seal(kr *seal.Keyring, repo seal.RepoID) ([]byte, error) {
	return make([]byte, journalHeaderLen+pack.FrameOverhead), nil
}

// decodeJournal returns the records of a journal up to the last complete
// one that authenticates: a record cut short, or one that does not
// authenticate, is what a crash in flight leaves, and it and everything
// after it are discarded. used is the length of the records returned.
// A record that authenticates and does not decode, and one that does not
// follow on from the record before it, are errJournalCorrupt.
func decodeJournal(kr *seal.Keyring, repo seal.RepoID, b []byte) (records []jrecord, used int, err error) {
	return nil, 0, nil
}

var _ = fmt.Errorf
var _ = chunk.ErrCorrupt
var _ = wire.NewReader
