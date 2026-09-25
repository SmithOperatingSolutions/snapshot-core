package packstore

import (
	"errors"
	"fmt"

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
	var p wire.Writer
	p.Raw([]byte(journalPlainMagic))
	p.U16(journalV1)
	p.Raw(r.expected[:])
	p.Raw(r.next[:])
	p.U64(r.gcGen)
	p.Raw(r.salt[:])
	p.Uvarint(uint64(len(r.frames)))
	for _, f := range r.frames {
		p.Raw(f.h[:])
		p.Uvarint(uint64(f.raw))
		p.U8(f.codec)
		p.Uvarint(uint64(len(f.sealed)))
		p.Raw(f.sealed)
	}
	p.Uvarint(uint64(len(r.counted)))
	for _, h := range r.counted {
		p.Raw(h[:])
	}
	plain := p.Bytes()
	if len(plain)+journalHeaderLen+pack.FrameOverhead > maxJournal {
		return nil, fmt.Errorf("packstore: a journal record of %d bytes, over %d", len(plain), maxJournal)
	}
	salt, err := seal.NewSalt()
	if err != nil {
		return nil, err
	}
	key, err := kr.Key(seal.Journal, repo, salt)
	if err != nil {
		return nil, err
	}
	defer key.Destroy()
	var h wire.Writer
	h.Raw([]byte(journalMagic))
	h.U16(journalV1)
	h.U32(uint32(len(plain) + pack.FrameOverhead))
	h.Raw(salt[:])
	sealed, err := key.Seal(h.Bytes(), plain)
	if err != nil {
		return nil, err
	}
	return append(h.Bytes(), sealed...), nil
}

// decodeJournal returns the records of a journal up to the last complete
// one that authenticates: a record cut short, or one that does not
// authenticate, is what a crash in flight leaves, and it and everything
// after it are discarded. used is the length of the records returned.
// A record that authenticates and does not decode, and one that does not
// follow on from the record before it, are errJournalCorrupt.
func decodeJournal(kr *seal.Keyring, repo seal.RepoID, b []byte) (records []jrecord, used int, err error) {
	for used+journalHeaderLen <= len(b) {
		hb := b[used : used+journalHeaderLen]
		h := wire.NewReader(hb)
		magic, v, n := h.Fixed(4), h.U16(), h.U32()
		var salt seal.Salt
		copy(salt[:], h.Fixed(len(salt)))
		if h.Done() != nil || string(magic) != journalMagic || v != journalV1 ||
			n > maxJournal-journalHeaderLen || int(n) > len(b)-used-journalHeaderLen {
			break // debris, or a record cut short
		}
		key, err := kr.Key(seal.Journal, repo, salt)
		if err != nil {
			return nil, 0, err
		}
		plain, err := key.Open(hb, b[used+journalHeaderLen:used+journalHeaderLen+int(n)])
		key.Destroy()
		if err != nil {
			break // torn, or not this repository's
		}
		r, err := decodeRecord(plain)
		if err != nil {
			return nil, 0, err
		}
		if len(records) > 0 && r.expected != records[len(records)-1].next {
			return nil, 0, fmt.Errorf("%w: record %d replaced root %s, and the one before set %s", errJournalCorrupt,
				len(records), r.expected.Short(), records[len(records)-1].next.Short())
		}
		records = append(records, r)
		used += journalHeaderLen + int(n)
	}
	return records, used, nil
}

// decodeRecord decodes a record's plaintext. No count sizes an allocation:
// every frame and hash is read from bytes that are there.
func decodeRecord(plain []byte) (jrecord, error) {
	var r jrecord
	p := wire.NewReader(plain)
	magic, v := p.Fixed(4), p.U16()
	copy(r.expected[:], p.Fixed(hash.Size))
	copy(r.next[:], p.Fixed(hash.Size))
	r.gcGen = p.U64()
	copy(r.salt[:], p.Fixed(len(r.salt)))
	frames := p.Uvarint()
	if p.Err() != nil || string(magic) != journalPlainMagic || v != journalV1 || frames > pack.MaxChunksPerPack {
		return jrecord{}, fmt.Errorf("%w: header", errJournalCorrupt)
	}
	for i := uint64(0); i < frames; i++ {
		var f jframe
		copy(f.h[:], p.Fixed(hash.Size))
		raw := p.Uvarint()
		f.codec = p.U8()
		stored := p.Uvarint()
		if p.Err() != nil || raw > pack.MaxChunkSize || f.codec > pack.CodecZstd || stored > maxFrameStored {
			return jrecord{}, fmt.Errorf("%w: frame %d", errJournalCorrupt, i)
		}
		f.raw = uint32(raw)
		f.sealed = p.Fixed(int(stored))
		if p.Err() != nil {
			return jrecord{}, fmt.Errorf("%w: frame %d", errJournalCorrupt, i)
		}
		r.frames = append(r.frames, f)
	}
	counted := p.Uvarint()
	if p.Err() != nil || counted > maxCounted {
		return jrecord{}, fmt.Errorf("%w: counted", errJournalCorrupt)
	}
	for i := uint64(0); i < counted; i++ {
		var h hash.Hash
		copy(h[:], p.Fixed(hash.Size))
		if p.Err() != nil {
			return jrecord{}, fmt.Errorf("%w: counted %d", errJournalCorrupt, i)
		}
		r.counted = append(r.counted, h)
	}
	if err := p.Done(); err != nil {
		return jrecord{}, fmt.Errorf("%w: %w", errJournalCorrupt, err)
	}
	return r, nil
}
