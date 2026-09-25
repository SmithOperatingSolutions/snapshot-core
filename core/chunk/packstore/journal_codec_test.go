package packstore

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/pack"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
	"github.com/SmithOperatingSolutions/snapshot-core/core/wire"
)

// journalFrames seals n chunks for one pack, as the pending pack holds
// them, and returns them with the pack's salt.
func journalFrames(t testing.TB, kr *seal.Keyring, repo seal.RepoID, seed string, n int) (seal.Salt, []jframe, [][]byte) {
	t.Helper()
	codec, err := pack.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	defer codec.Close()
	w, err := pack.NewWriter(kr, repo, codec, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	var frames []jframe
	var datas [][]byte
	for i := range n {
		data := bytes.Repeat([]byte(fmt.Sprintf("%s-%d ", seed, i)), 20+i)
		h := hash.Sum(data)
		payload, c := codec.Compress(data)
		sealed, err := w.Seal(h, payload)
		if err != nil {
			t.Fatal(err)
		}
		frames = append(frames, jframe{h: h, raw: uint32(len(data)), codec: c, sealed: sealed})
		datas = append(datas, data)
	}
	return w.Salt(), frames, datas
}

// chainedRecords is n records, each following on from the one before.
func chainedRecords(t testing.TB, kr *seal.Keyring, repo seal.RepoID, n int) []jrecord {
	t.Helper()
	var out []jrecord
	prev := hash.Sum([]byte("published root"))
	for i := range n {
		salt, frames, _ := journalFrames(t, kr, repo, fmt.Sprintf("rec%d", i), 3+i)
		next := hash.Sum([]byte(fmt.Sprintf("root %d", i)))
		out = append(out, jrecord{expected: prev, next: next, gcGen: 4, salt: salt, frames: frames,
			counted: []hash.Hash{hash.Sum([]byte(fmt.Sprintf("counted %d", i)))}})
		prev = next
	}
	return out
}

func sealRecords(t testing.TB, kr *seal.Keyring, repo seal.RepoID, recs []jrecord) ([]byte, []int) {
	t.Helper()
	var b []byte
	var ends []int
	for i := range recs {
		s, err := recs[i].seal(kr, repo)
		if err != nil {
			t.Fatal(err)
		}
		b = append(b, s...)
		ends = append(ends, len(b))
	}
	return b, ends
}

func sameRecord(a, b jrecord) bool {
	if a.expected != b.expected || a.next != b.next || a.gcGen != b.gcGen || a.salt != b.salt ||
		len(a.frames) != len(b.frames) || len(a.counted) != len(b.counted) {
		return false
	}
	for i := range a.frames {
		x, y := a.frames[i], b.frames[i]
		if x.h != y.h || x.raw != y.raw || x.codec != y.codec || !bytes.Equal(x.sealed, y.sealed) {
			return false
		}
	}
	for i := range a.counted {
		if a.counted[i] != b.counted[i] {
			return false
		}
	}
	return true
}

func TestJournalRecordsRoundTrip(t *testing.T) {
	kr := goldenKeyring(t)
	recs := chainedRecords(t, kr, goldenRepo, 3)
	b, _ := sealRecords(t, kr, goldenRepo, recs)
	got, used, err := decodeJournal(kr, goldenRepo, b)
	if err != nil {
		t.Fatalf("decoding a journal of three records: %v", err)
	}
	if used != len(b) || len(got) != len(recs) {
		t.Fatalf("decoded %d records from %d of %d bytes, want 3 from all: acknowledged commits would not replay", len(got), used, len(b))
	}
	for i := range recs {
		if !sameRecord(got[i], recs[i]) {
			t.Fatalf("record %d decodes to something other than was sealed: a replay would publish another root or other chunks", i)
		}
	}
}

// A crash in flight leaves the last record cut short, wherever the cut
// falls: the records before it replay, it and anything after do not.
func TestATornJournalTailIsDiscarded(t *testing.T) {
	kr := goldenKeyring(t)
	recs := chainedRecords(t, kr, goldenRepo, 2)
	b, ends := sealRecords(t, kr, goldenRepo, recs)
	if got, used, err := decodeJournal(kr, goldenRepo, b); err != nil || len(got) != 2 || used != len(b) {
		t.Fatalf("positive control: the whole journal decodes to %d records over %d bytes (%v), want 2 over %d", len(got), used, err, len(b))
	}
	for cut := ends[0]; cut < len(b); cut++ {
		got, used, err := decodeJournal(kr, goldenRepo, b[:cut])
		if err != nil || len(got) != 1 || used != ends[0] || !sameRecord(got[0], recs[0]) {
			t.Fatalf("a journal cut %d bytes into its second record decodes to %d records over %d bytes (%v); want the first "+
				"alone, over %d: a torn append must neither lose the commit before it nor replay itself", cut-ends[0], len(got), used, err, ends[0])
		}
	}
	for _, tail := range [][]byte{make([]byte, 4096), []byte(journalMagic), append([]byte(journalMagic), 1, 0, 0xff, 0xff)} {
		got, used, err := decodeJournal(kr, goldenRepo, append(append([]byte(nil), b...), tail...))
		if err != nil || len(got) != 2 || used != len(b) {
			t.Fatalf("a journal ending in %d bytes of debris decodes to %d records over %d bytes (%v), want 2 over %d", len(tail), len(got), used, err, len(b))
		}
	}
}

// A record that does not authenticate is what a torn write can leave:
// it ends the journal there.
func TestARecordThatDoesNotAuthenticateEndsTheJournal(t *testing.T) {
	kr := goldenKeyring(t)
	recs := chainedRecords(t, kr, goldenRepo, 3)
	b, ends := sealRecords(t, kr, goldenRepo, recs)
	bad := append([]byte(nil), b...)
	bad[ends[1]-5] ^= 1 // inside the second record's seal
	got, used, err := decodeJournal(kr, goldenRepo, bad)
	if err != nil || len(got) != 1 || used != ends[0] {
		t.Fatalf("with its second record altered, the journal decodes to %d records over %d bytes (%v), want the first alone", len(got), used, err)
	}
	other, err := seal.KeyringFromBytes(bytes.Repeat([]byte{0x77}, 32))
	if err != nil {
		t.Fatal(err)
	}
	if got, _, err := decodeJournal(other, goldenRepo, b); err != nil || len(got) != 0 {
		t.Fatalf("under another key the journal decodes to %d records (%v), want none: records would replay into the wrong repository", len(got), err)
	}
	if got, _, err := decodeJournal(kr, seal.RepoID{0x99}, b); err != nil || len(got) != 0 {
		t.Fatalf("for another repository the journal decodes to %d records (%v), want none", len(got), err)
	}
}

func TestAJournalRecordThatDoesNotFollowOnIsRefused(t *testing.T) {
	kr := goldenKeyring(t)
	recs := chainedRecords(t, kr, goldenRepo, 2)
	recs[1].expected = hash.Sum([]byte("some other root"))
	b, _ := sealRecords(t, kr, goldenRepo, recs)
	if _, _, err := decodeJournal(kr, goldenRepo, b); !errors.Is(err, errJournalCorrupt) {
		t.Fatalf("a journal whose second record replaced a root the first never set = %v, want errJournalCorrupt: "+
			"a replay would publish a history that never happened", err)
	}
}

// sealJournalPlain seals any plaintext as a record under the right key:
// only the decoder's own checks can refuse it.
func sealJournalPlain(t testing.TB, kr *seal.Keyring, repo seal.RepoID, plain []byte) []byte {
	t.Helper()
	salt, err := seal.NewSalt()
	if err != nil {
		t.Fatal(err)
	}
	key, err := kr.Key(seal.Journal, repo, salt)
	if err != nil {
		t.Fatal(err)
	}
	defer key.Destroy()
	var h wire.Writer
	h.Raw([]byte(journalMagic))
	h.U16(journalV1)
	h.U32(uint32(len(plain) + pack.FrameOverhead))
	h.Raw(salt[:])
	sealed, err := key.Seal(h.Bytes(), plain)
	if err != nil {
		t.Fatal(err)
	}
	return append(h.Bytes(), sealed...)
}

// plainRecord writes a record's plaintext field by field, so a forgery
// can set any of them.
type plainRecord struct {
	magic           string
	version         uint16
	frames          uint64 // the count written
	frameList       []jframe
	counted         uint64
	countedList     []hash.Hash
	trailing        []byte
	truncateAt      int // cut the plaintext here (0: whole)
	expected, next  hash.Hash
	rawOverride     map[int]uint64
	storedOverride  map[int]uint64
	codecOverride   map[int]uint8
	omitFrameBodies bool
}

func (p plainRecord) encode() []byte {
	var w wire.Writer
	w.Raw([]byte(p.magic))
	w.U16(p.version)
	w.Raw(p.expected[:])
	w.Raw(p.next[:])
	w.U64(1)
	w.Raw(make([]byte, 32))
	w.Uvarint(p.frames)
	for i, f := range p.frameList {
		w.Raw(f.h[:])
		raw := uint64(f.raw)
		if v, ok := p.rawOverride[i]; ok {
			raw = v
		}
		w.Uvarint(raw)
		codec := f.codec
		if v, ok := p.codecOverride[i]; ok {
			codec = v
		}
		w.U8(codec)
		stored := uint64(len(f.sealed))
		if v, ok := p.storedOverride[i]; ok {
			stored = v
		}
		w.Uvarint(stored)
		if !p.omitFrameBodies {
			w.Raw(f.sealed)
		}
	}
	w.Uvarint(p.counted)
	for _, h := range p.countedList {
		w.Raw(h[:])
	}
	w.Raw(p.trailing)
	b := w.Bytes()
	if p.truncateAt > 0 {
		b = b[:p.truncateAt]
	}
	return b
}

func validPlain(frames []jframe) plainRecord {
	return plainRecord{magic: journalPlainMagic, version: journalV1, frames: uint64(len(frames)), frameList: frames,
		counted: 1, countedList: []hash.Hash{hash.Sum([]byte("c"))}, expected: hash.Sum([]byte("e")), next: hash.Sum([]byte("n"))}
}

// Forgeries sealed under the right key: each must be refused as corrupt,
// never skipped as a torn tail (which would drop the commits after it
// silently) and never accepted.
func TestForgedJournalRecordsAreRefused(t *testing.T) {
	kr := goldenKeyring(t)
	_, frames, _ := journalFrames(t, kr, goldenRepo, "forge", 2)
	if got, _, err := decodeJournal(kr, goldenRepo, sealJournalPlain(t, kr, goldenRepo, validPlain(frames).encode())); err != nil || len(got) != 1 {
		t.Fatalf("positive control: a well-formed record sealed by hand decodes to %d records (%v), want 1", len(got), err)
	}
	cases := map[string]func(p *plainRecord){
		"plaintext magic":   func(p *plainRecord) { p.magic = "SCJX" },
		"plaintext version": func(p *plainRecord) { p.version = 2 },
		"codec 2":           func(p *plainRecord) { p.codecOverride = map[int]uint8{1: 2} },
		"raw over a chunk":  func(p *plainRecord) { p.rawOverride = map[int]uint64{0: pack.MaxChunkSize + 1} },
		"stored over a frame": func(p *plainRecord) {
			p.storedOverride = map[int]uint64{0: maxFrameStored + 1}
		},
		"stored past the end": func(p *plainRecord) { p.storedOverride = map[int]uint64{1: 1 << 20} },
		"frames over a pack":  func(p *plainRecord) { p.frames = pack.MaxChunksPerPack + 1 },
		"frames past the end": func(p *plainRecord) { p.frames = 5 },
		"counted over the limit": func(p *plainRecord) {
			p.counted = maxCounted + 1
		},
		"counted past the end": func(p *plainRecord) { p.counted = 3 },
		"trailing bytes":       func(p *plainRecord) { p.trailing = []byte{0} },
		"cut short":            func(p *plainRecord) { p.truncateAt = 40 },
	}
	for name, forge := range cases {
		p := validPlain(frames)
		forge(&p)
		if got, _, err := decodeJournal(kr, goldenRepo, sealJournalPlain(t, kr, goldenRepo, p.encode())); !errors.Is(err, errJournalCorrupt) {
			t.Errorf("%s: a forged record decodes to %d records (err %v), want errJournalCorrupt", name, len(got), err)
		}
	}
}

// Every limit is accepted at exactly the limit.
func TestJournalLimitsAreAcceptedAtTheLimit(t *testing.T) {
	kr := goldenKeyring(t)
	_, frames, _ := journalFrames(t, kr, goldenRepo, "limit", 1)
	// The most frames a pack holds: empty chunks, each its 28-byte seal.
	empty := jframe{h: hash.Sum(nil), raw: 0, codec: pack.CodecRaw, sealed: make([]byte, pack.FrameOverhead)}
	many := make([]jframe, pack.MaxChunksPerPack)
	for i := range many {
		many[i] = empty
	}
	p := validPlain(many)
	if got, _, err := decodeJournal(kr, goldenRepo, sealJournalPlain(t, kr, goldenRepo, p.encode())); err != nil || len(got) != 1 || len(got[0].frames) != pack.MaxChunksPerPack {
		t.Fatalf("a record of exactly %d frames decodes to %d records (%v), want it whole", pack.MaxChunksPerPack, len(got), err)
	}
	p = validPlain(frames)
	p.counted = maxCounted
	p.countedList = make([]hash.Hash, maxCounted)
	if got, _, err := decodeJournal(kr, goldenRepo, sealJournalPlain(t, kr, goldenRepo, p.encode())); err != nil || len(got) != 1 || len(got[0].counted) != maxCounted {
		t.Fatalf("a record counting exactly %d chunks decodes to %d records (%v), want it whole", maxCounted, len(got), err)
	}
	big := jframe{h: hash.Sum([]byte("big")), raw: pack.MaxChunkSize, codec: pack.CodecRaw, sealed: make([]byte, maxFrameStored)}
	p = validPlain([]jframe{big})
	if got, _, err := decodeJournal(kr, goldenRepo, sealJournalPlain(t, kr, goldenRepo, p.encode())); err != nil || len(got) != 1 {
		t.Fatalf("a frame of exactly the chunk limit (raw %d, stored %d) decodes to %d records (%v), want it", pack.MaxChunkSize, maxFrameStored, len(got), err)
	}
	// A record of exactly the journal's limit.
	if got, _, err := decodeJournal(kr, goldenRepo, sealJournalPlain(t, kr, goldenRepo, fillTo(t, kr, maxJournal))); err != nil || len(got) != 1 {
		t.Fatalf("a record of exactly %d bytes decodes to %d records (%v), want it", maxJournal, len(got), err)
	}
}

// fillTo is the plaintext of a record exactly n bytes long once sealed:
// frames of raw bytes up to the chunk limit, the last one sized to fit.
func fillTo(t testing.TB, kr *seal.Keyring, n int) []byte {
	t.Helper()
	budget := n - journalHeaderLen - pack.FrameOverhead
	p := validPlain(nil)
	p.counted, p.countedList = 0, nil
	base := len(p.encode())
	var frames []jframe
	used := base
	for used < budget {
		room := budget - used - hash.Size - 1 - 5 - 5 // the frame's hash, codec and two uvarints at most
		size := min(room, maxFrameStored)
		if size < pack.FrameOverhead {
			t.Fatalf("fillTo: cannot fill the last %d bytes", budget-used)
		}
		f := jframe{h: hash.Sum([]byte{byte(len(frames))}), raw: uint32(size - pack.FrameOverhead), codec: pack.CodecRaw, sealed: make([]byte, size)}
		frames = append(frames, f)
		p.frames, p.frameList = uint64(len(frames)), frames
		used = len(p.encode())
	}
	b := p.encode()
	if len(b) != budget {
		// Shorten the last frame by what the uvarints took less than assumed.
		over := len(b) - budget
		last := &frames[len(frames)-1]
		last.sealed = last.sealed[:len(last.sealed)-over]
		last.raw = uint32(len(last.sealed) - pack.FrameOverhead)
		p.frameList = frames
		b = p.encode()
	}
	if len(b) != budget {
		t.Fatalf("fillTo: %d bytes, want %d", len(b), budget)
	}
	return b
}

// A header that claims more than any writer leaves is not a record: it is
// neither opened nor read into memory, and nothing after it replays.
func TestAJournalHeaderOverTheLimitIsNotARecord(t *testing.T) {
	kr := goldenKeyring(t)
	recs := chainedRecords(t, kr, goldenRepo, 1)
	b, ends := sealRecords(t, kr, goldenRepo, recs)
	var h wire.Writer
	h.Raw([]byte(journalMagic))
	h.U16(journalV1)
	h.U32(maxJournal + 1)
	h.Raw(make([]byte, 32))
	j := append(append(append([]byte(nil), b...), h.Bytes()...), make([]byte, 1024)...)
	got, used, err := decodeJournal(kr, goldenRepo, j)
	if err != nil || len(got) != 1 || used != ends[0] {
		t.Fatalf("a header claiming %d bytes after one record: %d records over %d bytes (%v), want the one record", maxJournal+1, len(got), used, err)
	}
}

// Counts are never trusted for an allocation (#24): a record claiming
// 2^40 frames or counted chunks in a few bytes is refused within a small
// budget.
func TestJournalCountsAreNotTrustedForAllocation(t *testing.T) {
	kr := goldenKeyring(t)
	for name, p := range map[string]plainRecord{
		"frames":  {magic: journalPlainMagic, version: journalV1, frames: 1 << 40},
		"counted": {magic: journalPlainMagic, version: journalV1, counted: 1 << 40},
	} {
		b := sealJournalPlain(t, kr, goldenRepo, p.encode())
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		_, _, err := decodeJournal(kr, goldenRepo, b)
		runtime.ReadMemStats(&after)
		if !errors.Is(err, errJournalCorrupt) {
			t.Errorf("%s: a record claiming 2^40 = %v, want errJournalCorrupt", name, err)
		}
		if d := after.TotalAlloc - before.TotalAlloc; d > 256<<10 {
			t.Errorf("%s: decoding a %d-byte record claiming 2^40 allocated %d bytes, want under 256 KiB: a replay could be made to run out of memory", name, len(b), d)
		}
	}
}

// The spec's "no plaintext hash on disk": a sealed record holds no root
// and no chunk hash in the clear.
func TestAJournalRecordHoldsNoPlaintextHash(t *testing.T) {
	kr := goldenKeyring(t)
	recs := chainedRecords(t, kr, goldenRepo, 1)
	b, _ := sealRecords(t, kr, goldenRepo, recs)
	if got, _, err := decodeJournal(kr, goldenRepo, b); err != nil || len(got) != 1 || !sameRecord(got[0], recs[0]) {
		t.Fatalf("positive control: the sealed record decodes to %d records (%v), want it back", len(got), err)
	}
	r := recs[0]
	for _, h := range append([]hash.Hash{r.expected, r.next, r.frames[0].h}, r.counted...) {
		if bytes.Contains(b, h[:]) || bytes.Contains(b, h[:8]) {
			t.Fatalf("a sealed journal record carries %s in the clear", h.Short())
		}
	}
}

func goldenJournal(t testing.TB) []jrecord {
	t.Helper()
	frame := func(s string) jframe {
		data := []byte(s)
		h := hash.Sum(data)
		return jframe{h: h, raw: uint32(len(data)), codec: pack.CodecRaw}
	}
	// The frames' seals are part of the golden bytes; their contents are
	// fixed here and sealed once when the file is written.
	return []jrecord{
		{expected: hash.Sum([]byte("golden base")), next: hash.Sum([]byte("golden 1")), gcGen: 2, salt: seal.Salt{1},
			frames: []jframe{frame("chunk one"), frame("chunk two")}, counted: []hash.Hash{hash.Sum([]byte("counted"))}},
		{expected: hash.Sum([]byte("golden 1")), next: hash.Sum([]byte("golden 2")), gcGen: 2, salt: seal.Salt{1},
			frames: []jframe{frame("chunk three")}},
	}
}

// The v1 journal written once and checked in must decode forever.
func TestAV1JournalStillDecodes(t *testing.T) {
	kr := goldenKeyring(t)
	want := goldenJournal(t)
	path := filepath.Join("testdata", "journal_v1.bin")
	if *update {
		b := writeGoldenJournal(t, kr, want)
		if err := os.WriteFile(path, b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the v1 golden journal: %v", err)
	}
	got, used, err := decodeJournal(kr, goldenRepo, b)
	if err != nil || used != len(b) || len(got) != len(want) {
		t.Fatalf("the v1 golden journal decodes to %d records over %d of %d bytes (%v): journals written by v1 no longer replay", len(got), used, len(b), err)
	}
	for i := range want {
		g, w := got[i], want[i]
		if g.expected != w.expected || g.next != w.next || g.gcGen != w.gcGen || g.salt != w.salt || len(g.frames) != len(w.frames) || len(g.counted) != len(w.counted) {
			t.Fatalf("golden record %d decodes to other fields", i)
		}
		for k := range w.frames {
			if g.frames[k].h != w.frames[k].h || g.frames[k].raw != w.frames[k].raw || g.frames[k].codec != w.frames[k].codec {
				t.Fatalf("golden record %d frame %d decodes to another chunk", i, k)
			}
		}
	}
}

// writeGoldenJournal seals the golden records, their frames sealed for the
// golden pack salt.
func writeGoldenJournal(t testing.TB, kr *seal.Keyring, recs []jrecord) []byte {
	t.Helper()
	keys, err := kr.Key(seal.Chunk, goldenRepo, recs[0].salt)
	if err != nil {
		t.Fatal(err)
	}
	var b []byte
	for i := range recs {
		for k := range recs[i].frames {
			f := &recs[i].frames[k]
			data := map[hash.Hash][]byte{}
			for _, s := range []string{"chunk one", "chunk two", "chunk three"} {
				data[hash.Sum([]byte(s))] = []byte(s)
			}
			if f.sealed, err = keys.Seal(f.h[:], data[f.h]); err != nil {
				t.Fatal(err)
			}
		}
		s, err := recs[i].seal(kr, goldenRepo)
		if err != nil {
			t.Fatal(err)
		}
		b = append(b, s...)
	}
	return b
}

func FuzzDecodeJournal(f *testing.F) {
	kr, err := seal.KeyringFromBytes(bytes.Repeat([]byte{0x5c}, 32))
	if err != nil {
		f.Fatal(err)
	}
	if b, err := os.ReadFile(filepath.Join("testdata", "journal_v1.bin")); err == nil {
		f.Add(b)
		f.Add(b[:len(b)-7])
	}
	f.Add([]byte(journalMagic))
	f.Add(sealJournalPlain(f, kr, goldenRepo, validPlain(nil).encode()))
	f.Fuzz(func(t *testing.T, b []byte) {
		recs, used, err := decodeJournal(kr, goldenRepo, b)
		if err != nil {
			return
		}
		if used > len(b) {
			t.Fatalf("decodeJournal used %d of %d bytes", used, len(b))
		}
		if len(recs) == 0 {
			return
		}
		again, _ := sealRecords(t, kr, goldenRepo, recs)
		back, used2, err := decodeJournal(kr, goldenRepo, again)
		if err != nil || used2 != len(again) || len(back) != len(recs) {
			t.Fatalf("decoded records do not re-seal and decode: %d of %d (%v)", len(back), len(recs), err)
		}
		for i := range recs {
			if !sameRecord(recs[i], back[i]) {
				t.Fatalf("record %d changed through a re-seal", i)
			}
		}
	})
}
