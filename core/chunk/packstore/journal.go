package packstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
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

// openJournal takes the backend's journal, if it keeps one: a journal left
// by a writer that stopped is replayed, with the journal option or
// without it (GC's store opens without it), and with the option the store
// keeps the journal and commits into it. Without the option a journal
// another writer holds is left to it: this store's publishes refuse while
// it is held (holdJournal).
func (s *Store) openJournal(ctx context.Context) error {
	js, ok := s.o.Blobs.(blob.Journaler)
	if !ok {
		return nil
	}
	var j blob.Journal
	var err error
	if s.o.Journal {
		// A writer without the journal holds it only around a publish:
		// wait that out, as a lost manifest swap is waited out.
		for attempt := 0; ; attempt++ {
			j, err = js.OpenJournal(ctx)
			if !errors.Is(err, blob.ErrJournalBusy) || attempt == MaxSwapAttempts-1 {
				break
			}
			if err := s.sleep(ctx, attempt); err != nil {
				return err
			}
		}
		if err != nil {
			return fmt.Errorf("packstore: opening the journal: %w", err)
		}
	} else {
		size, release, err := js.HoldJournal(ctx)
		if errors.Is(err, blob.ErrJournalBusy) {
			return nil // a journal writer is open
		}
		if err != nil {
			return err
		}
		release()
		if size == 0 {
			return nil
		}
		if j, err = js.OpenJournal(ctx); errors.Is(err, blob.ErrJournalBusy) {
			return nil
		} else if err != nil {
			return err
		}
	}
	if err := s.replay(ctx, j); err != nil {
		_ = j.Close()
		return err
	}
	if !s.o.Journal {
		return j.Close()
	}
	s.mu.Lock()
	s.journal = j
	s.jkick, s.jstop, s.jdone = make(chan struct{}, 1), make(chan struct{}), make(chan struct{})
	s.mu.Unlock()
	go s.publisher() //nolint:gosec // G118: the publisher outlives Open's context by design; Close stops it
	return nil
}

// replay publishes what a journal holds, if the backend is not already at
// its last root, and empties it. Callers hold the journal and no one else
// writes: it runs inside Open.
func (s *Store) replay(ctx context.Context, j blob.Journal) error {
	b, err := j.Read(ctx, maxJournal)
	if err != nil {
		return fmt.Errorf("packstore: reading the journal: %w", err)
	}
	if len(b) == 0 {
		return nil
	}
	s.mu.Lock()
	noManifest, root := s.ver == blob.NoVersion, s.man.root
	s.mu.Unlock()
	if noManifest {
		// A repository's first root is published, never journaled, so a
		// journal before it is debris.
		return j.Reset(ctx)
	}
	recs, _, err := decodeJournal(s.o.Keys, s.o.Repo, b)
	if err != nil {
		return err
	}
	switch {
	case len(recs) == 0: // a first record cut short
		return j.Reset(ctx)
	case root == recs[len(recs)-1].next: // published, and not emptied before a crash
		return j.Reset(ctx)
	case root != recs[0].expected:
		return fmt.Errorf("%w: the backend's root %s is neither the one the journal builds on (%s) nor its last (%s)",
			ErrJournalConflict, root.Short(), recs[0].expected.Short(), recs[len(recs)-1].next.Short())
	}
	for _, r := range recs {
		keys, err := pack.DeriveKeys(s.o.Keys, s.o.Repo, r.salt)
		if err != nil {
			return err
		}
		for _, f := range r.frames {
			e := pack.Entry{Hash: f.h, StoredLen: uint32(len(f.sealed)), RawLen: f.raw, Codec: f.codec}
			data, err := pack.OpenFrame(keys, s.codec, e, f.sealed)
			if err != nil {
				return fmt.Errorf("%w: a journaled frame: %w", chunk.ErrCorrupt, err)
			}
			if _, err := s.Put(ctx, data); err != nil {
				return err
			}
		}
	}
	last := recs[len(recs)-1].next
	s.mu.Lock()
	for _, r := range recs {
		for _, h := range r.counted {
			s.deduped[h] = true
		}
	}
	s.sessGen = recs[0].gcGen // the oldest: survived checks them against any collection since
	stored := s.pending != nil && s.pending.Has(last)
	if !stored {
		stored, err = s.hasLocked(last)
	}
	s.mu.Unlock()
	if err != nil {
		return err
	}
	if !stored {
		return fmt.Errorf("%w: the journal's last root %s is a chunk it does not hold", chunk.ErrCorrupt, last.Short())
	}
	if err := s.publish(ctx, recs[0].expected, last); err != nil {
		return fmt.Errorf("packstore: publishing the journal: %w", err)
	}
	return j.Reset(ctx)
}

// holdJournal holds the backend's journal, if it keeps one, around a
// publish by a store without it: ErrJournalBusy while a journal writer is
// open, or while a journal left by one that stopped waits to be replayed.
func (s *Store) holdJournal(ctx context.Context) (func(), error) {
	js, ok := s.o.Blobs.(blob.Journaler)
	if !ok {
		return func() {}, nil
	}
	size, release, err := js.HoldJournal(ctx)
	if err != nil {
		return nil, fmt.Errorf("packstore: a writer with the journal is open: %w", err)
	}
	if size > 0 {
		release()
		return nil, fmt.Errorf("%w: a journal left by a writer that stopped holds %d bytes; open the repository again to replay it",
			blob.ErrJournalBusy, size)
	}
	return release, nil
}

// journalCommit is CompareAndSetRoot with the journal: one append and one
// fsync, unless the commit cannot journal, when it publishes, journal and
// all. Callers hold commitMu and have checked next is stored.
func (s *Store) journalCommit(ctx context.Context, expected, next hash.Hash) error {
	// The backend's root: GC may have moved the manifest; only a writer
	// that does not honor the journal moves the root itself.
	r, err := s.o.Blobs.Root(ctx)
	if err != nil {
		return err
	}
	s.mu.Lock()
	moved := r.Version != s.ver
	s.mu.Unlock()
	if moved {
		if err := s.refresh(ctx); err != nil {
			return err
		}
	}
	s.mu.Lock()
	m, first, n, cur, base := s.man, s.ver == blob.NoVersion, s.jrecords, s.man.root, s.jbase
	if n > 0 {
		cur = s.jroot
	}
	s.mu.Unlock()
	if n > 0 && m.root != base {
		return s.journalFailed(fmt.Errorf("%w: the backend's root is %s, the journal builds on %s", ErrJournalConflict, m.root.Short(), base.Short()))
	}
	if cur != expected {
		return chunk.ErrRootConflict
	}
	if first {
		return s.publishJournal(ctx, next) // a repository's first root is published
	}
	if err := s.survived(m); err != nil {
		return err
	}
	s.finishers.Wait()
	s.mu.Lock()
	if s.finishErr != nil {
		err := s.finishErr
		s.mu.Unlock()
		return err
	}
	// A pack that went to the backend since the last publish holds chunks
	// no record carries, and only an index object can name it: publish.
	must := len(s.session) > 0 || len(s.unuploaded) > 0 || len(s.sessionIdx) > 0 ||
		len(s.finishing) > 0 || len(s.inflight) > 0 || len(s.jcounted) > maxCounted
	w, mark := s.pending, 0
	rec := jrecord{expected: expected, next: next, gcGen: s.sessGen}
	if !must {
		rec.counted = append([]hash.Hash(nil), s.jcounted...)
		if w != nil {
			from := 0
			if w == s.jpending {
				from = s.jmark
			}
			for _, f := range w.FramesSince(from) {
				rec.frames = append(rec.frames, jframe{h: f.Hash, raw: f.RawLen, codec: f.Codec, sealed: f.Sealed})
			}
			rec.salt, mark = w.Salt(), w.Count()
		}
	}
	s.mu.Unlock()
	if !must {
		b, err := rec.seal(s.o.Keys, s.o.Repo)
		if err == nil && s.jbytes+len(b) <= maxJournal {
			if err := s.journal.Append(ctx, b); err != nil {
				return s.journalFailed(fmt.Errorf("packstore: appending to the journal (open the repository again to replay what it holds): %w", err))
			}
			s.mu.Lock()
			s.jcounted = s.jcounted[len(rec.counted):]
			s.jpending, s.jmark = w, mark
			if s.jrecords == 0 {
				s.jbase = m.root
				select {
				case s.jkick <- struct{}{}:
				default:
				}
			}
			s.jrecords++
			s.jbytes += len(b)
			s.jroot = next
			s.mu.Unlock()
			return nil
		}
		// Too large to journal (or it would pass the journal's limit):
		// publish instead.
	}
	return s.publishJournal(ctx, next)
}

// publishJournal publishes everything the store holds with root next, and
// empties the journal. The journal's commits build on the root it was
// started over: a root that moved since is ErrJournalConflict. Callers
// hold commitMu.
func (s *Store) publishJournal(ctx context.Context, next hash.Hash) error {
	s.mu.Lock()
	n, bytes, expected := s.jrecords, s.jbytes, s.man.root
	if n > 0 {
		expected = s.jbase
	}
	s.mu.Unlock()
	err := s.publish(ctx, expected, next)
	if errors.Is(err, chunk.ErrRootConflict) && n > 0 {
		return s.journalFailed(fmt.Errorf("%w: publishing it", ErrJournalConflict))
	}
	if err != nil {
		return err
	}
	if bytes > 0 {
		if err := s.journal.Reset(ctx); err != nil {
			return s.journalFailed(fmt.Errorf("packstore: the journal was published and could not be emptied (open the repository again): %w", err))
		}
	}
	s.mu.Lock()
	s.jrecords, s.jbytes, s.jroot, s.jbase, s.jpending, s.jmark = 0, 0, hash.Hash{}, hash.Hash{}, nil, 0
	s.mu.Unlock()
	return nil
}

// journalFailed makes err the store's: the journal cannot be trusted with
// another commit, so every write refuses and nothing publishes over it.
// Callers hold commitMu.
func (s *Store) journalFailed(err error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.jerr == nil {
		s.jerr = err
	}
	return s.jerr
}

// publisher publishes the journal JournalInterval after the first commit
// that lands in it, until the store closes.
func (s *Store) publisher() {
	defer close(s.jdone)
	for {
		select {
		case <-s.jstop:
			return
		case <-s.jkick:
		}
		t := time.NewTimer(s.o.JournalInterval)
		select {
		case <-s.jstop:
			t.Stop()
			return
		case <-t.C:
		}
		if s.holdPublish != nil {
			s.holdPublish()
		}
		_ = s.publishNow(context.Background())
	}
}

// publishNow publishes what the journal holds. A failure the store can
// retry is retried an interval later; one it cannot is the store's.
func (s *Store) publishNow(ctx context.Context) error {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	s.mu.Lock()
	n, jerr, root, open := s.jrecords, s.jerr, s.jroot, s.journal != nil && !s.closed
	s.mu.Unlock()
	if jerr != nil {
		return jerr
	}
	if !open || n == 0 {
		return nil
	}
	err := s.publishJournal(ctx, root)
	s.mu.Lock()
	retry := err != nil && s.jerr == nil && s.lost == nil
	s.mu.Unlock()
	if retry {
		select {
		case s.jkick <- struct{}{}:
		default:
		}
	}
	return err
}

// closeJournal stops the publisher and, with publish, lands what the
// journal holds; then it releases the journal. Without publish it is a
// process dying: nothing is written.
func (s *Store) closeJournal(publish bool) error {
	s.mu.Lock()
	j := s.journal
	s.mu.Unlock()
	if j == nil {
		return nil
	}
	s.jstopOnce.Do(func() { close(s.jstop) })
	<-s.jdone
	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	var err error
	s.mu.Lock()
	n, root, jerr := s.jrecords, s.jroot, s.jerr
	s.mu.Unlock()
	if publish && n > 0 && jerr == nil {
		err = s.publishJournal(context.Background(), root)
	}
	s.mu.Lock()
	s.journal = nil
	s.jrecords = 0
	s.mu.Unlock()
	return errors.Join(err, j.Close())
}
