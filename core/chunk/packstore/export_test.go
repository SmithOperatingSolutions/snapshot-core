package packstore

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/dedup"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
)

// WithBackoff shortens the retry backoff for tests.
func WithBackoff(o Options, d time.Duration) Options {
	o.backoff = d
	return o
}

// CorruptCached flips a byte of a chunk's cached copy, as a memory fault
// would; it reports whether the chunk was cached.
func CorruptCached(s *Store, h hash.Hash) bool {
	if s.cache == nil {
		return false
	}
	s.cache.mu.Lock()
	defer s.cache.mu.Unlock()
	e, ok := s.cache.items[h]
	if !ok {
		return false
	}
	d := e.Value.(*cached).data
	if len(d) == 0 {
		return false
	}
	d[len(d)/2] ^= 1
	return true
}

// Recorded lists, in order, the objects the store's manifest records as
// orphans GC deleted.
func Recorded(ctx context.Context, o Options) ([]string, error) {
	r, err := o.Blobs.Root(ctx)
	if err != nil || len(r.Value) == 0 {
		return nil, err
	}
	m, err := openManifest(r.Value, o.Keys, o.Repo)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, c := range m.condemned {
		switch c.kind {
		case deletedPack:
			out = append(out, dedup.PackName(c.sum))
		case deletedIndex:
			out = append(out, indexName(c.sum))
		}
	}
	sort.Strings(out)
	return out, nil
}

// RepackedNames lists, in order, the packs the store's manifest records as
// repacked.
func RepackedNames(ctx context.Context, o Options) ([]string, error) {
	r, err := o.Blobs.Root(ctx)
	if err != nil || len(r.Value) == 0 {
		return nil, err
	}
	m, err := openManifest(r.Value, o.Keys, o.Repo)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, c := range m.condemned {
		if c.kind == repackedPack {
			out = append(out, dedup.PackName(c.sum))
		}
	}
	sort.Strings(out)
	return out, nil
}

// PackOrder lists the packs the store's manifest indexes, in the order its
// index objects list them.
func PackOrder(ctx context.Context, o Options) ([]string, error) {
	r, err := o.Blobs.Root(ctx)
	if err != nil {
		return nil, err
	}
	m, err := openManifest(r.Value, o.Keys, o.Repo)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, sum := range m.indexes {
		infos, err := loadIndex(ctx, o, sum)
		if err != nil {
			return nil, err
		}
		for _, p := range infos {
			out = append(out, p.Name)
		}
	}
	return out, nil
}

// PackEntries lists the chunks the manifest's index objects say pack name
// holds, in the pack's order.
func PackEntries(ctx context.Context, o Options, name string) ([]hash.Hash, error) {
	r, err := o.Blobs.Root(ctx)
	if err != nil {
		return nil, err
	}
	m, err := openManifest(r.Value, o.Keys, o.Repo)
	if err != nil {
		return nil, err
	}
	for _, sum := range m.indexes {
		infos, err := loadIndex(ctx, o, sum)
		if err != nil {
			return nil, err
		}
		for _, p := range infos {
			if p.Name != name {
				continue
			}
			var out []hash.Hash
			for _, e := range p.Entries {
				out = append(out, e.Hash)
			}
			return out, nil
		}
	}
	return nil, fmt.Errorf("no index object lists %s", name)
}

// ForgedSpilled is a spilled index over a table whose records were written
// by hand, to prove the record decoder refuses what it should.
func ForgedSpilled(t *dedup.Table, packs int) interface {
	Lookup(h hash.Hash) (dedup.Location, bool, error)
} {
	sp := &spilled{table: t}
	for range packs {
		sp.packs = append(sp.packs, packRef{})
	}
	return forged{sp}
}

type forged struct{ sp *spilled }

func (f forged) Lookup(h hash.Hash) (dedup.Location, bool, error) { return f.sp.lookup(h) }

// JoinForged runs a round's join over a forged index table with the given
// number of packs, against a live set of every key in it.
func JoinForged(t *dedup.Table, packs int, live Live) error {
	r := &Round{index: &spilled{table: t}}
	for range packs {
		r.packs = append(r.packs, packSummary{})
	}
	_, _, err := r.join(live)
	return err
}

// WaitUploads waits for every pack a finisher is naming and uploading, so a
// test can count on what filled being in the backend.
func WaitUploads(s *Store) { s.finishers.Wait() }

// HoldFinish makes every finisher call f before it names and builds its
// pack, so a test can hold a pack at that point.
func HoldFinish(s *Store, f func()) { s.holdFinish = f }

// AfterWait makes a publish call f once it has waited for the finishers.
func AfterWait(s *Store, f func()) { s.afterWait = f }

// ManifestOpens is how many manifests the store's refreshes have opened.
func ManifestOpens(s *Store) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.opens
}

// IndexObjects is how many index objects the store's manifest lists.
func IndexObjects(ctx context.Context, o Options) (int, error) {
	r, err := o.Blobs.Root(ctx)
	if err != nil {
		return 0, err
	}
	m, err := openManifest(r.Value, o.Keys, o.Repo)
	if err != nil {
		return 0, err
	}
	return len(m.indexes), nil
}

// Abandon drops the store as a process that died would: the journal is
// released without a publish, and nothing is written.
func Abandon(s *Store) {
	_ = s.closeJournal(false)
	_ = s.Close()
}

// HoldPublish makes the background publisher call f before it publishes.
func HoldPublish(s *Store, f func()) { s.holdPublish = f }

// PublishJournal runs the background publish now.
func PublishJournal(s *Store) error { return s.publishNow(context.Background()) }

// JournalRecords is how many commits the store's journal holds.
func JournalRecords(s *Store) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.jrecords
}

// PublishedRoot is the root the backend's manifest names: what another
// process sees.
func PublishedRoot(ctx context.Context, o Options) (hash.Hash, error) {
	r, err := o.Blobs.Root(ctx)
	if err != nil || len(r.Value) == 0 {
		return hash.Hash{}, err
	}
	m, err := openManifest(r.Value, o.Keys, o.Repo)
	return m.root, err
}

// ManifestSeq is the backend manifest's sequence number.
func ManifestSeq(ctx context.Context, o Options) (uint64, error) {
	r, err := o.Blobs.Root(ctx)
	if err != nil || len(r.Value) == 0 {
		return 0, err
	}
	m, err := openManifest(r.Value, o.Keys, o.Repo)
	return m.seq, err
}

// ForceRoot swaps the backend's manifest to one naming root, as a writer
// that does not honor the journal (v0.2.0) would.
func ForceRoot(ctx context.Context, o Options, root hash.Hash) error {
	r, err := o.Blobs.Root(ctx)
	if err != nil {
		return err
	}
	m, err := openManifest(r.Value, o.Keys, o.Repo)
	if err != nil {
		return err
	}
	m.seq++
	m.root = root
	b, err := m.seal(o.Keys, o.Repo)
	if err != nil {
		return err
	}
	_, err = o.Blobs.SwapRoot(ctx, r.Version, b)
	return err
}

// ForgeJournalFrame rewrites the journal's last record so its first frame
// is listed under another chunk's hash, sealed under the right key: only
// the replay's reading of the frame can refuse it.
func ForgeJournalFrame(ctx context.Context, bs blob.Journaler, kr *seal.Keyring, repo seal.RepoID) error {
	j, err := bs.OpenJournal(ctx)
	if err != nil {
		return err
	}
	defer j.Close()
	b, err := j.Read(ctx, maxJournal)
	if err != nil {
		return err
	}
	recs, _, err := decodeJournal(kr, repo, b)
	if err != nil {
		return err
	}
	if len(recs) == 0 || len(recs[len(recs)-1].frames) == 0 {
		return fmt.Errorf("no frame to forge in %d records", len(recs))
	}
	recs[len(recs)-1].frames[0].h = hash.Sum([]byte("another chunk"))
	if err := j.Reset(ctx); err != nil {
		return err
	}
	for i := range recs {
		sealed, err := recs[i].seal(kr, repo)
		if err != nil {
			return err
		}
		if err := j.Append(ctx, sealed); err != nil {
			return err
		}
	}
	return nil
}

// ForgeJournalRoot rewrites the journal's last record so it sets a root no
// record carries and the store never held, sealed under the right key.
func ForgeJournalRoot(ctx context.Context, bs blob.Journaler, kr *seal.Keyring, repo seal.RepoID) error {
	j, err := bs.OpenJournal(ctx)
	if err != nil {
		return err
	}
	defer j.Close()
	b, err := j.Read(ctx, maxJournal)
	if err != nil {
		return err
	}
	recs, _, err := decodeJournal(kr, repo, b)
	if err != nil || len(recs) == 0 {
		return fmt.Errorf("%d records: %w", len(recs), err)
	}
	recs[len(recs)-1].next = hash.Sum([]byte("a root nobody stored"))
	if err := j.Reset(ctx); err != nil {
		return err
	}
	for i := range recs {
		sealed, err := recs[i].seal(kr, repo)
		if err != nil {
			return err
		}
		if err := j.Append(ctx, sealed); err != nil {
			return err
		}
	}
	return nil
}
