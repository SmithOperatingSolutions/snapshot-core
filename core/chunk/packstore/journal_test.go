package packstore_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/mem"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/packstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
)

// The commit journal (#34, DESIGN §6): a commit appends its new chunks and
// its root to the backend's journal and returns durable; a background
// publish lands it for other processes; a journal left by a writer that
// died is replayed by the next open.

func journalOptions(bs blob.BlobStore, kr *seal.Keyring, interval time.Duration) packstore.Options {
	return packstore.WithBackoff(packstore.Options{Blobs: bs, Keys: kr, Repo: repo, Journal: packstore.JournalOn, JournalInterval: interval}, time.Millisecond)
}

func openJournaled(t testing.TB, bs blob.BlobStore, kr *seal.Keyring, interval time.Duration) *packstore.Store {
	t.Helper()
	s, err := packstore.Open(ctx, journalOptions(bs, kr, interval))
	if err != nil {
		t.Fatalf("Open with the journal: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// commit puts data and sets it as the root over expected.
func commit(t testing.TB, s *packstore.Store, expected hash.Hash, data []byte) hash.Hash {
	t.Helper()
	h, err := s.Put(ctx, data)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.CompareAndSetRoot(ctx, expected, h); err != nil {
		t.Fatalf("CompareAndSetRoot: %v", err)
	}
	return h
}

func packCount(t testing.TB, bs blob.BlobStore) int {
	t.Helper()
	infos, err := bs.List(ctx, "packs/", "", blob.MaxListPage)
	if err != nil {
		t.Fatal(err)
	}
	return len(infos)
}

func publishedRoot(t testing.TB, bs blob.BlobStore, kr *seal.Keyring) hash.Hash {
	t.Helper()
	r, err := packstore.PublishedRoot(ctx, packstore.Options{Blobs: bs, Keys: kr, Repo: repo})
	if err != nil {
		t.Fatalf("reading the published root: %v", err)
	}
	return r
}

// A journaled commit returns before anything is published: the store
// reads its root and chunks at once, another process sees the old root,
// and a crash keeps the commit.
func TestAJournaledCommitIsDurableBeforeItIsPublished(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	s := openJournaled(t, bs, kr, time.Hour)
	if !s.Journaled() {
		t.Fatal("a store opened with the journal on a backend that keeps one does not journal")
	}
	first := commit(t, s, hash.Hash{}, payload("first", 3000)) // a repository's first root is published
	packsBefore := packCount(t, bs)
	data := payload("journaled", 5000)
	h := commit(t, s, first, data)

	if got, err := s.Root(ctx); err != nil || got != h {
		t.Fatalf("the committing store's root is %s (%v), want the commit %s: its own readers would not see it", got.Short(), err, h.Short())
	}
	if got, err := s.Get(ctx, h); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("the committing store cannot read the chunk it just committed (%v)", err)
	}
	if got := publishedRoot(t, bs, kr); got != first {
		t.Fatalf("the backend's root is %s right after a journaled commit, want the one before, %s: the commit published, so the journal saved nothing", got.Short(), first.Short())
	}
	if n := packCount(t, bs); n != packsBefore {
		t.Fatalf("a journaled commit wrote %d packs, want none until the background publish", n-packsBefore)
	}
	if n := packstore.JournalRecords(s); n != 1 {
		t.Fatalf("the journal holds %d commits after one, want 1", n)
	}
	other := open(t, bs, kr)
	if got, err := other.Root(ctx); err != nil || got != first {
		t.Fatalf("another process sees root %s (%v), want the published %s until the background publish", got.Short(), err, first.Short())
	}

	packstore.Abandon(s) // kill -9
	re := openJournaled(t, bs, kr, time.Hour)
	if got, err := re.Root(ctx); err != nil || got != h {
		t.Fatalf("after a crash the root is %s (%v), want the journaled commit %s: a commit told it was durable is gone", got.Short(), err, h.Short())
	}
	if got, err := re.Get(ctx, h); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("after a crash the journaled commit's chunk does not read (%v)", err)
	}
	if got := publishedRoot(t, bs, kr); got != h {
		t.Fatalf("the replay left the backend's root at %s, want the replayed %s published", got.Short(), h.Short())
	}
	if n := packstore.JournalRecords(re); n != 0 {
		t.Fatalf("after the replay published it, the journal holds %d commits, want none", n)
	}
}

// The background publish lands a journaled commit for other processes
// within the interval, and empties the journal. The publisher is held at
// its start, so the commit is seen journaled first whatever the scheduler.
func TestTheBackgroundPublishLandsWithinTheInterval(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	s := openJournaled(t, bs, kr, 20*time.Millisecond)
	first := commit(t, s, hash.Hash{}, payload("first", 100))
	gate := make(chan struct{})
	packstore.HoldPublish(s, func() { <-gate })
	h := commit(t, s, first, payload("second", 100))
	if got := publishedRoot(t, bs, kr); got != first {
		t.Fatalf("with the publisher held, the backend's root is %s, want %s: the commit published instead of journaling", got.Short(), first.Short())
	}
	close(gate)
	deadline := time.Now().Add(10 * time.Second)
	for publishedRoot(t, bs, kr) != h {
		if time.Now().After(deadline) {
			t.Fatalf("10s after a journaled commit with a 20ms interval, the backend's root is still %s: the background publish never ran",
				publishedRoot(t, bs, kr).Short())
		}
		time.Sleep(5 * time.Millisecond)
	}
	fresh := open(t, bs, kr)
	if _, err := fresh.Get(ctx, h); err != nil {
		t.Fatalf("another process cannot read the published commit's chunk: %v", err)
	}
	deadline = time.Now().Add(10 * time.Second)
	for packstore.JournalRecords(s) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the journal still holds %d commits after they were published", packstore.JournalRecords(s))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Close lands what the journal holds.
func TestCloseLandsTheJournal(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	s, err := packstore.Open(ctx, journalOptions(bs, kr, time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	first := commit(t, s, hash.Hash{}, payload("first", 100))
	h := commit(t, s, first, payload("closing", 100))
	if got := publishedRoot(t, bs, kr); got != first {
		t.Fatalf("positive control: the commit published at once (root %s)", got.Short())
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := publishedRoot(t, bs, kr); got != h {
		t.Fatalf("after Close the backend's root is %s, want the journaled %s", got.Short(), h.Short())
	}
	j, err := bs.OpenJournal(ctx)
	if err != nil {
		t.Fatalf("the journal is still held after Close: %v", err)
	}
	defer j.Close()
	if b, err := j.Read(ctx, 1<<20); err != nil || len(b) != 0 {
		t.Fatalf("after Close published it, the journal holds %d bytes (%v), want none", len(b), err)
	}
}

// Many journaled commits replay in order, and only the last root is
// published, with every chunk they reached.
func TestManyJournaledCommitsReplay(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	s := openJournaled(t, bs, kr, time.Hour)
	root := commit(t, s, hash.Hash{}, payload("base", 100))
	var datas [][]byte
	var roots []hash.Hash
	for i := range 20 {
		d := payload(fmt.Sprintf("commit-%d", i), 200+i*97)
		root = commit(t, s, root, d)
		datas, roots = append(datas, d), append(roots, root)
	}
	if n := packstore.JournalRecords(s); n != 20 {
		t.Fatalf("the journal holds %d commits after 20", n)
	}
	packstore.Abandon(s)
	re := openJournaled(t, bs, kr, time.Hour)
	if got, _ := re.Root(ctx); got != root {
		t.Fatalf("after a crash the root is %s, want the last of 20 journaled commits %s", got.Short(), root.Short())
	}
	fresh := open(t, bs, kr)
	for i, h := range roots {
		if got, err := fresh.Get(ctx, h); err != nil || !bytes.Equal(got, datas[i]) {
			t.Fatalf("after the replay another process cannot read commit %d's chunk (%v): the replay published a root without its chunks", i, err)
		}
	}
}

// A second writer with the journal is refused; the first keeps it.
func TestASecondJournalWriterIsRefused(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	_ = openJournaled(t, bs, kr, time.Hour)
	if s2, err := packstore.Open(ctx, journalOptions(bs, kr, time.Hour)); !errors.Is(err, blob.ErrJournalBusy) {
		if err == nil {
			_ = s2.Close()
		}
		t.Fatalf("a second Open with the journal = %v, want ErrJournalBusy: two writers would append to one journal", err)
	}
}

// A writer without the journal refuses to publish while a journal writer
// holds it: its swap would move the root under journaled commits.
func TestAWriterWithoutTheJournalIsRefusedWhileOneIsOpen(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	js, err := packstore.Open(ctx, journalOptions(bs, kr, time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	first := commit(t, js, hash.Hash{}, payload("first", 100))
	plain := open(t, bs, kr)
	h, err := plain.Put(ctx, payload("plain", 100))
	if err != nil {
		t.Fatal(err)
	}
	if err := plain.CompareAndSetRoot(ctx, first, h); !errors.Is(err, blob.ErrJournalBusy) {
		t.Fatalf("a publish without the journal while a journal writer is open = %v, want ErrJournalBusy", err)
	}
	if err := js.Close(); err != nil {
		t.Fatal(err)
	}
	if err := plain.CompareAndSetRoot(ctx, first, h); err != nil {
		t.Fatalf("once the journal writer closed, the publish = %v, want it to land", err)
	}
}

// Any open replays a journal left by a writer that died, with or without
// the journal itself, and GC's (which opens without it) does too.
func TestAJournalLeftByADeadWriterIsReplayedByAnyOpen(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	s := openJournaled(t, bs, kr, time.Hour)
	first := commit(t, s, hash.Hash{}, payload("first", 100))
	data := payload("left behind", 4000)
	h := commit(t, s, first, data)
	packstore.Abandon(s)
	if got := publishedRoot(t, bs, kr); got != first {
		t.Fatalf("positive control: the commit was published before the crash (root %s)", got.Short())
	}
	plain := open(t, bs, kr)
	if got, err := plain.Root(ctx); err != nil || got != h {
		t.Fatalf("an open without the journal sees root %s (%v), want the dead writer's journaled %s replayed", got.Short(), err, h.Short())
	}
	if got := publishedRoot(t, bs, kr); got != h {
		t.Fatalf("the replay left the backend's root at %s, want %s", got.Short(), h.Short())
	}
	h2 := commit(t, plain, h, payload("after", 100))
	if got := publishedRoot(t, bs, kr); got != h2 {
		t.Fatalf("a publish after the replay did not land (root %s)", got.Short())
	}
}

// A root moved under a journal that holds commits, by a writer that does
// not honor it, makes those commits unpublishable: the store says so and
// refuses writes, and never publishes over it. With nothing journaled, a
// moved root is an ordinary conflict.
func TestARootMovedUnderAJournalIsRefused(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	s := openJournaled(t, bs, kr, time.Hour)
	o := packstore.Options{Blobs: bs, Keys: kr, Repo: repo}
	first := commit(t, s, hash.Hash{}, payload("first", 100))
	intruder := commit(t, open(t, mem.New(), kr), hash.Hash{}, payload("v0.2.0's", 100)) // a hash no one here stored

	// Nothing journaled: an ordinary conflict, and the store goes on.
	if err := packstore.ForceRoot(ctx, o, intruder); err != nil {
		t.Fatal(err)
	}
	h, err := s.Put(ctx, payload("second", 100))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CompareAndSetRoot(ctx, first, h); !errors.Is(err, chunk.ErrRootConflict) {
		t.Fatalf("with nothing journaled, a commit over a root that moved = %v, want ErrRootConflict", err)
	}
	if err := packstore.ForceRoot(ctx, o, first); err != nil {
		t.Fatal(err)
	}
	if err := s.CompareAndSetRoot(ctx, first, h); err != nil {
		t.Fatalf("positive control: the commit over the root it expects: %v", err)
	}

	// A journaled commit, then the root moves under it.
	if err := packstore.ForceRoot(ctx, o, intruder); err != nil {
		t.Fatal(err)
	}
	h3, err := s.Put(ctx, payload("third", 100))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CompareAndSetRoot(ctx, h, h3); !errors.Is(err, packstore.ErrJournalConflict) {
		t.Fatalf("a commit after the root moved under the journal = %v, want ErrJournalConflict", err)
	}
	if err := packstore.PublishJournal(s); !errors.Is(err, packstore.ErrJournalConflict) {
		t.Fatalf("the background publish over a moved root = %v, want ErrJournalConflict", err)
	}
	if got := publishedRoot(t, bs, kr); got != intruder {
		t.Fatalf("the backend's root is %s, want the intruder's %s left alone: the store published over it", got.Short(), intruder.Short())
	}
	if _, err := s.Put(ctx, payload("fourth", 100)); !errors.Is(err, packstore.ErrJournalConflict) {
		t.Fatalf("a put after the conflict = %v, want ErrJournalConflict", err)
	}
	packstore.Abandon(s)
	if s2, err := packstore.Open(ctx, journalOptions(bs, kr, time.Hour)); !errors.Is(err, packstore.ErrJournalConflict) {
		if err == nil {
			_ = s2.Close()
		}
		t.Fatalf("reopening over a journal whose root was moved = %v, want ErrJournalConflict and the journal kept", err)
	}
}

// resetFails is a store whose journal's Reset fails once armed: a crash
// between a publish and the journal's reset.
type resetFails struct {
	*mem.Store
	armed *bool
}

func (r resetFails) OpenJournal(ctx context.Context) (blob.Journal, error) {
	j, err := r.Store.OpenJournal(ctx)
	if err != nil {
		return nil, err
	}
	return failingReset{j, r.armed}, nil
}

type failingReset struct {
	blob.Journal
	armed *bool
}

func (f failingReset) Reset(ctx context.Context) error {
	if *f.armed {
		return errors.New("disk gone")
	}
	return f.Journal.Reset(ctx)
}

// A journal published and not reset (a crash between the swap and the
// reset) is not published twice: the next open finds the backend at its
// last root and empties it.
func TestAPublishedJournalLeftUnresetIsNotReplayedTwice(t *testing.T) {
	armed := false
	raw := mem.New()
	bs := resetFails{raw, &armed}
	kr := keyring(t)
	s := openJournaled(t, bs, kr, time.Hour)
	first := commit(t, s, hash.Hash{}, payload("first", 100))
	h := commit(t, s, first, payload("second", 100))
	armed = true
	if err := packstore.PublishJournal(s); err == nil {
		t.Fatal("the publish whose reset failed reported success")
	}
	if got := publishedRoot(t, bs, kr); got != h {
		t.Fatalf("positive control: the swap landed before the reset failed (root %s, want %s)", got.Short(), h.Short())
	}
	if _, err := s.Put(ctx, payload("after", 10)); err == nil {
		t.Fatal("a put after the journal could not be reset succeeded: a later append would follow records already published")
	}
	packstore.Abandon(s)
	armed = false
	o := packstore.Options{Blobs: bs, Keys: kr, Repo: repo}
	seq, err := packstore.ManifestSeq(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	re := openJournaled(t, bs, kr, time.Hour)
	if got, _ := re.Root(ctx); got != h {
		t.Fatalf("reopened, the root is %s, want %s", got.Short(), h.Short())
	}
	if after, _ := packstore.ManifestSeq(ctx, o); after != seq {
		t.Fatalf("reopening swapped the manifest %d times: a journal already published was published again", after-seq)
	}
	if n := packstore.JournalRecords(re); n != 0 {
		t.Fatalf("reopened, the journal holds %d commits, want it emptied", n)
	}
}

// A commit that cannot journal (a full pack went to the backend since the
// last publish) publishes, journal and all.
func TestACommitAfterAFullPackPublishes(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	o := journalOptions(bs, kr, time.Hour)
	o.PackSize = 64 << 10
	s, err := packstore.Open(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	first := commit(t, s, hash.Hash{}, payload("first", 100))
	h := commit(t, s, first, payload("journaled", 100))
	if got := publishedRoot(t, bs, kr); got != first {
		t.Fatalf("positive control: a small commit published (root %s)", got.Short())
	}
	for i := range 8 { // past a pack
		if _, err := s.Put(ctx, payload(fmt.Sprintf("big-%d", i), 20000)); err != nil {
			t.Fatal(err)
		}
	}
	packstore.WaitUploads(s)
	h2 := commit(t, s, h, payload("after the full pack", 100))
	if got := publishedRoot(t, bs, kr); got != h2 {
		t.Fatalf("a commit after a full pack left the backend at %s, want it published at %s: a replay would not find the full pack's chunks", got.Short(), h2.Short())
	}
	if n := packstore.JournalRecords(s); n != 0 {
		t.Fatalf("after publishing, the journal holds %d commits", n)
	}
}

// The frames a replay reads are verified like every other read: a record
// whose frame does not open to its chunk is refused, not skipped.
func TestAReplayVerifiesItsFrames(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	s := openJournaled(t, bs, kr, time.Hour)
	first := commit(t, s, hash.Hash{}, payload("first", 100))
	if _, err := s.Put(ctx, payload("not the root", 100)); err != nil { // the frame forged: not the root, which the replay checks apart
		t.Fatal(err)
	}
	_ = commit(t, s, first, payload("second", 100))
	packstore.Abandon(s)
	if err := packstore.ForgeJournalFrame(ctx, bs, kr, repo); err != nil {
		t.Fatal(err)
	}
	if s2, err := packstore.Open(ctx, journalOptions(bs, kr, time.Hour)); !errors.Is(err, chunk.ErrCorrupt) {
		if err == nil {
			_ = s2.Close()
		}
		t.Fatalf("replaying a journal whose frame names another chunk = %v, want ErrCorrupt", err)
	}
}

// A replayed journal equals the published state (#34's acceptance): over
// random runs of commits, background publishes and crashes, a journaled
// commit never moves the backend's root, a publish lands exactly the last
// acknowledged root, and after every crash the reopened store, and
// another process reading the backend, stand at the last acknowledged
// root with every committed chunk readable.
func TestAReplayedJournalEqualsThePublishedState(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		bs, kr := mem.New(), keyring(t)
		s, err := packstore.Open(ctx, journalOptions(bs, kr, time.Hour))
		if err != nil {
			rt.Fatal(err)
		}
		defer func() { _ = s.Close() }()
		datas := map[hash.Hash][]byte{}
		n := 0
		// Every root is a chunk of its own, so no root repeats; the other
		// chunks of a commit are fresh, or one put before again (counted on,
		// not stored).
		var order []hash.Hash
		put := func() hash.Hash {
			n++
			d := payload(fmt.Sprintf("prop-%d", n), rapid.IntRange(1, 3000).Draw(rt, "size"))
			h, err := s.Put(ctx, d)
			if err != nil {
				rt.Fatalf("Put: %v", err)
			}
			datas[h] = d
			order = append(order, h)
			return h
		}
		reput := func() {
			h := order[rapid.IntRange(0, len(order)-1).Draw(rt, "again")]
			if _, err := s.Put(ctx, datas[h]); err != nil {
				rt.Fatalf("Put again: %v", err)
			}
		}
		acked := put()
		if err := s.CompareAndSetRoot(ctx, hash.Hash{}, acked); err != nil {
			rt.Fatal(err)
		}
		published := acked
		readAll := func(label string, r chunk.Reader) {
			for h, d := range datas {
				if got, err := r.Get(ctx, h); err != nil || !bytes.Equal(got, d) {
					rt.Fatalf("%s: committed chunk %s does not read back (%v)", label, h.Short(), err)
				}
			}
		}
		steps := rapid.IntRange(1, 25).Draw(rt, "steps")
		for i := 0; i < steps; i++ {
			switch rapid.IntRange(0, 3).Draw(rt, "op") {
			case 0, 1: // commit a few chunks, the last one the root
				for range rapid.IntRange(0, 3).Draw(rt, "chunks") {
					if rapid.Bool().Draw(rt, "again") {
						reput()
					} else {
						put()
					}
				}
				last := put()
				if err := s.CompareAndSetRoot(ctx, acked, last); err != nil {
					rt.Fatalf("step %d: commit: %v", i, err)
				}
				acked = last
				if got := publishedRoot(t, bs, kr); got != published {
					rt.Fatalf("step %d: a journaled commit moved the backend's root to %s (want %s until a publish)", i, got.Short(), published.Short())
				}
			case 2: // the background publish
				if err := packstore.PublishJournal(s); err != nil {
					rt.Fatalf("step %d: publish: %v", i, err)
				}
				published = acked
				if got := publishedRoot(t, bs, kr); got != acked {
					rt.Fatalf("step %d: the publish left the backend's root at %s, want the last acknowledged %s", i, got.Short(), acked.Short())
				}
			case 3: // kill -9, and open again
				packstore.Abandon(s)
				if s, err = packstore.Open(ctx, journalOptions(bs, kr, time.Hour)); err != nil {
					rt.Fatalf("step %d: reopening after a crash: %v", i, err)
				}
				published = acked
				if got, err := s.Root(ctx); err != nil || got != acked {
					rt.Fatalf("step %d: after a crash the root is %s (%v), want the last acknowledged %s", i, got.Short(), err, acked.Short())
				}
				if got := publishedRoot(t, bs, kr); got != acked {
					rt.Fatalf("step %d: after the replay the backend's root is %s, want %s", i, got.Short(), acked.Short())
				}
				fresh, err := packstore.Open(ctx, packstore.Options{Blobs: bs, Keys: kr, Repo: repo})
				if err != nil {
					rt.Fatal(err)
				}
				readAll(fmt.Sprintf("step %d, another process after the replay", i), fresh)
				_ = fresh.Close()
			}
		}
		readAll("the writer", s)
		if err := s.Close(); err != nil {
			rt.Fatalf("Close: %v", err)
		}
		if got := publishedRoot(t, bs, kr); got != acked {
			rt.Fatalf("after Close the backend's root is %s, want the last acknowledged %s", got.Short(), acked.Short())
		}
		fresh, err := packstore.Open(ctx, packstore.Options{Blobs: bs, Keys: kr, Repo: repo})
		if err != nil {
			rt.Fatal(err)
		}
		defer fresh.Close()
		readAll("another process after Close", fresh)
	})
}

// A repository's first root is published, never journaled: a journal is
// only ever replayed over a manifest that authenticated under its key.
func TestTheFirstRootIsPublished(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	s := openJournaled(t, bs, kr, time.Hour)
	h := commit(t, s, hash.Hash{}, payload("first", 100))
	if got := publishedRoot(t, bs, kr); got != h {
		t.Fatalf("the first root was journaled (the backend's root is %s, want %s)", got.Short(), h.Short())
	}
	if n := packstore.JournalRecords(s); n != 0 {
		t.Fatalf("the first commit left %d records in the journal", n)
	}
}

// On a backend without a journal the option changes nothing.
func TestTheJournalOptionOnABackendWithoutOne(t *testing.T) {
	bs, kr := noJournal{mem.New()}, keyring(t)
	s := openJournaled(t, bs, kr, time.Hour)
	if s.Journaled() {
		t.Fatal("a store on a backend without a journal says it journals")
	}
	first := commit(t, s, hash.Hash{}, payload("first", 100))
	h := commit(t, s, first, payload("second", 100))
	if got := publishedRoot(t, bs, kr); got != h {
		t.Fatalf("on a backend without a journal a commit left the root at %s, want it published at %s", got.Short(), h.Short())
	}
}

type noJournal struct{ blob.BlobStore }

// A journal's commits counted on chunks already stored; a replay checks
// they survived any GC since, as a publish does (DESIGN §9): one whose
// pack GC expired is ErrSessionLost, and nothing is published over it. A
// live journal writer checks the same at each commit. GC runs while the
// writer holds the journal, so it cannot replay it first.
func TestAJournalChecksTheChunksItCountedOn(t *testing.T) {
	for _, crash := range []bool{false, true} {
		t.Run(fmt.Sprintf("crash=%v", crash), func(t *testing.T) {
			bs, kr := mem.New(), keyring(t)
			hs := published(t, open(t, bs, kr), "a, the root", "b, garbage")
			s, err := packstore.Open(ctx, journalOptions(bs, kr, time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Put(ctx, []byte("b, garbage")); err != nil { // counted on: b's pack
				t.Fatal(err)
			}
			h := commit(t, s, hs[0], payload("journaled", 100))
			round(t, bs, kr, liveSet(hs[0]), t0)
			out := round(t, bs, kr, liveSet(hs[0]), t0.Add(time.Hour))
			if len(out.Expired) != 1 {
				t.Fatalf("fixture: expired %v, want b's pack", out.Expired)
			}
			remove(t, bs, out.Expired)
			if !crash {
				h2, err := s.Put(ctx, payload("third", 100))
				if err != nil {
					t.Fatal(err)
				}
				if err := s.CompareAndSetRoot(ctx, h, h2); !errors.Is(err, chunk.ErrSessionLost) {
					t.Fatalf("a journaled commit after GC expired a chunk the journal counted on = %v, want ErrSessionLost", err)
				}
				packstore.Abandon(s)
			} else {
				packstore.Abandon(s)
				if s2, err := packstore.Open(ctx, journalOptions(bs, kr, time.Hour)); !errors.Is(err, chunk.ErrSessionLost) {
					if err == nil {
						_ = s2.Close()
					}
					t.Fatalf("replaying a journal that counted on a chunk GC expired = %v, want ErrSessionLost", err)
				}
			}
			if got := publishedRoot(t, bs, kr); got != hs[0] {
				t.Fatalf("the backend's root moved to %s over a chunk GC expired", got.Short())
			}
		})
	}
}

// A commit too large to journal publishes instead, and so does one that
// would take the journal past its limit.
func TestACommitTooLargeToJournalPublishes(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	s := openJournaled(t, bs, kr, time.Hour)
	root := commit(t, s, hash.Hash{}, payload("first", 100))
	small := commit(t, s, root, payload("small", 100))
	if got := publishedRoot(t, bs, kr); got != root {
		t.Fatalf("positive control: a small commit published (root %s)", got.Short())
	}
	put := func(seed string, n int) {
		for i := range n {
			if _, err := s.Put(ctx, payload(fmt.Sprintf("%s-%d", seed, i), 1<<20)); err != nil {
				t.Fatal(err)
			}
		}
	}
	put("big", 17) // 17 MiB of incompressible chunks in the pending pack
	big := commit(t, s, small, payload("over the record limit", 100))
	if got := publishedRoot(t, bs, kr); got != big {
		t.Fatalf("a commit of 17 MiB left the backend at %s, want it published at %s", got.Short(), big.Short())
	}
	put("half", 9)
	half := commit(t, s, big, payload("9 MiB", 100))
	if got := publishedRoot(t, bs, kr); got != big {
		t.Fatalf("positive control: a 9 MiB commit published (root %s, want %s)", got.Short(), big.Short())
	}
	put("more", 9)
	more := commit(t, s, half, payload("past the journal", 100))
	if got := publishedRoot(t, bs, kr); got != more {
		t.Fatalf("a commit that would take the journal past %d MiB left the backend at %s, want it published", 16, got.Short())
	}
}

// A replay refuses a journal whose last root is a chunk it does not hold.
func TestAReplayRefusesARootItDoesNotHold(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	s := openJournaled(t, bs, kr, time.Hour)
	first := commit(t, s, hash.Hash{}, payload("first", 100))
	_ = commit(t, s, first, payload("second", 100))
	packstore.Abandon(s)
	if err := packstore.ForgeJournalRoot(ctx, bs, kr, repo); err != nil {
		t.Fatal(err)
	}
	if s2, err := packstore.Open(ctx, journalOptions(bs, kr, time.Hour)); !errors.Is(err, chunk.ErrCorrupt) {
		if err == nil {
			_ = s2.Close()
		}
		t.Fatalf("replaying a journal whose root it does not hold = %v, want ErrCorrupt", err)
	}
	if got := publishedRoot(t, bs, kr); got != first {
		t.Fatalf("the backend's root moved to %s", got.Short())
	}
}

// A store without the journal, opened while a journal writer was live,
// refuses to publish over the journal that writer left when it died.
func TestAWriterWithoutTheJournalRefusesAJournalLeftBehind(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	js := openJournaled(t, bs, kr, time.Hour)
	first := commit(t, js, hash.Hash{}, payload("first", 100))
	plain := open(t, bs, kr)
	_ = commit(t, js, first, payload("journaled", 100))
	packstore.Abandon(js)
	h, err := plain.Put(ctx, payload("plain", 100))
	if err != nil {
		t.Fatal(err)
	}
	if err := plain.CompareAndSetRoot(ctx, first, h); !errors.Is(err, blob.ErrJournalBusy) {
		t.Fatalf("a publish over a journal left behind = %v, want ErrJournalBusy until it is replayed", err)
	}
	if got := publishedRoot(t, bs, kr); got != first {
		t.Fatalf("the backend's root moved to %s over journaled commits", got.Short())
	}
}

// A root that moves under a journal holding commits, found first by the
// background publish, is ErrJournalConflict there too, and sticks.
func TestAPublishOverAMovedRootIsRefused(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	s := openJournaled(t, bs, kr, time.Hour)
	o := packstore.Options{Blobs: bs, Keys: kr, Repo: repo}
	first := commit(t, s, hash.Hash{}, payload("first", 100))
	_ = commit(t, s, first, payload("journaled", 100))
	intruder := commit(t, open(t, mem.New(), kr), hash.Hash{}, payload("v0.2.0's", 100))
	if err := packstore.ForceRoot(ctx, o, intruder); err != nil {
		t.Fatal(err)
	}
	if err := packstore.PublishJournal(s); !errors.Is(err, packstore.ErrJournalConflict) {
		t.Fatalf("the background publish over a root that moved under the journal = %v, want ErrJournalConflict", err)
	}
	if _, err := s.Put(ctx, payload("after", 100)); !errors.Is(err, packstore.ErrJournalConflict) {
		t.Fatalf("a put after the publish found the conflict = %v, want ErrJournalConflict", err)
	}
	if got := publishedRoot(t, bs, kr); got != intruder {
		t.Fatalf("the backend's root is %s, want the intruder's left alone", got.Short())
	}
}

// The visibility bound (#34, the owner's): another process, a fresh open
// of the same backend, sees the last published state, never a journaled
// commit before the publish lands, and sees a journaled commit within
// JournalInterval of the first commit the journal took, plus the
// publish's own time (here, in memory, well under a second of slack).
func TestAnotherProcessSeesACommitWithinTheInterval(t *testing.T) {
	const interval = 300 * time.Millisecond
	bs, kr := mem.New(), keyring(t)
	s := openJournaled(t, bs, kr, interval)
	first := commit(t, s, hash.Hash{}, payload("first", 100))
	t0 := time.Now()
	h := commit(t, s, first, payload("second", 100))
	fresh := func() hash.Hash {
		f, err := packstore.Open(ctx, packstore.Options{Blobs: bs, Keys: kr, Repo: repo})
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		r, err := f.Root(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	if got := fresh(); got != first && time.Since(t0) < interval {
		t.Fatalf("a fresh open %v after a journaled commit sees %s, want the published %s until the interval (%v) has passed", time.Since(t0), got.Short(), first.Short(), interval)
	}
	bound := interval + time.Second
	for {
		if got := fresh(); got == h {
			break
		}
		if time.Since(t0) > bound {
			t.Fatalf("a fresh open %v after a journaled commit still does not see it: the bound is the interval (%v) plus one publish", time.Since(t0), interval)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if d := time.Since(t0); d < interval {
		t.Fatalf("a fresh open saw the journaled commit %v after it, before the interval (%v): it published at once", d, interval)
	}
}

// syncCounting is a store whose journal counts its syncs and remembers how
// much of it they made durable, and can hold the first sync until a
// number of writes have landed; crash leaves only what was synced.
type syncCounting struct {
	*mem.Store
	mu       sync.Mutex
	syncs    int
	writes   int
	written  int // bytes written
	durable  int // bytes the last sync covered
	holdFor  int // the first sync waits until this many writes have landed
	held     bool
	failSync error
}

func (c *syncCounting) OpenJournal(ctx context.Context) (blob.Journal, error) {
	j, err := c.Store.OpenJournal(ctx)
	if err != nil {
		return nil, err
	}
	b, err := j.Read(ctx, 1<<30)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.written, c.durable = len(b), len(b)
	c.mu.Unlock()
	return &countingJournal{Journal: j, c: c}, nil
}

type countingJournal struct {
	blob.Journal
	c *syncCounting
}

func (j *countingJournal) Write(ctx context.Context, b []byte) error {
	if err := j.Journal.Write(ctx, b); err != nil {
		return err
	}
	j.c.mu.Lock()
	j.c.writes++
	j.c.written += len(b)
	j.c.mu.Unlock()
	return nil
}

func (j *countingJournal) Sync(ctx context.Context) error {
	j.c.mu.Lock()
	hold := j.c.holdFor > 0 && !j.c.held
	j.c.held = true
	j.c.mu.Unlock()
	if hold {
		deadline := time.Now().Add(5 * time.Second)
		for {
			j.c.mu.Lock()
			n := j.c.writes
			j.c.mu.Unlock()
			if n >= j.c.holdFor || time.Now().After(deadline) {
				break
			}
			time.Sleep(time.Millisecond)
		}
	}
	j.c.mu.Lock()
	defer j.c.mu.Unlock()
	if j.c.failSync != nil {
		return j.c.failSync
	}
	j.c.syncs++
	j.c.durable = j.c.written
	return j.Journal.Sync(ctx)
}

func (j *countingJournal) Append(ctx context.Context, b []byte) error {
	if err := j.Write(ctx, b); err != nil {
		return err
	}
	return j.Sync(ctx)
}

func (j *countingJournal) Reset(ctx context.Context) error {
	if err := j.Journal.Reset(ctx); err != nil {
		return err
	}
	j.c.mu.Lock()
	j.c.written, j.c.durable = 0, 0
	j.c.mu.Unlock()
	return nil
}

// crash leaves the journal as a power cut would: only what a sync covered.
func (c *syncCounting) crash(t *testing.T) {
	t.Helper()
	j, err := c.Store.OpenJournal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	b, err := j.Read(ctx, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	keep := b[:c.durable]
	c.mu.Unlock()
	if err := j.Reset(ctx); err != nil {
		t.Fatal(err)
	}
	if err := j.Append(ctx, keep); err != nil {
		t.Fatal(err)
	}
}

// Grouped fsync (#34, the owner's item 3): commits that queue while a sync
// is in flight share the next one. Sixteen writers each commit once, the
// first sync held until all sixteen records are written: two syncs cover
// them all (the held one, and one for the fifteen that queued behind it).
// Every commit returned only once a sync covered its record: a crash that
// keeps only what was synced reopens at the last commit, and the records
// replay in order.
func TestCommitsThatQueueShareAnFsync(t *testing.T) {
	const writers = 16
	c := &syncCounting{Store: mem.New()}
	kr := keyring(t)
	s, err := packstore.Open(ctx, journalOptions(c, kr, time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	commit(t, s, hash.Hash{}, payload("first", 100)) // published
	c.mu.Lock()
	c.holdFor, c.writes = writers, 0
	c.mu.Unlock()
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h, err := s.Put(ctx, payload(fmt.Sprintf("writer %d", w), 200))
			if err != nil {
				errs <- err
				return
			}
			for {
				root, err := s.Root(ctx)
				if err != nil {
					errs <- err
					return
				}
				err = s.CompareAndSetRoot(ctx, root, h)
				if errors.Is(err, chunk.ErrRootConflict) {
					continue
				}
				if err != nil {
					errs <- err
				}
				return
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("a writer's commit: %v", err)
	}
	c.mu.Lock()
	syncs, writes := c.syncs, c.writes
	c.mu.Unlock()
	if writes != writers {
		t.Fatalf("fixture: %d records written for %d commits", writes, writers)
	}
	if syncs > 2 {
		t.Fatalf("%d commits that queued behind one held sync took %d syncs, want at most 2: commits do not share an fsync", writers, syncs)
	}
	last, err := s.Root(ctx)
	if err != nil {
		t.Fatal(err)
	}
	packstore.Abandon(s)
	c.crash(t)
	re, err := packstore.Open(ctx, journalOptions(c, kr, time.Hour))
	if err != nil {
		t.Fatalf("reopening after a crash that kept only what was synced: %v", err)
	}
	defer re.Close()
	if got, _ := re.Root(ctx); got != last {
		t.Fatalf("after a crash that kept only what was synced the root is %s, want the last commit %s: a commit returned before a sync covered it", got.Short(), last.Short())
	}
}

// A commit whose context is cancelled before it starts writes nothing.
func TestACancelledCommitLeavesNothing(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	s := openJournaled(t, bs, kr, time.Hour)
	first := commit(t, s, hash.Hash{}, payload("first", 100))
	h, err := s.Put(ctx, payload("cancelled", 100))
	if err != nil {
		t.Fatal(err)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := s.CompareAndSetRoot(cctx, first, h); !errors.Is(err, context.Canceled) {
		t.Fatalf("a commit with a cancelled context = %v, want context.Canceled", err)
	}
	if n := packstore.JournalRecords(s); n != 0 {
		t.Fatalf("a cancelled commit left %d records in the journal", n)
	}
	if got, _ := s.Root(ctx); got != first {
		t.Fatalf("a cancelled commit moved the root to %s", got.Short())
	}
	if err := s.CompareAndSetRoot(ctx, first, h); err != nil {
		t.Fatalf("positive control: the same commit uncancelled: %v", err)
	}
}

// A sync that fails fails every commit waiting on it, and the store's
// writes after: what those commits wrote may or may not be on disk.
func TestAFailedSyncFailsItsCommits(t *testing.T) {
	c := &syncCounting{Store: mem.New()}
	kr := keyring(t)
	s := openJournaled(t, c, kr, time.Hour)
	first := commit(t, s, hash.Hash{}, payload("first", 100))
	c.mu.Lock()
	c.failSync = errors.New("disk gone")
	c.mu.Unlock()
	h, err := s.Put(ctx, payload("second", 100))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CompareAndSetRoot(ctx, first, h); err == nil {
		t.Fatal("a commit whose sync failed reported success")
	}
	if _, err := s.Put(ctx, payload("third", 100)); err == nil {
		t.Fatal("a put after a failed sync succeeded: the journal cannot be trusted with another commit")
	}
}

// The owner's decision (#34): a store opened with the default on a disk
// backend, finding another writer holding the journal, is refused with an
// error that says so, not opened quietly without the journal.
func TestADefaultOpenFindingTheJournalHeldIsRefused(t *testing.T) {
	bs := localStore(t)
	kr := keyring(t)
	first, err := packstore.Open(ctx, packstore.Options{Blobs: bs, Keys: kr, Repo: repo})
	if err != nil {
		t.Fatalf("positive control: the first default open: %v", err)
	}
	defer first.Close()
	if !first.Journaled() {
		t.Fatal("fixture: the first default open on local does not journal")
	}
	s2, err := packstore.Open(ctx, packstore.Options{Blobs: bs, Keys: kr, Repo: repo})
	if err == nil {
		_ = s2.Close()
		t.Fatal("a second default open while a writer holds the journal succeeded: it would read stale state and refuse every publish without saying why")
	}
	if !errors.Is(err, blob.ErrJournalBusy) || !strings.Contains(err.Error(), "another writer") {
		t.Fatalf("a second default open = %v, want ErrJournalBusy naming another writer", err)
	}
	ro, err := packstore.Open(ctx, packstore.Options{Blobs: bs, Keys: kr, Repo: repo, Journal: packstore.JournalOff})
	if err != nil {
		t.Fatalf("an open asking for no journal beside the writer: %v", err)
	}
	_ = ro.Close()
}
