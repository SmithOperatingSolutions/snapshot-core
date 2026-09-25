package packstore_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
	return packstore.WithBackoff(packstore.Options{Blobs: bs, Keys: kr, Repo: repo, Journal: true, JournalInterval: interval}, time.Millisecond)
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

type noJournal struct{ blob.BlobStore }


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
		put := func() hash.Hash {
			n++
			d := payload(fmt.Sprintf("prop-%d", n), rapid.IntRange(0, 3000).Draw(rt, "size"))
			h, err := s.Put(ctx, d)
			if err != nil {
				rt.Fatalf("Put: %v", err)
			}
			datas[h] = d
			return h
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
				var last hash.Hash
				for range rapid.IntRange(1, 4).Draw(rt, "chunks") {
					last = put()
				}
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
