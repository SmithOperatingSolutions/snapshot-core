// Package contract is the suite every blob.Journaler must pass (#34):
// kept apart from blob/contract so that the object store's contract, which
// every backend runs, does not change when the journal's does.
package contract

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
)

// Factory returns a fresh, empty store that keeps a journal, and a
// function that opens the same store again, as a new process would.
type Factory func(t *testing.T) (s blob.Journaler, reopen func(t *testing.T) blob.Journaler)

// Run runs the journal's contract (blob.Journaler, #34): what an
// Append that returned added is there after the store is opened again,
// one writer at a time, and the journal is never an object.
func Run(t *testing.T, newStore Factory) {
	t.Run("StartsEmpty", func(t *testing.T) { journalStartsEmpty(t, newStore) })
	t.Run("AppendsReadBackAfterReopen", func(t *testing.T) { journalAppendsReadBack(t, newStore) })
	t.Run("ResetEmpties", func(t *testing.T) { journalResetEmpties(t, newStore) })
	t.Run("OneWriterAtATime", func(t *testing.T) { journalOneWriter(t, newStore) })
	t.Run("HoldKeepsWritersOut", func(t *testing.T) { journalHold(t, newStore) })
	t.Run("ReadLimit", func(t *testing.T) { journalReadLimit(t, newStore) })
	t.Run("ClosedRefuses", func(t *testing.T) { journalClosed(t, newStore) })
	t.Run("IsNotAnObject", func(t *testing.T) { journalNotAnObject(t, newStore) })
	t.Run("WritesAreReadAndSyncedInOrder", func(t *testing.T) { journalWriteSync(t, newStore) })
}

func openJournal(t *testing.T, s blob.Journaler) blob.Journal {
	t.Helper()
	j, err := s.OpenJournal(context.Background())
	if err != nil {
		t.Fatalf("OpenJournal: %v", err)
	}
	return j
}

func readJournal(t *testing.T, j blob.Journal) []byte {
	t.Helper()
	b, err := j.Read(context.Background(), 1<<30)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	return b
}

func appendJournal(t *testing.T, j blob.Journal, b []byte) {
	t.Helper()
	if err := j.Append(context.Background(), b); err != nil {
		t.Fatalf("Append: %v", err)
	}
}

func journalStartsEmpty(t *testing.T, newStore Factory) {
	s, _ := newStore(t)
	j := openJournal(t, s)
	defer j.Close()
	if b := readJournal(t, j); len(b) != 0 {
		t.Fatalf("a new store's journal holds %d bytes; a writer would replay commits nobody made", len(b))
	}
}

func journalAppendsReadBack(t *testing.T, newStore Factory) {
	ctx := context.Background()
	s, reopen := newStore(t)
	j := openJournal(t, s)
	a, b := payload("journal-a", 5000), payload("journal-b", 70000)
	appendJournal(t, j, a)
	appendJournal(t, j, b)
	want := append(append([]byte(nil), a...), b...)
	if got := readJournal(t, j); !bytes.Equal(got, want) {
		t.Fatalf("the journal reads %d bytes after appending %d: an acknowledged commit would not replay", len(got), len(want))
	}
	if err := j.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	s2 := reopen(t)
	j2, err := s2.OpenJournal(ctx)
	if err != nil {
		t.Fatalf("OpenJournal on the store opened again: %v", err)
	}
	defer j2.Close()
	if got := readJournal(t, j2); !bytes.Equal(got, want) {
		t.Fatalf("opened again, the journal reads %d bytes where %d were appended and acknowledged: commits a writer was told were durable are gone",
			len(got), len(want))
	}
	c := payload("journal-c", 100)
	appendJournal(t, j2, c)
	if got := readJournal(t, j2); !bytes.Equal(got, append(want, c...)) {
		t.Fatalf("an append after reopening does not land after what was there (%d bytes, want %d)", len(got), len(want)+len(c))
	}
}

func journalResetEmpties(t *testing.T, newStore Factory) {
	ctx := context.Background()
	s, reopen := newStore(t)
	j := openJournal(t, s)
	appendJournal(t, j, payload("before-reset", 3000))
	if got := readJournal(t, j); len(got) != 3000 {
		t.Fatalf("positive control: the journal holds %d bytes, want 3000", len(got))
	}
	if err := j.Reset(ctx); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if got := readJournal(t, j); len(got) != 0 {
		t.Fatalf("after Reset the journal holds %d bytes: published commits would replay again", len(got))
	}
	after := payload("after-reset", 1000)
	appendJournal(t, j, after)
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j2 := openJournal(t, reopen(t))
	defer j2.Close()
	if got := readJournal(t, j2); !bytes.Equal(got, after) {
		t.Fatalf("opened again after Reset and one append, the journal reads %d bytes, want the %d appended since", len(got), len(after))
	}
}

func journalOneWriter(t *testing.T, newStore Factory) {
	ctx := context.Background()
	s, reopen := newStore(t)
	j := openJournal(t, s)
	if j2, err := s.OpenJournal(ctx); !errors.Is(err, blob.ErrJournalBusy) {
		if err == nil {
			_ = j2.Close()
		}
		t.Fatalf("a second OpenJournal while one is open = %v, want ErrJournalBusy: two writers would append to one journal", err)
	}
	if j2, err := reopen(t).OpenJournal(ctx); !errors.Is(err, blob.ErrJournalBusy) {
		if err == nil {
			_ = j2.Close()
		}
		t.Fatalf("OpenJournal through the store opened again, while the first is open = %v, want ErrJournalBusy", err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j3, err := s.OpenJournal(ctx)
	if err != nil {
		t.Fatalf("OpenJournal after the writer closed it = %v, want it open: a closed journal stays locked", err)
	}
	_ = j3.Close()
}

func journalHold(t *testing.T, newStore Factory) {
	ctx := context.Background()
	s, _ := newStore(t)
	j := openJournal(t, s)
	appendJournal(t, j, payload("held", 777))
	if _, release, err := s.HoldJournal(ctx); !errors.Is(err, blob.ErrJournalBusy) {
		if err == nil {
			release()
		}
		t.Fatalf("HoldJournal while a writer has it open = %v, want ErrJournalBusy: a publish could land under the journal", err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	size, release1, err := s.HoldJournal(ctx)
	if err != nil {
		t.Fatalf("HoldJournal with no writer: %v", err)
	}
	if size != 777 {
		t.Fatalf("HoldJournal reports %d bytes, want the 777 in it: a publish would land over unreplayed commits", size)
	}
	_, release2, err := s.HoldJournal(ctx)
	if err != nil {
		t.Fatalf("a second HoldJournal beside the first = %v, want both held", err)
	}
	if j2, err := s.OpenJournal(ctx); !errors.Is(err, blob.ErrJournalBusy) {
		if err == nil {
			_ = j2.Close()
		}
		t.Fatalf("OpenJournal while it is held = %v, want ErrJournalBusy", err)
	}
	release1()
	if j2, err := s.OpenJournal(ctx); !errors.Is(err, blob.ErrJournalBusy) {
		if err == nil {
			_ = j2.Close()
		}
		t.Fatalf("OpenJournal while one hold remains = %v, want ErrJournalBusy", err)
	}
	release2()
	j3, err := s.OpenJournal(ctx)
	if err != nil {
		t.Fatalf("OpenJournal after every hold was released = %v, want it open", err)
	}
	_ = j3.Close()
}

func journalReadLimit(t *testing.T, newStore Factory) {
	ctx := context.Background()
	s, _ := newStore(t)
	j := openJournal(t, s)
	defer j.Close()
	appendJournal(t, j, payload("limit", 4096))
	if b, err := j.Read(ctx, 4096); err != nil || len(b) != 4096 {
		t.Fatalf("positive control: Read at exactly the journal's length = %d bytes, %v", len(b), err)
	}
	if _, err := j.Read(ctx, 4095); !errors.Is(err, blob.ErrTooLarge) {
		t.Fatalf("Read with a limit one byte short = %v, want ErrTooLarge: a reader would take a journal of any size into memory", err)
	}
}

func journalClosed(t *testing.T, newStore Factory) {
	ctx := context.Background()
	s, _ := newStore(t)
	j := openJournal(t, s)
	appendJournal(t, j, []byte("x"))
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if err := j.Append(ctx, []byte("y")); !errors.Is(err, blob.ErrJournalClosed) {
		t.Fatalf("Append after Close = %v, want ErrJournalClosed: a released journal could be written by two", err)
	}
	if err := j.Reset(ctx); !errors.Is(err, blob.ErrJournalClosed) {
		t.Fatalf("Reset after Close = %v, want ErrJournalClosed", err)
	}
	if _, err := j.Read(ctx, 10); !errors.Is(err, blob.ErrJournalClosed) {
		t.Fatalf("Read after Close = %v, want ErrJournalClosed", err)
	}
}

func journalNotAnObject(t *testing.T, newStore Factory) {
	ctx := context.Background()
	s, _ := newStore(t)
	j := openJournal(t, s)
	defer j.Close()
	appendJournal(t, j, payload("hidden", 2048))
	put(t, s, "packs/visible", []byte("an object"))
	infos, err := s.List(ctx, "", "", blob.MaxListPage)
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 1 || infos[0].Name != "packs/visible" {
		t.Fatalf("List shows %v, want only packs/visible: GC would see the journal as an orphan and delete it", infos)
	}
	for _, name := range []string{"journal", "root", "journal.lock"} {
		if _, err := s.Stat(ctx, name); !errors.Is(err, blob.ErrNotFound) {
			t.Fatalf("Stat(%q) = %v, want ErrNotFound: the journal is reachable as an object", name, err)
		}
	}
}

func journalWriteSync(t *testing.T, newStore Factory) {
	ctx := context.Background()
	s, reopen := newStore(t)
	j := openJournal(t, s)
	a, b, c := payload("write-a", 3000), payload("write-b", 5000), payload("write-c", 100)
	for _, x := range [][]byte{a, b} {
		if err := j.Write(ctx, x); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	want := append(append([]byte(nil), a...), b...)
	if got := readJournal(t, j); !bytes.Equal(got, want) {
		t.Fatalf("after two writes the journal reads %d bytes, want the %d written, in order", len(got), len(want))
	}
	if err := j.Sync(ctx); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	appendJournal(t, j, c) // an append after writes lands after them
	want = append(want, c...)
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j2 := openJournal(t, reopen(t))
	defer j2.Close()
	if got := readJournal(t, j2); !bytes.Equal(got, want) {
		t.Fatalf("opened again, the journal reads %d bytes, want the %d written and synced: commits told they were durable are gone", len(got), len(want))
	}
	if err := j2.Close(); err != nil {
		t.Fatal(err)
	}
	if err := j2.Write(ctx, []byte("x")); !errors.Is(err, blob.ErrJournalClosed) {
		t.Fatalf("Write after Close = %v, want ErrJournalClosed", err)
	}
	if err := j2.Sync(ctx); !errors.Is(err, blob.ErrJournalClosed) {
		t.Fatalf("Sync after Close = %v, want ErrJournalClosed", err)
	}
}

func payload(seed string, n int) []byte {
	out := make([]byte, 0, n+32)
	for i := 0; len(out) < n; i++ {
		h := sha256.Sum256([]byte(fmt.Sprintf("%s/%d", seed, i)))
		out = append(out, h[:]...)
	}
	return out[:n]
}

func put(t *testing.T, s blob.BlobStore, name string, b []byte) {
	t.Helper()
	if err := s.Put(context.Background(), name, strings.NewReader(string(b)), int64(len(b))); err != nil {
		t.Fatalf("Put(%s): %v", name, err)
	}
}

var _ = io.EOF
