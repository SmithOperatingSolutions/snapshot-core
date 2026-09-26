//go:build unix

package local_test

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
)

// The journal is a regular file. A named pipe (or a directory, or a
// device) where the journal should be opens on some kernels: Linux
// refuses a directory with O_CREATE, macOS does not (the race suite on
// macOS found it, #34), and a FIFO with both ends held open opens on
// either. Opening or holding such a journal is the filesystem's kind of
// error, never a working journal and never ErrJournalBusy, so a store
// pointed at the wrong thing fails loudly instead of appending to it.
func TestAJournalThatIsNotARegularFileIsRefused(t *testing.T) {
	s, dir := newStore(t)
	path := filepath.Join(dir, "journal")
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Fatal(err)
	}
	// Both ends held open, so neither open below blocks on the pipe.
	ends, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer ends.Close()
	if j, err := s.OpenJournal(ctx); err == nil || errors.Is(err, blob.ErrJournalBusy) {
		if j != nil {
			_ = j.Close()
		}
		t.Fatalf("OpenJournal where the journal is a named pipe = %v, want an error that it is not a regular file", err)
	}
	if _, release, err := s.HoldJournal(ctx); err == nil || errors.Is(err, blob.ErrJournalBusy) {
		if release != nil {
			release()
		}
		t.Fatalf("HoldJournal where the journal is a named pipe = %v, want an error that it is not a regular file", err)
	}
}
