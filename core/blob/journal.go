package blob

import (
	"context"
	"errors"
)

// ErrJournalBusy is returned while another writer holds a store's journal,
// in this process or another.
var ErrJournalBusy = errors.New("blob: another writer holds the journal")

// Journaler is a BlobStore that keeps one append-only journal beside its
// objects, for the chunk layer's commit journal (#34, docs/DESIGN.md §6).
// It is optional, beside the frozen port as chunk.Flusher is beside the
// chunk port: a store without it (S3, blob/split) commits by publishing.
//
// The journal is not an object: no name reaches it, List never shows it,
// and GC never deletes it. At most one writer has it open at a time; a
// writer that dies (kill -9 included) releases it.
type Journaler interface {
	BlobStore
	// OpenJournal opens the journal for its one writer, empty if there is
	// none yet. It is ErrJournalBusy while the journal is open or held
	// (HoldJournal), in this process or another.
	OpenJournal(ctx context.Context) (Journal, error)
	// HoldJournal keeps the journal from being opened until release is
	// called, and reports its length: a writer that publishes without the
	// journal holds it around each publish, so no journal writer starts
	// meanwhile, and refuses to publish over a journal that is not empty.
	// Any number may hold it at once. It is ErrJournalBusy while the
	// journal is open.
	HoldJournal(ctx context.Context) (size int64, release func(), err error)
	// JournalByDefault says whether a chunk store commits into the journal
	// unless told otherwise: true on disk, where it saves fsyncs.
	JournalByDefault() bool
}

// Journal is an open journal. Its methods may be called from one goroutine
// at a time.
type Journal interface {
	// Read returns the whole journal: ErrTooLarge if it is longer than
	// limit, without reading it.
	Read(ctx context.Context, limit int64) ([]byte, error)
	// Append adds b at the end and returns once it is durable. A crash
	// keeps everything an Append that returned added; what a crash in
	// flight leaves of b is a prefix of it.
	Append(ctx context.Context, b []byte) error
	// Reset empties the journal, durably.
	Reset(ctx context.Context) error
	// Close releases the journal. Every other method fails afterwards.
	Close() error
}

// ErrJournalClosed is returned by a Journal used after Close.
var ErrJournalClosed = errors.New("blob: the journal is closed")

// NoDelete wraps a store so Delete fails with ErrDeleteForbidden. The
// repository runs on a NoDelete store; only core/gc holds the raw one. A
// store's journal stays reachable through it: a Journaler stays one.
func NoDelete(s BlobStore) BlobStore {
	if j, ok := s.(Journaler); ok {
		return noDeleteJournaler{noDelete{s}, j}
	}
	return noDelete{s}
}

type noDelete struct{ BlobStore }

func (noDelete) Delete(context.Context, string) error { return ErrDeleteForbidden }

// noDeleteJournaler is NoDelete of a store with a journal.
type noDeleteJournaler struct {
	noDelete
	j Journaler
}

func (n noDeleteJournaler) OpenJournal(ctx context.Context) (Journal, error) {
	return n.j.OpenJournal(ctx)
}

func (n noDeleteJournaler) JournalByDefault() bool { return n.j.JournalByDefault() }

func (n noDeleteJournaler) HoldJournal(ctx context.Context) (int64, func(), error) {
	return n.j.HoldJournal(ctx)
}
