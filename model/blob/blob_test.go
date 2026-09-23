package blob_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/cdc"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/memstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/stream"
	"github.com/SmithOperatingSolutions/snapshot-core/model/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/model/contract"
)

var ctx = context.Background()

func content(seed uint64, n int) []byte {
	out := make([]byte, 0, n+32)
	for i := 0; len(out) < n; i++ {
		h := sha256.Sum256([]byte(fmt.Sprintf("blob/%d/%d", seed, i)))
		out = append(out, h[:]...)
	}
	return out[:n]
}

// small cuts small chunks, so modest files are multi-chunk streams.
func small() stream.Config {
	c := stream.DefaultConfig()
	c.CDC.Min, c.CDC.Max, c.CDC.Mask = 256, 4<<10, 0x3FF
	return c
}

func subject(t *testing.T) contract.Subject {
	s := memstore.New()
	return contract.Subject{
		Model:    blob.Model{},
		Store:    s,
		Generate: func(seed uint64) []byte { return content(seed, int(seed*7919%50000)) },
		Mutate: func(c []byte, seed uint64) []byte {
			out := bytes.Clone(c)
			return append(out, byte(seed), 'x')
		},
		Write: func(t *testing.T, c []byte) model.Root {
			t.Helper()
			r, err := blob.Write(ctx, s, bytes.NewReader(c), small())
			if err != nil {
				t.Fatal(err)
			}
			return r
		},
		Read: func(t *testing.T, r model.Root) []byte {
			t.Helper()
			rd, err := blob.Open(ctx, s, r)
			if err != nil {
				t.Fatal(err)
			}
			b, err := io.ReadAll(rd)
			if err != nil {
				t.Fatal(err)
			}
			return b
		},
	}
}

// Storage Core Spec: "Every registered model passes model/contract".
func TestContract(t *testing.T) { contract.Run(t, subject) }

func write(t *testing.T, s contract.Subject, b []byte) model.Root { t.Helper(); return s.Write(t, b) }

// Storage Core Spec: "model/blob: both sides edit one file → exactly one
// conflict; one side edits → clean merge".
func TestBothSidesEditingIsOneConflict(t *testing.T) {
	s := subject(t)
	base := write(t, s, content(1, 20000))
	ours, theirs := write(t, s, content(2, 20000)), write(t, s, content(3, 20000))
	m := blob.Model{}
	r, err := m.Merge(ctx, base, ours, theirs, s.Store)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Conflicts) != 1 {
		t.Fatalf("both sides editing one file made %d conflicts, want exactly 1", len(r.Conflicts))
	}
	if r, err = m.Merge(ctx, base, base, theirs, s.Store); err != nil || len(r.Conflicts) != 0 || r.Root != theirs {
		t.Fatalf("only theirs editing: root %+v, %d conflicts, %v; want theirs, clean", r.Root, len(r.Conflicts), err)
	}
	if r, err = m.Merge(ctx, base, ours, base, s.Store); err != nil || len(r.Conflicts) != 0 || r.Root != ours {
		t.Fatalf("only ours editing: root %+v, %d conflicts, %v; want ours, clean", r.Root, len(r.Conflicts), err)
	}
}

// A blob's root is its stream, in the model's format.
func TestABlobIsItsStream(t *testing.T) {
	s := memstore.New()
	data := content(9, 30000)
	r, err := blob.Write(ctx, s, bytes.NewReader(data), small())
	if err != nil {
		t.Fatal(err)
	}
	ref, err := stream.Write(ctx, s, bytes.NewReader(data), small())
	if err != nil {
		t.Fatal(err)
	}
	if r != (model.Root{Hash: ref.Root, Size: ref.Size, Depth: ref.Depth, Format: blob.Format}) || (blob.Model{}).ID() != 1 {
		t.Fatalf("a blob's root is %+v, want its stream %+v in format %d (model id 1)", r, ref, blob.Format)
	}
}

// Validate reads a blob through: damage in its last chunk is found, not only
// in what opening it touches.
func TestValidateReadsTheWholeBlob(t *testing.T) {
	s := memstore.New()
	data := content(4, 40000)
	r, err := blob.Write(ctx, s, bytes.NewReader(data), small())
	if err != nil {
		t.Fatal(err)
	}
	if err := (blob.Model{}).Validate(ctx, r, s); err != nil || r.Depth < 1 {
		t.Fatalf("positive control: a %d-level blob validates: %v", r.Depth, err)
	}
	cut, err := cdc.New(bytes.NewReader(data), small().CDC) // the same cut the writer made
	if err != nil {
		t.Fatal(err)
	}
	var last []byte
	for {
		piece, err := cut.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		last = piece
	}
	if !s.Tamper(hash.Sum(last)) {
		t.Fatal("fixture: the blob's last chunk is not in the store")
	}
	if err := (blob.Model{}).Validate(ctx, r, s); !errors.Is(err, chunk.ErrCorrupt) {
		t.Fatalf("a blob whose last chunk is damaged validates as %v, want ErrCorrupt", err)
	}
}

// A blob in a format this package does not write is not read as one.
func TestOpenRefusesAnotherFormat(t *testing.T) {
	s := memstore.New()
	r, err := blob.Write(ctx, s, bytes.NewReader(content(5, 100)), small())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := blob.Open(ctx, s, r); err != nil {
		t.Fatalf("positive control: %v", err)
	}
	r.Format = 2
	if _, err := blob.Open(ctx, s, r); !errors.Is(err, model.ErrUnknownModel) {
		t.Fatalf("Open of a format-2 blob = %v, want ErrUnknownModel", err)
	}
}
