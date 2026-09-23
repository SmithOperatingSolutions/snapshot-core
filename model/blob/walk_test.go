package blob_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/memstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/model/blob"
)

// recording is a memstore that remembers the chunks put into it.
type recording struct {
	*memstore.Store
	mu     sync.Mutex
	stored map[hash.Hash]bool
}

func (r *recording) Put(ctx context.Context, b []byte) (hash.Hash, error) {
	h, err := r.Store.Put(ctx, b)
	if err == nil {
		r.mu.Lock()
		r.stored[h] = true
		r.mu.Unlock()
	}
	return h, err
}

// GC marks a blob through its model (DESIGN §9): Walk names every chunk the
// blob's stream is made of, from an empty blob to one several levels deep,
// and refuses a blob of a format this package does not write.
func TestABlobWalksToEveryChunk(t *testing.T) {
	w, ok := model.Model(blob.Model{}).(model.Walker)
	if !ok {
		t.Fatal("blob.Model is not a model.Walker: GC could not collect a repository holding a blob")
	}
	var last model.Root
	for _, n := range []int{0, 100, 200_000} {
		s := &recording{Store: memstore.New(), stored: map[hash.Hash]bool{}}
		r, err := blob.Write(ctx, s, bytes.NewReader(content(uint64(n), n)), small())
		if err != nil {
			t.Fatal(err)
		}
		named := map[hash.Hash]bool{}
		err = w.Walk(ctx, r, s, func(h hash.Hash) (bool, error) {
			fresh := !named[h]
			named[h] = true
			return fresh, nil
		})
		if err != nil {
			t.Fatalf("a %d-byte blob: Walk: %v", n, err)
		}
		for h := range s.stored {
			if !named[h] {
				t.Fatalf("a %d-byte blob: Walk never named stored chunk %s", n, h.Short())
			}
		}
		if len(named) != len(s.stored) {
			t.Fatalf("a %d-byte blob: Walk named %d chunks, the write stored %d", n, len(named), len(s.stored))
		}
		last = r
	}
	if last.Depth < 2 {
		t.Fatalf("fixture: the largest blob has depth %d, want at least 2", last.Depth)
	}
	other := last
	other.Format = 2
	err := w.Walk(ctx, other, memstore.New(), func(hash.Hash) (bool, error) { return true, nil })
	if !errors.Is(err, model.ErrUnknownModel) {
		t.Fatalf("Walk of a format-2 blob = %v, want ErrUnknownModel", err)
	}
}
