package tree_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/memstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
	"github.com/SmithOperatingSolutions/snapshot-core/core/stream"
	"github.com/SmithOperatingSolutions/snapshot-core/model/tree"
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

// noise is n bytes that repeat nothing, so a stream of it has no two chunks alike.
func noise(seed string, n int) []byte {
	out := make([]byte, 0, n+32)
	for i := 0; len(out) < n; i++ {
		h := sha256.Sum256([]byte(fmt.Sprintf("%s/%d", seed, i)))
		out = append(out, h[:]...)
	}
	return out[:n]
}

func walkNamed(w model.Walker, r chunk.Reader, root model.Root) (map[hash.Hash]bool, error) {
	named := map[hash.Hash]bool{}
	err := w.Walk(ctx, root, r, func(h hash.Hash) (bool, error) {
		fresh := !named[h]
		named[h] = true
		return fresh, nil
	})
	return named, err
}

// GC marks a tree through its model (DESIGN §9): what a tree reaches is its
// map and, inside the model's own entry bytes, every file's content. Walk
// names every chunk of both, contents one chunk or several levels deep; a
// tree of another format, one claiming a stream depth, and one holding a
// path outside the grammar are refused.
func TestATreeWalksToItsEntriesContents(t *testing.T) {
	w, ok := model.Model(tree.Model{Config: cfg()}).(model.Walker)
	if !ok {
		t.Fatal("tree.Model is not a model.Walker: GC could not collect a repository holding a tree")
	}
	s := &recording{Store: memstore.New(), stored: map[hash.Hash]bool{}}
	sc := stream.DefaultConfig()
	sc.CDC.Min, sc.CDC.Max, sc.CDC.Mask = 256, 4<<10, 0x3FF
	entries := map[string]tree.Entry{}
	deepest := 0
	for i := range 300 {
		n := 20 + i
		if i%30 == 0 {
			n = 200_000 + i
		}
		ref, err := stream.Write(ctx, s, bytes.NewReader(noise(fmt.Sprint("file", i), n)), sc)
		if err != nil {
			t.Fatal(err)
		}
		deepest = max(deepest, int(ref.Depth))
		entries[fmt.Sprintf("dir%d/file%03d", i%7, i)] = tree.Entry{Mode: 0o644, ModTime: int64(i),
			Content: model.Root{Hash: ref.Root, Size: ref.Size, Depth: ref.Depth, Format: 1}}
	}
	root := write(t, s, entries)
	m, err := prolly.Open(ctx, s, cfg(), root.Hash)
	if err != nil {
		t.Fatal(err)
	}
	if m.Height() < 1 || deepest < 2 {
		t.Fatalf("fixture: the map has height %d and the deepest content depth %d, want at least 1 and 2", m.Height(), deepest)
	}
	named, err := walkNamed(w, s, root)
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	emptyNode := hash.Sum([]byte{0x01, 0x00, 0x00}) // stored by Write's empty start, reached by nothing
	for h := range s.stored {
		if h != emptyNode && !named[h] {
			t.Fatalf("Walk never named stored chunk %s", h.Short())
		}
	}
	if len(named) != len(s.stored)-1 {
		t.Fatalf("Walk named %d chunks, want the %d stored less the empty map's node", len(named), len(s.stored))
	}

	other := root
	other.Format = 2
	if _, err := walkNamed(w, s, other); !errors.Is(err, model.ErrUnknownModel) {
		t.Errorf("Walk of a format-2 tree = %v, want ErrUnknownModel", err)
	}
	deep := root
	deep.Depth = 1
	if _, err := walkNamed(w, s, deep); !errors.Is(err, chunk.ErrCorrupt) {
		t.Errorf("Walk of a tree claiming stream depth 1 = %v, want ErrCorrupt", err)
	}
	pm, err := prolly.Empty(ctx, s, cfg())
	if err != nil {
		t.Fatal(err)
	}
	e := pm.Editor()
	for _, k := range []string{"ok", "a/../b"} {
		if err := e.Put([]byte(k), file(k).Encode()); err != nil {
			t.Fatal(err)
		}
	}
	if pm, err = e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	bad := model.Root{Hash: pm.Root(), Size: pm.Count(), Format: tree.Format}
	if _, err := walkNamed(w, s, bad); !errors.Is(err, chunk.ErrCorrupt) {
		t.Errorf("Walk of a tree holding a/../b = %v, want ErrCorrupt", err)
	}
}
