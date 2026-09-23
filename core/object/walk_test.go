package object_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/memstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/object"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
	"github.com/SmithOperatingSolutions/snapshot-core/core/stream"
)

// streams (model 5) is a model whose objects are streams, and walks them.
type streams struct{ fake }

func (streams) ID() model.ID { return 5 }
func (streams) Walk(ctx context.Context, root model.Root, r chunk.Reader, visit func(hash.Hash, bool) (bool, error)) error {
	return stream.Walk(ctx, r, stream.Ref{Root: root.Hash, Size: root.Size, Depth: root.Depth}, visit)
}

// recorded is a memstore that remembers the chunks put into it.
type recorded struct {
	*memstore.Store
	mu     sync.Mutex
	stored map[hash.Hash]bool
}

func (r *recorded) Put(ctx context.Context, b []byte) (hash.Hash, error) {
	h, err := r.Store.Put(ctx, b)
	if err == nil {
		r.mu.Lock()
		r.stored[h] = true
		r.mu.Unlock()
	}
	return h, err
}

// walkNamed walks as GC marks: everything named is kept, and a chunk that
// reaches others is gone into once, however it was named before.
func walkNamed(rd chunk.Reader, reg *model.Registry, root hash.Hash) (map[hash.Hash]bool, int, error) {
	named, gone, visits := map[hash.Hash]bool{}, map[hash.Hash]bool{}, 0
	err := object.Walk(ctx, rd, prolly.DefaultConfig(), reg, root, func(h hash.Hash, leaf bool) (bool, error) {
		visits++
		named[h] = true
		if leaf || gone[h] {
			return false, nil
		}
		gone[h] = true
		return true, nil
	})
	return named, visits, err
}

// noise is n bytes that repeat nothing, so no two chunks of it are alike.
func noise(seed string, n int) []byte {
	out := make([]byte, 0, n+32)
	for i := 0; len(out) < n; i++ {
		h := hash.Sum([]byte(fmt.Sprintf("%s/%d", seed, i)))
		out = append(out, h[:]...)
	}
	return out[:n]
}

func streamsRegistry(t *testing.T) *model.Registry {
	t.Helper()
	reg, err := model.NewRegistry(streams{fake{id: 5, diffs: &[][2]model.Root{}}}, fake{id: 1, diffs: &[][2]model.Root{}})
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

// GC marks a namespace (DESIGN §9): Walk names its nodes, root first, and
// every chunk of every object it names, through the object's model; an
// object named at two paths is walked once.
func TestAWalkReachesEveryObject(t *testing.T) {
	s := &recorded{Store: memstore.New(), stored: map[hash.Hash]bool{}}
	reg := streamsRegistry(t)
	c := stream.DefaultConfig()
	c.CDC.Min, c.CDC.Max, c.CDC.Mask = 256, 4<<10, 0x3FF
	e := newNamespace(t, s, reg).Editor()
	var shared object.Ref
	for i := range 3000 {
		n := 50
		if i%500 == 0 {
			n = 60_000
		}
		ref, err := stream.Write(ctx, s, bytes.NewReader(noise(fmt.Sprint("obj", i), n)), c)
		if err != nil {
			t.Fatal(err)
		}
		shared = object.Ref{Model: 5, Root: model.Root{Hash: ref.Root, Size: ref.Size, Depth: ref.Depth, Format: 1}}
		must(t, e.Put(fmt.Sprintf("dir%02d/obj%04d", i%30, i), shared))
	}
	must(t, e.Put("elsewhere/the-same-object", shared))
	n := flush(t, e)
	named, visits, err := walkNamed(s, reg, n.Root())
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	emptyNode := hash.Sum([]byte{0x01, 0x00, 0x00}) // New stored it; a full namespace does not reach it
	for h := range s.stored {
		if h != emptyNode && !named[h] {
			t.Fatalf("Walk never named stored chunk %s", h.Short())
		}
	}
	if len(named) != len(s.stored)-1 {
		t.Fatalf("Walk named %d chunks, want the %d stored less the empty namespace's node", len(named), len(s.stored))
	}
	if visits != len(named)+1 {
		t.Fatalf("Walk made %d visits for %d chunks, want one each and one more for the object named twice: it was walked twice", visits, len(named))
	}
}

// What a namespace reaches is known only through its objects' models, so
// Walk refuses, rather than name too little, a namespace holding an object
// whose model is not registered (ErrUnknownModel) or cannot walk
// (ErrNotWalkable), and one whose stored bytes are forged (ErrCorrupt);
// WalkRef refuses the same objects on their own.
func TestWalkRefusesWhatItCannotWalk(t *testing.T) {
	s := newStore()
	reg := streamsRegistry(t)
	one := func(ref object.Ref) hash.Hash {
		m, err := prolly.Empty(ctx, s, prolly.DefaultConfig())
		must(t, err)
		ed := m.Editor()
		must(t, ed.Put([]byte("x"), ref.Encode()))
		m, err = ed.Flush(ctx)
		must(t, err)
		return m.Root()
	}
	blob := func(seed string) model.Root {
		h, err := s.Put(ctx, []byte(seed))
		must(t, err)
		return model.Root{Hash: h, Size: uint64(len(seed)), Format: 1}
	}
	walkable := object.Ref{Model: 5, Root: blob("walks")}
	if _, _, err := walkNamed(s, reg, one(walkable)); err != nil {
		t.Fatalf("positive control: %v", err)
	}
	unknown := object.Ref{Model: 9, Root: blob("model 9")}
	cannot := object.Ref{Model: 1, Root: blob("model 1 cannot walk")}
	for _, c := range []struct {
		name string
		ref  object.Ref
		want error
	}{
		{"an unregistered model", unknown, model.ErrUnknownModel},
		{"a model that cannot walk", cannot, object.ErrNotWalkable},
		{"a format the model does not know", object.Ref{Model: 5, Root: model.Root{Hash: walkable.Root.Hash, Size: 5, Format: 2}}, model.ErrUnknownModel},
	} {
		if _, _, err := walkNamed(s, reg, one(c.ref)); !errors.Is(err, c.want) {
			t.Errorf("a namespace holding an object of %s: Walk = %v, want %v", c.name, err, c.want)
		}
		err := object.WalkRef(ctx, s, reg, c.ref, func(hash.Hash, bool) (bool, error) { return true, nil })
		if !errors.Is(err, c.want) {
			t.Errorf("an object of %s: WalkRef = %v, want %v", c.name, err, c.want)
		}
	}
	good := walkable.Encode()
	for name, root := range map[string]hash.Hash{
		"a 45-byte reference": forged(t, s, reg, "x", string(good[:45])).Root(),
		"a stored a/../b":     forged(t, s, reg, "a/../b", string(good)).Root(),
	} {
		if _, _, err := walkNamed(s, reg, root); !errors.Is(err, chunk.ErrCorrupt) {
			t.Errorf("%s: Walk = %v, want ErrCorrupt", name, err)
		}
	}
	if _, _, err := walkNamed(s, nil, one(walkable)); err == nil {
		t.Error("Walk without a model registry succeeded")
	}
}
