package object_test

import (
	"runtime"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/memstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/object"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
)

// allocated is how many bytes f allocated, by the runtime's own count.
func allocated(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// #23: a namespace's value is an object reference, exactly RefSize bytes.
// Getting, walking (GC's mark, scrub) or diffing a namespace whose value
// at a path is a 4 MiB stream must refuse it without reading the stream.
func TestRegression_SC23_ANamespaceReadsNoValueLongerThanARef(t *testing.T) {
	s := memstore.New()
	reg := streamsRegistry(t)
	w := prolly.DefaultConfig()
	w.MaxValue = 8 << 20
	m, err := prolly.Empty(ctx, s, w)
	must(t, err)
	e := m.Editor()
	must(t, e.Put([]byte("a"), ref(5, "a").Encode()))
	good, err := e.Flush(ctx)
	must(t, err)
	must(t, e.Put([]byte("big"), noise("sc23 value", 4<<20)))
	bad, err := e.Flush(ctx)
	must(t, err)
	c := prolly.DefaultConfig()
	open := func(root hash.Hash) *object.Namespace {
		n, err := object.Open(ctx, s, c, reg, root)
		must(t, err)
		return n
	}
	empty := newNamespace(t, s, reg)
	for how, read := range map[string]func(root hash.Hash, path string) error{
		"Get": func(root hash.Hash, path string) error {
			_, _, _, err := open(root).Get(ctx, path)
			return err
		},
		"Walk": func(root hash.Hash, _ string) error {
			return object.Walk(ctx, s, c, reg, root, func(hash.Hash, bool) (bool, error) { return true, nil })
		},
		"Diff": func(root hash.Hash, _ string) error {
			d, err := object.Diff(ctx, empty, open(root))
			for err == nil {
				var ok bool
				if _, ok, err = d.Next(); !ok {
					break
				}
			}
			return err
		},
	} {
		if err := read(good.Root(), "a"); err != nil {
			t.Fatalf("positive control: %s of the honest namespace: %v", how, err)
		}
		used := allocated(func() { err = read(bad.Root(), "big") })
		if err == nil || used > 512<<10 {
			t.Errorf("%s of a namespace with a 4 MiB value allocated %d bytes (%v); want it refused within 512 KiB", how, used, err)
		}
	}
}
