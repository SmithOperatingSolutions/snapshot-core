package object_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"unicode/utf8"

	"pgregory.net/rapid"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/memstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/object"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
)

var ctx = context.Background()

// counting is a memstore that counts reads, in all and of each hash.
type counting struct {
	*memstore.Store
	gets atomic.Int64
	mu   sync.Mutex
	of   map[hash.Hash]int
}

func newStore() *counting { return &counting{Store: memstore.New(), of: map[hash.Hash]int{}} }

func (c *counting) Get(ctx context.Context, h hash.Hash) ([]byte, error) {
	c.gets.Add(1)
	c.mu.Lock()
	c.of[h]++
	c.mu.Unlock()
	return c.Store.Get(ctx, h)
}

// fake is a model that records the diffs it is asked for.
type fake struct {
	id    model.ID
	diffs *[][2]model.Root
}

func (f fake) ID() model.ID                                             { return f.id }
func (f fake) FormatVersion() uint16                                    { return 1 }
func (f fake) Validate(context.Context, model.Root, chunk.Reader) error { return nil }
func (f fake) Diff(_ context.Context, from, to model.Root, _ chunk.Reader) (model.DiffIter, error) {
	*f.diffs = append(*f.diffs, [2]model.Root{from, to})
	return nil, nil
}
func (f fake) Merge(context.Context, model.Root, model.Root, model.Root, chunk.ReadWriter) (model.MergeResult, error) {
	return model.MergeResult{}, nil
}

func registry(t *testing.T, ids ...model.ID) (*model.Registry, *[][2]model.Root) {
	t.Helper()
	diffs := &[][2]model.Root{}
	var ms []model.Model
	for _, id := range ids {
		ms = append(ms, fake{id: id, diffs: diffs})
	}
	r, err := model.NewRegistry(ms...)
	if err != nil {
		t.Fatal(err)
	}
	return r, diffs
}

func ref(m model.ID, seed string) object.Ref {
	return object.Ref{Model: m, Root: model.Root{Hash: hash.Sum([]byte(seed)), Size: uint64(len(seed)), Format: 1}}
}

func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func newNamespace(t *testing.T, s chunk.ReadWriter, reg *model.Registry) *object.Namespace {
	t.Helper()
	n, err := object.New(ctx, s, prolly.DefaultConfig(), reg)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func flush(t *testing.T, e *object.Editor) *object.Namespace {
	t.Helper()
	n, err := e.Flush(ctx)
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	return n
}

// The record is the documented 46 bytes, laid out by hand here.
func TestARefIsTheDocumented46Bytes(t *testing.T) {
	root := hash.Sum([]byte("an object"))
	r := object.Ref{Model: 3, Root: model.Root{Hash: root, Size: 123456, Depth: 5, Format: 2}}
	want := binary.LittleEndian.AppendUint16(nil, 3)
	want = binary.LittleEndian.AppendUint16(want, 2)
	want = append(want, 0, 5)
	want = binary.LittleEndian.AppendUint64(want, 123456)
	want = append(want, root[:]...)
	got := r.Encode()
	if !bytes.Equal(got, want) || len(got) != object.RefSize {
		t.Fatalf("Encode = %x (%d bytes), want %x", got, len(got), want)
	}
	if back, err := object.DecodeRef(want); err != nil || back != r {
		t.Fatalf("DecodeRef = %+v, %v; want %+v", back, err, r)
	}
	flags := bytes.Clone(want)
	flags[4] = 1
	zeroModel, zeroFormat := bytes.Clone(want), bytes.Clone(want)
	zeroModel[0], zeroFormat[2] = 0, 0
	for name, b := range map[string][]byte{
		"45 bytes": want[:45], "47 bytes": append(bytes.Clone(want), 0),
		"flags set": flags, "model 0": zeroModel, "format 0": zeroFormat,
	} {
		if _, err := object.DecodeRef(b); !errors.Is(err, chunk.ErrCorrupt) {
			t.Errorf("%s: DecodeRef = %v, want ErrCorrupt", name, err)
		}
	}
}

// refValid is the path grammar of DESIGN §8, written here independently.
func refValid(p string) bool {
	if p == "" || len(p) > 4096 || !utf8.ValidString(p) {
		return false
	}
	segs := strings.Split(p, "/")
	if len(segs) > 64 {
		return false
	}
	for _, s := range segs {
		if s == "" || len(s) > 255 || s == "." || s == ".." {
			return false
		}
		for i := 0; i < len(s); i++ {
			if s[i] < 0x20 || s[i] == 0x7f {
				return false
			}
		}
	}
	return true
}

// Storage Core Spec: "Invalid paths (a/../b, empty segment, 256-byte
// segment, NUL) are rejected (property test)".
func TestInvalidPathsAreRejected(t *testing.T) {
	for _, p := range []string{"a", "a/b", "dir.d/file.txt", "é/ü", strings.Repeat("s", 255), "a/..b", "..a", "a.", "..."} {
		if err := object.ValidPath(p); err != nil {
			t.Errorf("positive control: ValidPath(%q) = %v", p, err)
		}
	}
	for _, p := range []string{"", "a/../b", "a//b", "/a", "a/", ".", "..", "a/./b", "a\x00b", "a\nb", "a\x7fb", "\xff",
		strings.Repeat("s", 256), strings.Repeat("a/", 64) + "a", strings.Repeat("s", 255) + strings.Repeat("/"+strings.Repeat("s", 255), 16)} {
		if err := object.ValidPath(p); !errors.Is(err, object.ErrInvalidPath) {
			t.Errorf("ValidPath(%q) = %v, want ErrInvalidPath", p, err)
		}
	}
}

// Strings built from the characters that matter agree with the grammar.
func TestPathsAgreeWithTheGrammar(t *testing.T) {
	alphabet := []byte{'a', 'b', '/', '.', 0x00, 0x1f, 0x7f, 0xc3, 0xa9, 0xff, ' '}
	rapid.Check(t, func(rt *rapid.T) {
		var b []byte
		switch rapid.IntRange(0, 2).Draw(rt, "kind") {
		case 0:
			b = rapid.SliceOfN(rapid.SampledFrom(alphabet), 0, 12).Draw(rt, "short")
		case 1:
			seg := rapid.SliceOfN(rapid.SampledFrom([]byte{'a', '.', 'b'}), 0, 300).Draw(rt, "segment")
			b = append(append([]byte("x/"), seg...), "/y"...)
		default:
			n := rapid.IntRange(60, 70).Draw(rt, "segments")
			b = []byte(strings.TrimSuffix(strings.Repeat("s/", n), "/"))
		}
		p := string(b)
		if got, want := object.ValidPath(p) == nil, refValid(p); got != want {
			rt.Fatalf("ValidPath(%q) accepted = %v, the grammar says %v", p, got, want)
		}
	})
}

// Storage Core Spec: "One commit holding a table, a blob and a JSON document
// round-trips all three" (the namespace half; the commit is core/vcs).
func TestObjectsOfSeveralModelsRoundTrip(t *testing.T) {
	s := newStore()
	reg, _ := registry(t, 1, 3, 4)
	e := newNamespace(t, s, reg).Editor()
	objs := map[string]object.Ref{"files/photo.jpg": ref(1, "blob"), "db/users": ref(3, "table"), "docs/config.json": ref(4, "json")}
	for p, r := range objs {
		must(t, e.Put(p, r))
	}
	n := flush(t, e)
	re, err := object.Open(ctx, s, prolly.DefaultConfig(), reg, n.Root())
	if err != nil {
		t.Fatal(err)
	}
	if re.Count() != 3 {
		t.Fatalf("Count = %d, want 3", re.Count())
	}
	for p, want := range objs {
		got, m, ok, err := re.Get(ctx, p)
		if err != nil || !ok || got != want || m == nil || m.ID() != want.Model {
			t.Fatalf("Get(%s) = %+v, model %v, %v, %v; want %+v", p, got, m, ok, err, want)
		}
	}
	if _, _, ok, err := re.Get(ctx, "files/other.jpg"); ok || err != nil {
		t.Fatalf("Get of a missing path = %v, %v", ok, err)
	}
}

// Storage Core Spec: "An object with an unregistered model id returns
// ErrUnknownModel; nothing is decoded" — nothing of it is even read.
func TestAnUnregisteredModelIsUnknownAndNothingIsRead(t *testing.T) {
	s := newStore()
	full, _ := registry(t, 1, 2)
	e := newNamespace(t, s, full).Editor()
	stranger := ref(2, "an object of model 2")
	must(t, e.Put("known", ref(1, "blob")))
	must(t, e.Put("stranger", stranger))
	n := flush(t, e)
	partial, _ := registry(t, 1)
	re, err := object.Open(ctx, s, prolly.DefaultConfig(), partial, n.Root())
	if err != nil {
		t.Fatal(err)
	}
	if _, m, ok, err := re.Get(ctx, "known"); err != nil || !ok || m == nil {
		t.Fatalf("positive control: %v %v", ok, err)
	}
	if _, m, _, err := re.Get(ctx, "stranger"); !errors.Is(err, model.ErrUnknownModel) || m != nil {
		t.Fatalf("Get of an object whose model is unregistered = %v, %v; want ErrUnknownModel", m, err)
	}
	if n := s.of[stranger.Root.Hash]; n != 0 {
		t.Fatalf("the unknown object's root was read %d times", n)
	}
	if err := re.Editor().Put("new", ref(9, "x")); !errors.Is(err, model.ErrUnknownModel) {
		t.Fatalf("Put of an object of an unregistered model = %v, want ErrUnknownModel", err)
	}
}

func TestEditsRefuseInvalidPathsAndRefs(t *testing.T) {
	s := newStore()
	reg, _ := registry(t, 1)
	n := newNamespace(t, s, reg)
	e := n.Editor()
	if err := e.Put("ok/path", ref(1, "x")); err != nil {
		t.Fatalf("positive control: %v", err)
	}
	for _, p := range []string{"a/../b", "a//b", ""} {
		if err := e.Put(p, ref(1, "x")); !errors.Is(err, object.ErrInvalidPath) {
			t.Errorf("Put(%q) = %v, want ErrInvalidPath", p, err)
		}
		if err := e.Delete(p); !errors.Is(err, object.ErrInvalidPath) {
			t.Errorf("Delete(%q) = %v, want ErrInvalidPath", p, err)
		}
		if _, _, _, err := n.Get(ctx, p); !errors.Is(err, object.ErrInvalidPath) {
			t.Errorf("Get(%q) = %v, want ErrInvalidPath", p, err)
		}
	}
	flagged := ref(1, "x")
	flagged.Flags = 1
	if err := e.Put("flagged", flagged); err == nil {
		t.Error("Put of a ref with flags set, which format v1 does not define, succeeded")
	}
}

func diff(t *testing.T, from, to *object.Namespace) (*object.DiffIter, []object.Change) {
	t.Helper()
	d, err := object.Diff(ctx, from, to)
	if err != nil {
		t.Fatal(err)
	}
	var out []object.Change
	for {
		c, ok, err := d.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			return d, out
		}
		out = append(out, c)
	}
}

// Storage Core Spec: "Changing one file in a 100,000-path namespace reads
// fewer than 200 nodes to diff".
func TestChangingOneFileInA100kNamespaceReadsFewNodes(t *testing.T) {
	s := newStore()
	reg, _ := registry(t, 1)
	e := newNamespace(t, s, reg).Editor()
	path := func(i int) string { return fmt.Sprintf("dir%03d/file%05d.dat", i%500, i) }
	for i := 0; i < 100000; i++ {
		must(t, e.Put(path(i), ref(1, path(i))))
	}
	from := flush(t, e)
	changed := ref(1, "new content")
	must(t, e.Put(path(54321), changed))
	to := flush(t, e)
	s.gets.Store(0)
	_, got := diff(t, from, to)
	if reads := s.gets.Load(); reads >= 200 {
		t.Fatalf("diffing a one-file change in 100,000 paths read %d nodes, want fewer than 200", reads)
	}
	if len(got) != 1 || got[0].Path != path(54321) || got[0].Kind != prolly.Modified || got[0].From != ref(1, path(54321)) || got[0].To != changed {
		t.Fatalf("the diff was %+v, want the one modified path", got)
	}
}

// A change within one model asks that model where it changed; an add, a
// delete, or a change of model has no detail.
func TestDetailAsksTheOwningModel(t *testing.T) {
	s := newStore()
	reg, diffs := registry(t, 1, 3)
	e := newNamespace(t, s, reg).Editor()
	must(t, e.Put("same model", ref(1, "v1")))
	must(t, e.Put("changes model", ref(1, "blob")))
	must(t, e.Put("removed", ref(1, "gone")))
	from := flush(t, e)
	must(t, e.Put("same model", ref(1, "v2")))
	must(t, e.Put("changes model", ref(3, "now a table")))
	must(t, e.Delete("removed"))
	must(t, e.Put("added", ref(3, "new")))
	to := flush(t, e)
	d, changes := diff(t, from, to)
	if len(changes) != 4 {
		t.Fatalf("the diff has %d changes, want 4: %+v", len(changes), changes)
	}
	for _, c := range changes {
		_, err := d.Detail(ctx, c)
		switch c.Path {
		case "same model":
			if err != nil || len(*diffs) != 1 || (*diffs)[0] != [2]model.Root{ref(1, "v1").Root, ref(1, "v2").Root} {
				t.Fatalf("Detail of a change within one model: %v, model asked %v", err, *diffs)
			}
		default:
			if err == nil {
				t.Errorf("Detail of %q (%v) returned no error: it has no one model to ask", c.Path, c.Kind)
			}
		}
	}
	if len(*diffs) != 1 {
		t.Fatalf("models were asked %d diffs, want 1", len(*diffs))
	}
}
