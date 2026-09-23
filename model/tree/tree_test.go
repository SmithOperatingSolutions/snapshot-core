package tree_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/memstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
	"github.com/SmithOperatingSolutions/snapshot-core/model/contract"
	"github.com/SmithOperatingSolutions/snapshot-core/model/tree"
)

var ctx = context.Background()

func cfg() prolly.Config { return prolly.DefaultConfig() }

func file(seed string) tree.Entry {
	return tree.Entry{Mode: 0o644, ModTime: int64(len(seed)) * 1e9,
		Content: model.Root{Hash: hash.Sum([]byte(seed)), Size: uint64(len(seed)), Format: 1}}
}

// serialize is a tree's content in a canonical text form, one sorted line per entry.
func serialize(es map[string]tree.Entry) []byte {
	paths := make([]string, 0, len(es))
	for p := range es {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	var b bytes.Buffer
	for _, p := range paths {
		e := es[p]
		fmt.Fprintf(&b, "%s\t%o\t%d\t%s\t%d\t%d\t%d\n", p, e.Mode, e.ModTime, e.Content.Hash.String(), e.Content.Size, e.Content.Depth, e.Content.Format)
	}
	return b.Bytes()
}

func parse(t *testing.T, b []byte) map[string]tree.Entry {
	t.Helper()
	es := map[string]tree.Entry{}
	for _, line := range strings.Split(strings.TrimSuffix(string(b), "\n"), "\n") {
		if line == "" {
			continue
		}
		f := strings.Split(line, "\t")
		mode, _ := strconv.ParseUint(f[1], 8, 32)
		mtime, _ := strconv.ParseInt(f[2], 10, 64)
		h, _ := hash.Parse(f[3])
		size, _ := strconv.ParseUint(f[4], 10, 64)
		depth, _ := strconv.ParseUint(f[5], 10, 8)
		format, _ := strconv.ParseUint(f[6], 10, 16)
		es[f[0]] = tree.Entry{Mode: uint32(mode), ModTime: mtime, Content: model.Root{Hash: h, Size: size, Depth: uint8(depth), Format: uint16(format)}}
	}
	return es
}

func generate(seed uint64) map[string]tree.Entry {
	es := map[string]tree.Entry{}
	for i := 0; i < int(seed%40)+3; i++ {
		p := fmt.Sprintf("dir%d/file%d.txt", i%4, i)
		es[p] = file(fmt.Sprintf("%d/%d", seed, i))
	}
	return es
}

func subject(t *testing.T) contract.Subject {
	s := memstore.New()
	return contract.Subject{
		Model:    tree.Model{Config: cfg()},
		Store:    s,
		Generate: func(seed uint64) []byte { return serialize(generate(seed)) },
		Mutate: func(c []byte, seed uint64) []byte {
			es := parse(t, c)
			es[fmt.Sprintf("new/file%d", seed)] = file(fmt.Sprintf("new %d", seed))
			for p, e := range es {
				e.ModTime += int64(seed) + 1
				es[p] = e
				break
			}
			return serialize(es)
		},
		Write: func(t *testing.T, c []byte) model.Root {
			t.Helper()
			r, err := tree.Write(ctx, s, cfg(), parse(t, c))
			if err != nil {
				t.Fatal(err)
			}
			return r
		},
		Read: func(t *testing.T, r model.Root) []byte {
			t.Helper()
			es, err := tree.Read(ctx, s, cfg(), r)
			if err != nil {
				t.Fatal(err)
			}
			return serialize(es)
		},
	}
}

// Storage Core Spec: "Every registered model passes model/contract".
func TestContract(t *testing.T) { contract.Run(t, subject) }

// The record is the documented 55 bytes, laid out by hand here.
func TestAnEntryIsTheDocumented55Bytes(t *testing.T) {
	root := hash.Sum([]byte("content"))
	e := tree.Entry{Mode: 0o755, ModTime: -5, Content: model.Root{Hash: root, Size: 70000, Depth: 2, Format: 1}}
	want := binary.LittleEndian.AppendUint32(nil, 0o755)
	want = binary.LittleEndian.AppendUint64(want, uint64(0xfffffffffffffffb))
	want = binary.LittleEndian.AppendUint64(want, 70000)
	want = append(want, 2)
	want = binary.LittleEndian.AppendUint16(want, 1)
	want = append(want, root[:]...)
	if got := e.Encode(); !bytes.Equal(got, want) || len(got) != tree.EntrySize {
		t.Fatalf("Encode = %x (%d bytes), want %x", got, len(got), want)
	}
	if back, err := tree.DecodeEntry(want); err != nil || back != e {
		t.Fatalf("DecodeEntry = %+v, %v", back, err)
	}
	noFormat := bytes.Clone(want)
	noFormat[21], noFormat[22] = 0, 0
	for name, b := range map[string][]byte{"54 bytes": want[:54], "56 bytes": append(bytes.Clone(want), 0), "content format 0": noFormat} {
		if _, err := tree.DecodeEntry(b); !errors.Is(err, chunk.ErrCorrupt) {
			t.Errorf("%s: DecodeEntry = %v, want ErrCorrupt", name, err)
		}
	}
}

func write(t *testing.T, s chunk.ReadWriter, es map[string]tree.Entry) model.Root {
	t.Helper()
	r, err := tree.Write(ctx, s, cfg(), es)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func clone(es map[string]tree.Entry) map[string]tree.Entry {
	out := make(map[string]tree.Entry, len(es))
	for k, v := range es {
		out[k] = v
	}
	return out
}

func merge(t *testing.T, s chunk.ReadWriter, base, ours, theirs map[string]tree.Entry) (model.MergeResult, map[string]tree.Entry) {
	t.Helper()
	r, err := tree.Model{Config: cfg()}.Merge(ctx, write(t, s, base), write(t, s, ours), write(t, s, theirs), s)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Conflicts) > 0 {
		return r, nil
	}
	es, err := tree.Read(ctx, s, cfg(), r.Root)
	if err != nil {
		t.Fatal(err)
	}
	return r, es
}

func locations(cs []model.Conflict) []string {
	var out []string
	for _, c := range cs {
		out = append(out, string(c.Location))
	}
	sort.Strings(out)
	return out
}

// Storage Core Spec: "model/tree: branch A deletes x/, branch B edits x/y →
// delete-vs-edit conflict on x/y".
func TestDeletingADirectoryAgainstAnEditIsAConflict(t *testing.T) {
	s := memstore.New()
	base := map[string]tree.Entry{"x/y": file("y"), "x/z": file("z"), "keep": file("keep")}
	a := clone(base)
	delete(a, "x/y")
	delete(a, "x/z")
	b := clone(base)
	b["x/y"] = file("y edited")
	r, _ := merge(t, s, base, a, b)
	if got := locations(r.Conflicts); len(got) != 1 || got[0] != "x/y" {
		t.Fatalf("deleting x/ against editing x/y conflicted at %v, want exactly [x/y]", got)
	}
}

// Changes to different entries combine; one side's delete is taken.
func TestChangesToDifferentEntriesCombine(t *testing.T) {
	s := memstore.New()
	base := map[string]tree.Entry{"a": file("a"), "b": file("b"), "c": file("c")}
	ours, theirs := clone(base), clone(base)
	ours["a"] = file("a by ours")
	ours["new/ours"] = file("added by ours")
	theirs["b"] = file("b by theirs")
	delete(theirs, "c")
	theirs["new/theirs"] = file("added by theirs")
	r, got := merge(t, s, base, ours, theirs)
	if len(r.Conflicts) != 0 {
		t.Fatalf("changes to different entries conflicted at %v", locations(r.Conflicts))
	}
	want := map[string]tree.Entry{"a": file("a by ours"), "b": file("b by theirs"), "new/ours": file("added by ours"), "new/theirs": file("added by theirs")}
	if !bytes.Equal(serialize(got), serialize(want)) || r.Root.Size != 4 {
		t.Fatalf("the merged tree is\n%s(size %d), want\n%s", serialize(got), r.Root.Size, serialize(want))
	}
}

// The same change on both sides is taken once; different changes to one
// entry, and different adds of one path, conflict there.
func TestConflictsPerEntry(t *testing.T) {
	s := memstore.New()
	base := map[string]tree.Entry{"same": file("s"), "both": file("b"), "gone": file("g")}
	ours, theirs := clone(base), clone(base)
	ours["same"], theirs["same"] = file("s2"), file("s2")
	ours["both"], theirs["both"] = file("ours"), file("theirs")
	ours["added"], theirs["added"] = file("ours add"), file("theirs add")
	delete(ours, "gone")
	delete(theirs, "gone")
	r, _ := merge(t, s, base, ours, theirs)
	if got := locations(r.Conflicts); len(got) != 2 || got[0] != "added" || got[1] != "both" {
		t.Fatalf("conflicts at %v, want [added both]", got)
	}
}

// Validate refuses a tree whose root size is not its entry count.
func TestValidateChecksTheCount(t *testing.T) {
	s := memstore.New()
	r := write(t, s, generate(5))
	if err := (tree.Model{Config: cfg()}).Validate(ctx, r, s); err != nil {
		t.Fatalf("positive control: %v", err)
	}
	r.Size++
	if err := (tree.Model{Config: cfg()}).Validate(ctx, r, s); err == nil {
		t.Fatal("a tree whose root claims one entry too many validates")
	}
}

// A tree map holding a path outside the grammar (written past this package,
// straight into prolly) is refused when read and when merged from.
func TestPathsAreCheckedOnReadAndMerge(t *testing.T) {
	s := memstore.New()
	base := write(t, s, map[string]tree.Entry{"ok": file("ok")})
	m, err := prolly.Empty(ctx, s, cfg())
	if err != nil {
		t.Fatal(err)
	}
	e := m.Editor()
	for _, k := range []string{"ok", "a/../b"} {
		if err := e.Put([]byte(k), file(k).Encode()); err != nil {
			t.Fatal(err)
		}
	}
	if m, err = e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	bad := model.Root{Hash: m.Root(), Size: m.Count(), Format: tree.Format}
	if _, err := tree.Read(ctx, s, cfg(), bad); !errors.Is(err, chunk.ErrCorrupt) {
		t.Errorf("Read of a tree holding a/../b = %v, want ErrCorrupt", err)
	}
	if _, err := (tree.Model{Config: cfg()}).Merge(ctx, base, base, bad, s); !errors.Is(err, chunk.ErrCorrupt) {
		t.Errorf("merging in a tree holding a/../b = %v, want ErrCorrupt", err)
	}
}
