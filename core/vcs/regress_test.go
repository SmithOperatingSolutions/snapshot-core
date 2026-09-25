package vcs_test

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/auth"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/memstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/merge"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/vcs"
	"github.com/SmithOperatingSolutions/snapshot-core/core/wire"
)

// allocated reports the bytes f allocates on the heap, averaged over runs.
func allocated(runs int, f func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < runs; i++ {
		f()
	}
	runtime.ReadMemStats(&after)
	return (after.TotalAlloc - before.TotalAlloc) / uint64(runs)
}

// #26: decodeConflict kept looping over the model conflicts a record
// claims after its reader had failed, appending an empty one each time: a
// 6-byte record claiming 10,000 made 10,000 appends before the refusal. A
// truncated record must be refused for what its bytes hold.
func TestRegression_SC26_ATruncatedConflictIsRefusedWithoutFillingItsClaim(t *testing.T) {
	record := func(claimed, present int) []byte {
		var w wire.Writer
		w.U8(uint8(merge.BothChanged))
		w.U8(0)
		w.U8(0)
		w.U8(0) // no base, ours or theirs
		w.Uvarint(uint64(claimed))
		for i := 0; i < present; i++ {
			w.LenBytes(nil) // location
			w.LenBytes(nil) // reason
		}
		return w.Bytes()
	}
	// Positive control, at the limit: a record holding every model
	// conflict it claims decodes them all.
	full := record(vcs.MaxModelConflicts, vcs.MaxModelConflicts)
	if c, err := vcs.DecodeConflict("p", full); err != nil || len(c.Model) != vcs.MaxModelConflicts {
		t.Fatalf("positive control: a conflict record with %d model conflicts decoded %d, %v",
			vcs.MaxModelConflicts, len(c.Model), err)
	}

	truncated := record(vcs.MaxModelConflicts, 0)
	if _, err := vcs.DecodeConflict("p", truncated); !errors.Is(err, chunk.ErrCorrupt) {
		t.Fatalf("a %d-byte conflict record claiming %d model conflicts: %v, want ErrCorrupt", len(truncated), vcs.MaxModelConflicts, err)
	}
	const budget = 4 << 10
	if got := allocated(5, func() { _, _ = vcs.DecodeConflict("p", truncated) }); got > budget {
		t.Errorf("refusing a %d-byte conflict record claiming %d model conflicts allocated %d bytes (budget %d): "+
			"every stored conflict a working set lists costs its claim, not its bytes, to read or walk",
			len(truncated), vcs.MaxModelConflicts, got, budget)
	}
}

// reporting is a model whose every merge reports the conflicts it is given.
type reporting struct {
	lines
	cs []model.Conflict
}

func (reporting) ID() model.ID { return 10 }
func (m reporting) Merge(context.Context, model.Root, model.Root, model.Root, chunk.ReadWriter) (model.MergeResult, error) {
	return model.MergeResult{Conflicts: m.cs}, nil
}

// mergeReporting sets up a repository whose "doc" changed on main and on
// dev under a model that reports cs, and returns it with dev's head.
func mergeReporting(t *testing.T, cs []model.Conflict) (*fixture, hash.Hash) {
	t.Helper()
	clk := &clock{}
	f := &fixture{t: t, s: memstore.New(), clk: clk}
	f.o = options(t, auth.AllowAll{}, clk)
	reg, err := model.NewRegistry(lines{}, reporting{cs: cs})
	if err != nil {
		t.Fatal(err)
	}
	f.o.Registry = reg
	if f.r, err = vcs.Init(ctx, f.s, alice, f.o); err != nil {
		t.Fatal(err)
	}
	f.put(vcs.MainBranch, "doc", f.obj(10, "base"))
	f.commit(vcs.MainBranch, "base")
	f.branchFrom("dev")
	f.put("dev", "doc", f.obj(10, "dev"))
	theirs := f.commit("dev", "dev work")
	f.put(vcs.MainBranch, "doc", f.obj(10, "main"))
	f.commit(vcs.MainBranch, "main work")
	return f, theirs.Hash
}

func conflicts(n int, location, reason string) []model.Conflict {
	cs := make([]model.Conflict, n)
	for i := range cs {
		cs[i] = model.Conflict{Location: []byte(location), Reason: reason}
	}
	return cs
}

// #26: a conflict record holds at most 10,000 model conflicts, each with a
// location of at most 4,096 bytes and a reason of at most 1,024, and its
// decoder refuses anything larger; but Merge wrote whatever a model
// reported. A model over any limit merged cleanly, and the branch's merge
// state then did not read: its conflicts could be neither listed nor
// resolved, and nothing but abandoning the merge moved the branch on. Such
// a merge must be refused before it writes anything.
func TestRegression_SC26_AConflictTooLargeToRecordRefusesTheMerge(t *testing.T) {
	// Positive controls, at exactly each limit: the merge records the
	// conflict and it reads back as the model reported it.
	for label, cs := range map[string][]model.Conflict{
		"a 4,096-byte location and a 1,024-byte reason": conflicts(1, strings.Repeat("l", 4096), strings.Repeat("r", 1024)),
		"10,000 model conflicts":                        conflicts(10_000, "l", "r"),
	} {
		f, theirs := mergeReporting(t, cs)
		if _, err := f.r.Merge(ctx, alice, vcs.MainBranch, theirs); err != nil {
			t.Fatalf("positive control, %s: the merge failed: %v", label, err)
		}
		got, err := f.r.Conflicts(ctx, alice, vcs.MainBranch)
		if err != nil || len(got) != 1 || len(got[0].Model) != len(cs) ||
			!bytes.Equal(got[0].Model[0].Location, cs[0].Location) || got[0].Model[0].Reason != cs[0].Reason {
			t.Fatalf("positive control, %s: the recorded conflicts read back as %d paths (%v), want one with %d model conflicts",
				label, len(got), err, len(cs))
		}
	}

	for label, cs := range map[string][]model.Conflict{
		"a 4,097-byte location":  conflicts(1, strings.Repeat("l", 4097), "r"),
		"a 1,025-byte reason":    conflicts(1, "l", strings.Repeat("r", 1025)),
		"10,001 model conflicts": conflicts(10_001, "l", "r"),
	} {
		f, theirs := mergeReporting(t, cs)
		root, err := f.s.Root(ctx)
		if err != nil {
			t.Fatal(err)
		}
		before, err := f.r.WorkingSet(ctx, alice, vcs.MainBranch)
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.r.Merge(ctx, alice, vcs.MainBranch, theirs)
		after, werr := f.r.WorkingSet(ctx, alice, vcs.MainBranch)
		rootAfter, rerr := f.s.Root(ctx)
		if !errors.Is(err, vcs.ErrConflictTooLarge) || werr != nil || rerr != nil || after != before || rootAfter != root {
			_, cerr := f.r.Conflicts(ctx, alice, vcs.MainBranch)
			t.Errorf("a model reporting %s: Merge = %v (want ErrConflictTooLarge), working set changed: %v, "+
				"root changed: %v; the conflicts then read as %v: a merge recording a conflict its own decoder "+
				"refuses leaves a branch whose conflicts can be neither listed nor resolved",
				label, err, after != before, rootAfter != root, cerr)
		}
	}
}
