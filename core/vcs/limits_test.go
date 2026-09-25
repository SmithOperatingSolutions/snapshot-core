package vcs_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/auth"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/memstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
	"github.com/SmithOperatingSolutions/snapshot-core/core/vcs"
)

// forgeRef sets key in the refs map at the store's root to val, written by
// a map that takes values of up to 64 MiB, and swaps it in: a refs map no
// repository writes, as a damaged or hostile store can hold one.
func forgeRef(t *testing.T, f *fixture, key string, val []byte) {
	t.Helper()
	root, err := f.s.Root(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c := f.o.Config
	c.MaxValue = 64 << 20
	m, err := prolly.Open(ctx, f.s, c, root)
	if err != nil {
		t.Fatal(err)
	}
	e := m.Editor()
	if err := e.Put([]byte(key), val); err != nil {
		t.Fatal(err)
	}
	if m, err = e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.s.CompareAndSetRoot(ctx, root, m.Root()); err != nil {
		t.Fatal(err)
	}
}

// walkAll is GC's mark over the repository at the store's root.
func walkAll(f *fixture) error {
	root, err := f.s.Root(ctx)
	if err != nil {
		return err
	}
	return vcs.Walk(ctx, f.s, f.o, root, func(hash.Hash, bool) (bool, error) { return true, nil })
}

// A refs map's every value is a hash, 32 bytes. Reading a branch head,
// listing branches or walking the repository (GC's mark) where a ref holds
// a 4 MiB stream must refuse it without reading it; and, in a repository
// whose every value is a stream (inline limit 0), a ref of 33 bytes is
// refused while one of exactly 32 reads.
func TestTheRefsMapReadsNoValueLongerThanAHash(t *testing.T) {
	streamed := prolly.DefaultConfig()
	streamed.InlineLimit = 0
	for _, tc := range []struct {
		name   string
		config prolly.Config
		size   int
	}{
		{"a 4 MiB ref", prolly.DefaultConfig(), 4 << 20},
		{"a 33-byte ref, stored as a stream", streamed, hash.Size + 1},
	} {
		clk := &clock{}
		f := &fixture{t: t, s: memstore.New(), clk: clk}
		f.o = options(t, auth.AllowAll{}, clk)
		f.o.Config = tc.config
		var err error
		if f.r, err = vcs.Init(ctx, f.s, alice, f.o); err != nil {
			t.Fatalf("%s: Init: %v", tc.name, err)
		}
		reads := map[string]func() error{
			"Head": func() error { _, err := f.r.Head(ctx, alice, vcs.MainBranch); return err },
			"Branches": func() error {
				_, err := f.r.Branches(ctx, alice)
				return err
			},
			"Walk": func() error { return walkAll(f) },
		}
		// Positive control, at exactly the limit: every ref the repository
		// wrote is a 32-byte hash, and each read takes it.
		for how, read := range reads {
			if err := read(); err != nil {
				t.Fatalf("%s: positive control: %s of an honest repository: %v", tc.name, how, err)
			}
		}
		forgeRef(t, f, "heads/main", bytes.Repeat([]byte{0xA5}, tc.size))
		for how, read := range reads {
			used := allocated(1, func() { err = read() })
			if !errors.Is(err, prolly.ErrValueTooLarge) || used > 512<<10 {
				t.Errorf("%s: %s allocated %d bytes (%v); want ErrValueTooLarge within 512 KiB: a ref is a hash, "+
					"and a longer one is read whole before it is refused", tc.name, how, used, err)
			}
		}
	}
}

// The largest conflict record Merge writes (#27's limits: 10,000 model
// conflicts, each located in 4,096 bytes with a 1,024-byte reason, and all
// three sides) is 51,240,144 bytes. It is recorded and reads back; a
// record one byte longer, which no repository writes, is refused by
// listing the conflicts, resolving one and walking the repository (GC's
// mark), without being read.
func TestAConflictRecordIsReadUpToTheLargestMergeWrites(t *testing.T) {
	const largest = 51_240_144
	f, theirs := mergeReporting(t, conflicts(vcs.MaxModelConflicts, strings.Repeat("l", 4096), strings.Repeat("r", 1024)))
	if _, err := f.r.Merge(ctx, alice, vcs.MainBranch, theirs); err != nil {
		t.Fatalf("positive control: a merge recording the largest conflict: %v", err)
	}
	got, err := f.r.Conflicts(ctx, alice, vcs.MainBranch)
	if err != nil || len(got) != 1 || len(got[0].Model) != vcs.MaxModelConflicts {
		t.Fatalf("positive control: the largest conflict read back as %d paths (%v), want one with %d model conflicts",
			len(got), err, vcs.MaxModelConflicts)
	}
	record := vcs.EncodeConflict(got[0])
	if len(record) != largest {
		t.Fatalf("fixture: the conflict's record is %d bytes, want %d, the largest Merge writes: the control would not test the limit", len(record), largest)
	}
	if err := walkAll(f); err != nil {
		t.Fatalf("positive control: walking a repository recording the largest conflict: %v", err)
	}

	ws, err := f.r.WorkingSet(ctx, alice, vcs.MainBranch)
	if err != nil {
		t.Fatal(err)
	}
	cm, err := prolly.Open(ctx, f.s, prolly.DefaultConfig(), ws.Merge.Conflicts) // takes up to 64 MiB
	if err != nil {
		t.Fatal(err)
	}
	e := cm.Editor()
	if err := e.Put([]byte(got[0].Path), append(record, 0)); err != nil {
		t.Fatal(err)
	}
	if cm, err = e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	merging := *ws.Merge
	merging.Conflicts = cm.Root()
	ws.Merge = &merging
	h, err := f.s.Put(ctx, vcs.EncodeWorkingSet(ws))
	if err != nil {
		t.Fatal(err)
	}
	forgeRef(t, f, "work/"+vcs.MainBranch, h[:])

	// In order: ResolveConflict last, since a resolve that took the record
	// would drop it.
	for _, r := range []struct {
		how  string
		read func() error
	}{
		{"Conflicts", func() error { _, err := f.r.Conflicts(ctx, alice, vcs.MainBranch); return err }},
		{"Walk", func() error { return walkAll(f) }},
		{"ResolveConflict", func() error { return f.r.ResolveConflict(ctx, alice, vcs.MainBranch, "doc", nil) }},
	} {
		how, read := r.how, r.read
		used := allocated(1, func() { err = read() })
		if !errors.Is(err, prolly.ErrValueTooLarge) || used > 1<<20 {
			t.Errorf("%s of a %d-byte conflict record allocated %d bytes (%v); want ErrValueTooLarge within 1 MiB: "+
				"a record longer than any Merge writes is read whole before it is refused", how, largest+1, used, err)
		}
	}
}
