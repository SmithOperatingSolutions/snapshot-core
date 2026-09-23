//go:build slow

package merge_test

import (
	"fmt"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/merge"
	"github.com/SmithOperatingSolutions/snapshot-core/core/object"
)

// Engine Spec L3 at its own scale: "Merging 1M-row tables with 10 changed
// rows reads fewer than 1,000 nodes". The normal suite merges 100,000 paths.
func TestSlowMergingA1MNamespaceReadsLittle(t *testing.T) {
	const million = 1_000_000
	f := newFixture(t)
	path := func(i int) string { return fmt.Sprintf("dir%03d/file%07d", i%1000, i) }
	x := f.obj(7, "x")
	all := make(map[string]object.Ref, million)
	for i := range million {
		all[path(i)] = x
	}
	base := f.ns(all)
	side := func(name string, first int) *object.Namespace {
		e := base.Editor()
		for i := range 10 {
			if err := e.Put(path((first+i*99991)%million), f.obj(7, name, fmt.Sprint(i))); err != nil {
				t.Fatal(err)
			}
		}
		n, err := e.Flush(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	ours, theirs := side("ours", 1), side("theirs", million/2)
	f.s.gets.Store(0)
	r, err := merge.Merge(ctx, f.reg, base, ours, theirs, f.s, merge.Options{})
	if err != nil || len(r.Conflicts) != 0 {
		t.Fatalf("merge: %d conflicts, %v", len(r.Conflicts), err)
	}
	if n := f.s.gets.Load(); n >= 1000 {
		t.Fatalf("merging ten changes a side into 1,000,000 paths read %d nodes, want fewer than 1,000", n)
	}
	if r.Stats.Modified != 10 || r.Merged.Count() != million {
		t.Fatalf("the merge applied %d of theirs' changes over %d paths, want 10 over %d", r.Stats.Modified, r.Merged.Count(), million)
	}
}
