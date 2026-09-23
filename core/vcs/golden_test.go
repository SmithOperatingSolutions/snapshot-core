package vcs_test

import (
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/object"
	"github.com/SmithOperatingSolutions/snapshot-core/core/vcs"
)

// Engine Spec L2: "Golden test: a fixed sequence of commits yields a fixed
// head hash". The clock, principal and edits are fixed, so the head and the
// store's root are too: a change to how commits, working sets, namespaces or
// refs are encoded changes them, even one the encoder and decoder make
// together, which every round trip would miss.
func TestAFixedHistoryHasAFixedHead(t *testing.T) {
	f := newFixture(t)
	f.put(vcs.MainBranch, "files/a", f.obj(7, "a"))
	f.commit(vcs.MainBranch, "one")
	f.branchFrom("dev")
	f.put("dev", "files/b", f.obj(7, "b"))
	dev := f.commit("dev", "two")
	f.edit(alice, vcs.MainBranch, map[string]*object.Ref{"files/a": ptr(f.obj(7, "a", "c"))})
	f.commit(vcs.MainBranch, "three")
	if _, err := f.r.Merge(ctx, alice, vcs.MainBranch, dev.Hash); err != nil {
		t.Fatal(err)
	}
	head := f.commit(vcs.MainBranch, "merge dev")
	root, err := f.s.Root(ctx)
	if err != nil {
		t.Fatal(err)
	}
	const wantHead, wantRoot = "c0adecca7dce005d30a9d5bc076ec76397059a759002bd21b84245b7bc2bdb45", "2211ff7e664ca339f7a4ab0bd1ce2ac05776760ef49843967475e47655d38031"
	if head.Hash.String() != wantHead || root.String() != wantRoot {
		t.Fatalf("a fixed history ended at head %s, root %s; want %s, %s", head.Hash, root, wantHead, wantRoot)
	}
}
