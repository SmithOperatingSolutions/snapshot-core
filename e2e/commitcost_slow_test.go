//go:build slow

package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/auth"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/mem"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/object"
	"github.com/SmithOperatingSolutions/snapshot-core/core/repo"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
	"github.com/SmithOperatingSolutions/snapshot-core/core/vcs"
	"github.com/SmithOperatingSolutions/snapshot-core/model/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/model/tree"
)

// #43: a host that cannot batch (an interactive session committing per
// statement) commits once per object, and then the commit is its ceiling.
// On tools/commitbench's blob/mem run that cost about 570 µs, 38% of the
// CPU the namespace diffs the per-path authorization ran; CommitNamespace
// asks by what the flush changed and a map holds its root, and it costs
// about 420 µs there, about 510 µs here (the object write included). This
// is a regression guard, not a red: the bar, 2 ms a commit, is four times
// what the i7-1360P this was measured on takes, so a slower machine
// passes and a commit that grows with the namespace (a walk or a full
// diff of its 20,000 paths) fails. It cannot tell the diffs' 20% from a
// bad day; the reads are held exactly by
// TestCommitNamespaceReadsNoNamespaceNode and
// TestTheRootIsReadOnceWhenTheMapIsMade.
func TestSlowACommitPerObjectCostsLittle(t *testing.T) {
	const base, commits = 20000, 2000
	const bar = 2 * time.Millisecond
	ctx := context.Background()
	keys, err := seal.NewKeyring()
	if err != nil {
		t.Fatal(err)
	}
	models, err := model.NewRegistry(blob.Model{}, tree.Model{Config: repo.DefaultGeometry().Prolly()})
	if err != nil {
		t.Fatal(err)
	}
	me := auth.Principal{ID: "user:me"}
	r, err := repo.Init(ctx, me, repo.Options{Blobs: mem.New(), Keys: keys, Registry: models, Authorizer: auth.AllowAll{}})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	geo := r.Config.Geometry.Stream()
	write := func(i int) object.Ref {
		root, err := blob.Write(ctx, r.Chunks(), bytes.NewReader([]byte(fmt.Sprintf("object %08d", i))), geo)
		if err != nil {
			t.Fatal(err)
		}
		return object.Ref{Model: blob.ID, Root: root}
	}
	// A namespace of some size first, so each commit edits a tree of a
	// few levels, as a host's would.
	ws, err := r.WorkingSet(ctx, me, vcs.MainBranch)
	if err != nil {
		t.Fatal(err)
	}
	n, err := r.Namespace(ctx, ws.Working)
	if err != nil {
		t.Fatal(err)
	}
	e := n.Editor()
	for i := range base {
		if err := e.Put(fmt.Sprintf("base/%06d", i), write(i)); err != nil {
			t.Fatal(err)
		}
	}
	if n, err = e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := r.CommitNamespace(ctx, me, vcs.MainBranch, ws, n, "base"); err != nil {
		t.Fatal(err)
	}
	commitOne := func(i int) {
		ws, err := r.WorkingSet(ctx, me, vcs.MainBranch)
		if err != nil {
			t.Fatal(err)
		}
		n, err := r.Namespace(ctx, ws.Working)
		if err != nil {
			t.Fatal(err)
		}
		e := n.Editor()
		if err := e.Put(fmt.Sprintf("obj/%06d", i), write(base+i)); err != nil {
			t.Fatal(err)
		}
		if n, err = e.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := r.CommitNamespace(ctx, me, vcs.MainBranch, ws, n, "put"); err != nil {
			t.Fatal(err)
		}
	}
	t0 := time.Now()
	for i := range commits {
		commitOne(i)
	}
	el := time.Since(t0)
	per := el / commits
	t.Logf("%d commits of one object each onto %d objects in %v: %v a commit, %.0f a second", commits, base, el.Round(time.Millisecond), per.Round(time.Microsecond), commits/el.Seconds())

	// What was committed is every commit, each holding its object.
	head, err := r.Head(ctx, me, vcs.MainBranch)
	if err != nil {
		t.Fatal(err)
	}
	if head.Height != commits+1 {
		t.Fatalf("the head is at height %d, want %d: a commit is missing", head.Height, commits+1)
	}
	if n, err = r.Namespace(ctx, head.Namespace); err != nil {
		t.Fatal(err)
	}
	if n.Count() != base+commits {
		t.Fatalf("the head's namespace holds %d objects, want %d", n.Count(), base+commits)
	}
	if per > bar {
		t.Fatalf("a commit of one object took %v, over the %v bar (about 510 µs when #43 closed): a host committing per statement is back to its old ceiling", per.Round(time.Microsecond), bar)
	}
}
