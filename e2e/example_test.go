package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/auth"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/local"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/object"
	"github.com/SmithOperatingSolutions/snapshot-core/core/repo"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
	"github.com/SmithOperatingSolutions/snapshot-core/core/vcs"
	"github.com/SmithOperatingSolutions/snapshot-core/model/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/model/tree"
)

// Example is a host using the storage core end to end, as the README walks
// through it: a repository on a local disk, a file written and committed, a
// branch, a merge, the file read back, and a collection. It runs with the
// tests, so the walkthrough keeps compiling and keeps working.
func Example() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "snapshot-core-example-*")
	must(err)
	defer os.RemoveAll(dir)

	// A backend, a master key, the data models, and who is asking.
	store, err := local.Create(filepath.Join(dir, "repo"), local.Options{})
	must(err)
	keys, err := seal.NewKeyring() // keep it with seal.NewKeyFile or seal.WrapKeyring, never beside the data
	must(err)
	models, err := model.NewRegistry(blob.Model{}, tree.Model{Config: repo.DefaultGeometry().Prolly()})
	must(err)
	me := auth.Principal{ID: "user:me"}
	o := repo.Options{Blobs: store, Keys: keys, Registry: models, Authorizer: auth.AllowAll{}}

	// A new repository: branch main, at an empty first commit.
	r, err := repo.Init(ctx, me, o)
	must(err)

	// Write a file, stage it at a path, commit.
	stage(ctx, r, me, vcs.MainBranch, "notes/hello.txt", "hello\n")
	first, err := r.CommitWorkingSet(ctx, me, vcs.MainBranch, "add a note")
	must(err)

	// Branch, change the file there, and merge the branch back.
	must(r.CreateBranch(ctx, me, "draft", first.Hash))
	stage(ctx, r, me, "draft", "notes/hello.txt", "hello, world\n")
	draft, err := r.CommitWorkingSet(ctx, me, "draft", "reword the note")
	must(err)
	merged, err := r.Merge(ctx, me, vcs.MainBranch, draft.Hash)
	must(err)
	head, err := r.CommitWorkingSet(ctx, me, vcs.MainBranch, "merge draft")
	must(err)
	fmt.Println("conflicts:", len(merged.Conflicts), "parents:", len(head.Parents))

	// Read the file back from main.
	fmt.Print(readFile(ctx, r, me, vcs.MainBranch, "notes/hello.txt"))

	// Collect, with admin and the raw store: packs nothing reaches are
	// condemned now and deleted by a run a grace window later.
	must(r.DeleteBranch(ctx, me, "draft"))
	must(r.Close())
	report, err := repo.GC(ctx, me, o, 7*24*time.Hour)
	must(err)
	fmt.Println("deleted by the first run:", len(report.Deleted))

	// Output:
	// conflicts: 0 parents: 2
	// hello, world
	// deleted by the first run: 0
}

// stage writes content as a blob and puts it at path in branch's working
// set. A conflict (another writer, or GC) means read again and write again.
func stage(ctx context.Context, r *repo.Repo, p auth.Principal, branch, path, content string) {
	for {
		root, err := blob.Write(ctx, r.Chunks(), strings.NewReader(content), r.Config.Geometry.Stream())
		must(err)
		ws, err := r.WorkingSet(ctx, p, branch)
		must(err)
		n, err := r.Namespace(ctx, ws.Working)
		must(err)
		e := n.Editor()
		must(e.Put(path, object.Ref{Model: blob.ID, Root: root}))
		n, err = e.Flush(ctx)
		must(err)
		next := ws
		next.Working, next.Staged = n.Root(), n.Root()
		if _, err = r.UpdateWorkingSet(ctx, p, branch, ws, next); !errors.Is(err, vcs.ErrConflict) {
			must(err)
			return
		}
	}
}

// readFile reads the file at path in branch's head commit.
func readFile(ctx context.Context, r *repo.Repo, p auth.Principal, branch, path string) string {
	head, err := r.Head(ctx, p, branch)
	must(err)
	n, err := r.Namespace(ctx, head.Namespace)
	must(err)
	ref, _, ok, err := n.Get(ctx, path)
	must(err)
	if !ok {
		panic("no " + path)
	}
	rd, err := blob.Open(ctx, r.Chunks(), ref.Root)
	must(err)
	b, err := io.ReadAll(rd)
	must(err)
	return string(b)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
