package e2e_test

import (
	"context"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/auth"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/local"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/s3"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/s3/s3fake"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/split"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/object"
	"github.com/SmithOperatingSolutions/snapshot-core/core/repo"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
	"github.com/SmithOperatingSolutions/snapshot-core/core/vcs"
	"github.com/SmithOperatingSolutions/snapshot-core/model/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/model/tree"
)

// A repository on an S3 endpoint that ignores conditional writes, as
// Backblaze B2 is reported to (#8): its objects there, opened objects only,
// its root on a local disk, and a copy of the root kept on the endpoint.
// Files are written, committed, branched and merged, GC collects, and when
// the disk holding the root is lost, the repository comes back from the
// copy and reads whole.
func TestARepositoryRunsOnAnEndpointWithoutConditionalWrites(t *testing.T) {
	ctx := context.Background()
	srv := s3fake.New()
	t.Cleanup(srv.Close)
	srv.CreateBucket("b2-like")
	srv.IgnoreIfMatch(true)
	srv.IgnoreIfNoneMatch(true)
	client := s3.NewClient(srv.URL(), "us-east-1", "key", "secret")
	objects, err := s3.Open(ctx, s3.Options{Client: client, Bucket: "b2-like", Prefix: "repo/", AllowHTTP: true, ObjectsOnly: true})
	if err != nil {
		t.Fatalf("opening the endpoint objects only: %v", err)
	}
	dir := t.TempDir()
	roots, err := local.Create(filepath.Join(dir, "roots"), local.Options{})
	if err != nil {
		t.Fatal(err)
	}
	store, err := split.New(split.Options{Objects: objects, Roots: roots})
	if err != nil {
		t.Fatal(err)
	}
	keys, err := seal.NewKeyring()
	if err != nil {
		t.Fatal(err)
	}
	models, err := model.NewRegistry(blob.Model{}, tree.Model{Config: repo.DefaultGeometry().Prolly()})
	if err != nil {
		t.Fatal(err)
	}
	me := auth.Principal{ID: "user:me"}
	o := repo.Options{Blobs: store, Keys: keys, Registry: models, Authorizer: auth.AllowAll{}}
	r, err := repo.Init(ctx, me, o)
	if err != nil {
		t.Fatalf("Init on the split store: %v", err)
	}
	write := func(r *repo.Repo, branch, path, content string) {
		t.Helper()
		root, err := blob.Write(ctx, r.Chunks(), strings.NewReader(content), r.Config.Geometry.Stream())
		if err != nil {
			t.Fatal(err)
		}
		ws, err := r.WorkingSet(ctx, me, branch)
		if err != nil {
			t.Fatal(err)
		}
		n, err := r.Namespace(ctx, ws.Working)
		if err != nil {
			t.Fatal(err)
		}
		e := n.Editor()
		if err := e.Put(path, object.Ref{Model: blob.ID, Root: root}); err != nil {
			t.Fatal(err)
		}
		if n, err = e.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		next := ws
		next.Working, next.Staged = n.Root(), n.Root()
		if _, err := r.UpdateWorkingSet(ctx, me, branch, ws, next); err != nil {
			t.Fatal(err)
		}
		if _, err := r.CommitWorkingSet(ctx, me, branch, "write "+path); err != nil {
			t.Fatal(err)
		}
	}
	write(r, vcs.MainBranch, "notes/hello.txt", "hello\n")
	first, err := r.Head(ctx, me, vcs.MainBranch)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.CreateBranch(ctx, me, "draft", first.Hash); err != nil {
		t.Fatal(err)
	}
	write(r, "draft", "notes/hello.txt", "hello, world\n")
	write(r, vcs.MainBranch, "notes/other.txt", "another note\n")
	draft, err := r.Head(ctx, me, "draft")
	if err != nil {
		t.Fatal(err)
	}
	if res, err := r.Merge(ctx, me, vcs.MainBranch, draft.Hash); err != nil || len(res.Conflicts) != 0 {
		t.Fatalf("merging draft: %d conflicts, %v", len(res.Conflicts), err)
	}
	merged, err := r.CommitWorkingSet(ctx, me, vcs.MainBranch, "merge draft")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.DeleteBranch(ctx, me, "draft"); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.GC(ctx, me, o, 7*24*time.Hour); err != nil {
		t.Fatalf("GC on the split store: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// The disk holding the root is lost: a fresh root store, seeded from
	// the copy on the endpoint, opens the repository at its last commit.
	fresh, err := local.Create(filepath.Join(dir, "roots-again"), local.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := split.Recover(ctx, objects, fresh); err != nil {
		t.Fatalf("recovering the root from its copy: %v", err)
	}
	recovered, err := split.New(split.Options{Objects: objects, Roots: fresh})
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	o.Blobs = recovered
	re, err := repo.Open(ctx, o)
	if err != nil {
		t.Fatalf("opening the recovered repository: %v", err)
	}
	defer re.Close()
	head, err := re.Head(ctx, me, vcs.MainBranch)
	if err != nil || head.Hash != merged.Hash {
		t.Fatalf("the recovered repository's main is %v (%v), want the merge %s", head.Hash, err, merged.Hash.Short())
	}
	n, err := re.Namespace(ctx, head.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]string{"notes/hello.txt": "hello, world\n", "notes/other.txt": "another note\n"} {
		ref, _, ok, err := n.Get(ctx, path)
		if err != nil || !ok {
			t.Fatalf("%s: %v, %v", path, ok, err)
		}
		rd, err := blob.Open(ctx, re.Chunks(), ref.Root)
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(rd)
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != want {
			t.Fatalf("%s reads %q after recovery, want %q", path, b, want)
		}
	}
	write(re, vcs.MainBranch, "notes/after.txt", "written after recovery\n")
}
