package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/auth"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/local"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/object"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
	"github.com/SmithOperatingSolutions/snapshot-core/core/repo"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
	"github.com/SmithOperatingSolutions/snapshot-core/core/vcs"
	"github.com/SmithOperatingSolutions/snapshot-core/model/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/model/tree"
)

var (
	ctx = context.Background()
	me  = auth.Principal{ID: "user:e2e"}
)

// host drives a repository the way an application would: files in, folders
// as trees, commits, branches, merges.
type host struct {
	t *testing.T
	o repo.Options
	r *repo.Repo
}

func open(t *testing.T, dir string, keys *seal.Keyring, create bool) *host {
	t.Helper()
	return openWith(t, dir, keys, create, repo.Geometry{})
}

// openWith is open, creating the repository with geometry g (the zero
// Geometry: the default).
func openWith(t *testing.T, dir string, keys *seal.Keyring, create bool, g repo.Geometry) *host {
	t.Helper()
	store, err := func() (*local.Store, error) {
		if create {
			return local.Create(dir, local.Options{})
		}
		return local.Open(dir, local.Options{})
	}()
	if err != nil {
		t.Fatal(err)
	}
	reg, err := model.NewRegistry(blob.Model{}, tree.Model{Config: repo.DefaultGeometry().Prolly()})
	if err != nil {
		t.Fatal(err)
	}
	h := &host{t: t, o: repo.Options{Blobs: store, Keys: keys, Registry: reg, Authorizer: auth.AllowAll{},
		Clock: func() time.Time { return time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC) }, Geometry: g}}
	if create {
		h.r, err = repo.Init(ctx, me, h.o)
	} else {
		h.r, err = repo.Open(ctx, h.o)
	}
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func (h *host) prolly() prolly.Config { return h.r.Config.Geometry.Prolly() }

// folder writes files as blobs and a tree of them, returning the tree.
func (h *host) folder(files map[string]string) model.Root {
	h.t.Helper()
	entries := map[string]tree.Entry{}
	for p, content := range files {
		root, err := blob.Write(ctx, h.r.Chunks(), bytes.NewReader([]byte(content)), h.r.Config.Geometry.Stream())
		if err != nil {
			h.t.Fatal(err)
		}
		entries[p] = tree.Entry{Mode: 0o644, ModTime: 1, Content: root}
	}
	root, err := tree.Write(ctx, h.r.Chunks(), h.prolly(), entries)
	if err != nil {
		h.t.Fatal(err)
	}
	return root
}

func (h *host) setFolder(branch string, files map[string]string) {
	h.t.Helper()
	root := h.folder(files)
	ws, err := h.r.WorkingSet(ctx, me, branch)
	if err != nil {
		h.t.Fatal(err)
	}
	n, err := h.r.Namespace(ctx, ws.Working)
	if err != nil {
		h.t.Fatal(err)
	}
	e := n.Editor()
	if err := e.Put("project", object.Ref{Model: tree.ID, Root: root}); err != nil {
		h.t.Fatal(err)
	}
	if n, err = e.Flush(ctx); err != nil {
		h.t.Fatal(err)
	}
	next := ws
	next.Working, next.Staged = n.Root(), n.Root()
	if _, err := h.r.UpdateWorkingSet(ctx, me, branch, ws, next); err != nil {
		h.t.Fatal(err)
	}
}

func (h *host) commit(branch, msg string) vcs.Commit {
	h.t.Helper()
	c, err := h.r.CommitWorkingSet(ctx, me, branch, msg)
	if err != nil {
		h.t.Fatalf("commit on %s: %v", branch, err)
	}
	return c
}

// files reads the folder at a commit back into path -> content.
func (h *host) files(c vcs.Commit) map[string]string {
	h.t.Helper()
	n, err := h.r.Namespace(ctx, c.Namespace)
	if err != nil {
		h.t.Fatal(err)
	}
	ref, _, ok, err := n.Get(ctx, "project")
	if err != nil || !ok {
		h.t.Fatalf("project at %s: %v %v", c.Hash.Short(), ok, err)
	}
	entries, err := tree.Read(ctx, h.r.Chunks(), h.prolly(), ref.Root)
	if err != nil {
		h.t.Fatal(err)
	}
	out := map[string]string{}
	for p, e := range entries {
		rd, err := blob.Open(ctx, h.r.Chunks(), e.Content)
		if err != nil {
			h.t.Fatal(err)
		}
		b, err := io.ReadAll(rd)
		if err != nil {
			h.t.Fatal(err)
		}
		out[p] = string(b)
	}
	return out
}

func same(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// The C3 exit criterion: "A folder of files branches, diffs and merges end
// to end", on a disk store, through encrypted packs, and back after reopening.
func TestAFolderBranchesDiffsAndMerges(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "repo")
	keys, err := seal.NewKeyring()
	if err != nil {
		t.Fatal(err)
	}
	h := open(t, dir, keys, true)
	base := map[string]string{"README.md": "# project\n", "src/main.go": "package main\n", "docs/guide.md": "Read me first.\n"}
	h.setFolder(vcs.MainBranch, base)
	imported := h.commit(vcs.MainBranch, "import the folder")
	if err := h.r.CreateBranch(ctx, me, "feature", imported.Hash); err != nil {
		t.Fatal(err)
	}

	feature := map[string]string{"README.md": base["README.md"], "docs/guide.md": base["docs/guide.md"],
		"src/main.go": "package main\n\nfunc main() {}\n", "src/util.go": "package main\n"}
	h.setFolder("feature", feature)
	theirs := h.commit("feature", "work on the code")
	mainSide := map[string]string{"src/main.go": base["src/main.go"], "docs/guide.md": "Read me first, then the code.\n"}
	h.setFolder(vcs.MainBranch, mainSide)
	h.commit(vcs.MainBranch, "work on the docs")

	// Diff: the folder changed, and the tree model says which files.
	from, err := h.r.Namespace(ctx, imported.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	to, err := h.r.Namespace(ctx, theirs.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	d, err := object.Diff(ctx, from, to)
	if err != nil {
		t.Fatal(err)
	}
	c, ok, err := d.Next()
	if err != nil || !ok || c.Path != "project" || c.Kind != prolly.Modified {
		t.Fatalf("the namespace diff is %+v, %v, %v; want project modified", c, ok, err)
	}
	detail, err := d.Detail(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	var changed []string
	for {
		ch, ok, err := detail.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		changed = append(changed, fmt.Sprintf("%d %s", ch.Kind, ch.Location))
	}
	sort.Strings(changed)
	if fmt.Sprint(changed) != fmt.Sprint([]string{"1 src/util.go", "3 src/main.go"}) {
		t.Fatalf("the folder's diff is %v, want src/util.go added and src/main.go modified", changed)
	}

	// Merge: different files changed on each side combine, with no conflict.
	res, err := h.r.Merge(ctx, me, vcs.MainBranch, theirs.Hash)
	if err != nil || len(res.Conflicts) != 0 {
		t.Fatalf("merging feature into main: %d conflicts, %v", len(res.Conflicts), err)
	}
	merged := h.commit(vcs.MainBranch, "merge feature")
	want := map[string]string{"src/main.go": feature["src/main.go"], "src/util.go": feature["src/util.go"], "docs/guide.md": mainSide["docs/guide.md"]}
	if got := h.files(merged); !same(got, want) || len(merged.Parents) != 2 {
		t.Fatalf("after the merge the folder holds %v (parents %d), want %v", got, len(merged.Parents), want)
	}

	// Reopened from disk with the same key: the history and the files are there.
	if err := h.r.Close(); err != nil {
		t.Fatal(err)
	}
	h = open(t, dir, keys, false)
	head, err := h.r.Head(ctx, me, vcs.MainBranch)
	if err != nil || head.Hash != merged.Hash || !same(h.files(head), want) {
		t.Fatalf("reopened, main is %v (%v)", head.Hash, err)
	}

	// Both sides editing one file conflict at that file, inside the folder.
	h.setFolder(vcs.MainBranch, map[string]string{"src/main.go": "main's version\n", "src/util.go": want["src/util.go"], "docs/guide.md": want["docs/guide.md"]})
	h.commit(vcs.MainBranch, "main edits main.go")
	if err := h.r.CreateBranch(ctx, me, "other", merged.Hash); err != nil {
		t.Fatal(err)
	}
	h.setFolder("other", map[string]string{"src/main.go": "other's version\n", "src/util.go": want["src/util.go"], "docs/guide.md": want["docs/guide.md"]})
	other := h.commit("other", "other edits main.go")
	res, err = h.r.Merge(ctx, me, vcs.MainBranch, other.Hash)
	if err != nil || len(res.Conflicts) != 1 || res.Conflicts[0].Path != "project" ||
		len(res.Conflicts[0].Model) != 1 || string(res.Conflicts[0].Model[0].Location) != "src/main.go" {
		t.Fatalf("both editing src/main.go gave %+v (%v); want one conflict at project, inside it src/main.go", res.Conflicts, err)
	}
	if _, err := h.r.CommitWorkingSet(ctx, me, vcs.MainBranch, "too soon"); err == nil {
		t.Fatal("a commit went through with a conflict standing")
	}
	theirsRef := res.Conflicts[0].Theirs
	if err := h.r.ResolveConflict(ctx, me, vcs.MainBranch, "project", &theirsRef); err != nil {
		t.Fatal(err)
	}
	resolved := h.commit(vcs.MainBranch, "take other's main.go")
	if got := h.files(resolved); got["src/main.go"] != "other's version\n" {
		t.Fatalf("after resolving, src/main.go is %q", got["src/main.go"])
	}
}
