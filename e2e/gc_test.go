package e2e_test

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/memstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/repo"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
	"github.com/SmithOperatingSolutions/snapshot-core/core/vcs"
	"github.com/SmithOperatingSolutions/snapshot-core/model/blob"
)

// noise is n bytes that repeat nothing: a file of many chunks.
func noise(seed string, n int) string {
	var b []byte
	for i := 0; len(b) < n; i++ {
		h := sha256.Sum256([]byte(fmt.Sprintf("%s/%d", seed, i)))
		b = append(b, h[:]...)
	}
	return string(b[:n])
}

// C4 on a real repository: a folder's history, with files of one chunk and
// of many, comes through GC on a disk store whole, and the files only a
// deleted branch held go, a grace window after GC condemned them. Packs are
// small, so a large file's data sits in packs of its own, which only a walk
// that names every chunk of it keeps.
func TestAFoldersHistoryComesThroughGC(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "repo")
	keys, err := seal.NewKeyring()
	if err != nil {
		t.Fatal(err)
	}
	g := repo.DefaultGeometry()
	g.PackSize = 64 << 10
	h := openWith(t, dir, keys, true, g)
	var jump time.Duration
	h.o.Clock = func() time.Time { return time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC).Add(jump) }
	files := map[string]string{"README.md": "# project\n", "data/big.bin": noise("big", 3<<20)}
	h.setFolder(vcs.MainBranch, files)
	imported := h.commit(vcs.MainBranch, "import the folder")
	files["docs/guide.md"] = "Read me first.\n"
	h.setFolder(vcs.MainBranch, files)
	head := h.commit(vcs.MainBranch, "add the guide")

	scratchOnly := noise("scratch only", 1<<20)
	if err := h.r.CreateBranch(ctx, me, "scratch", imported.Hash); err != nil {
		t.Fatal(err)
	}
	h.setFolder("scratch", map[string]string{"README.md": files["README.md"], "scratch.bin": scratchOnly})
	h.commit("scratch", "a scratch file")
	if err := h.r.DeleteBranch(ctx, me, "scratch"); err != nil {
		t.Fatal(err)
	}
	gone, err := blob.Write(ctx, memstore.New(), bytes.NewReader([]byte(scratchOnly)), h.r.Config.Geometry.Stream())
	if err != nil {
		t.Fatal(err)
	}
	if got, err := h.r.Chunks().Get(ctx, gone.Hash); err != nil || len(got) == 0 {
		t.Fatalf("fixture: the scratch file's root chunk reads as %d bytes, %v", len(got), err)
	}

	if rep, err := repo.GC(ctx, me, h.o, time.Hour); err != nil || rep.Condemned == 0 || len(rep.Deleted) != 0 {
		t.Fatalf("the first GC condemned %d, deleted %d (%v); want some condemned, nothing deleted", rep.Condemned, len(rep.Deleted), err)
	}
	jump = time.Hour + time.Minute
	if rep, err := repo.GC(ctx, me, h.o, time.Hour); err != nil || len(rep.Deleted) == 0 {
		t.Fatalf("a grace window later GC deleted %d objects (%v), want the condemned packs", len(rep.Deleted), err)
	}
	if err := h.r.Close(); err != nil {
		t.Fatal(err)
	}
	h = open(t, dir, keys, false)
	if got := h.files(head); !same(got, files) {
		t.Fatalf("after GC main's folder holds %d files, want the %d it had", len(got), len(files))
	}
	if got := h.files(imported); !same(got, map[string]string{"README.md": files["README.md"], "data/big.bin": files["data/big.bin"]}) {
		t.Fatal("after GC the first commit's folder does not read back")
	}
	if _, err := h.r.Chunks().Get(ctx, gone.Hash); !errors.Is(err, chunk.ErrNotFound) {
		t.Fatalf("the file only the deleted branch held reads as %v after GC, want ErrNotFound", err)
	}
}
