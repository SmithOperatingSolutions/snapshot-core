package repo

import (
	"context"
	"strings"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/auth"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/mem"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/pack"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
	"github.com/SmithOperatingSolutions/snapshot-core/core/vcs"
)

// What a host reaches through repo.Chunks() must carry every optional
// interface the store has, or the host's models silently take the slow
// path. A host's prolly maps store their nodes through chunk.RawWriter
// when the store offers it (D17); the repository's narrowing wrapper
// forwarded Prepare and Flush but not PutRaw, so every one of a host's
// tree nodes went through Put and the zstd encoder, 27% of a consumer's
// CPU under load (#45). Proved here the way the store proves it: a node
// put raw through repo.Chunks() is stored as a raw frame, and a user
// chunk of the same shape put through Put is still compressed.
func TestRegression_SC45_ChunksForwardsPutRaw(t *testing.T) {
	ctx := context.Background()
	kr, err := seal.NewKeyring()
	if err != nil {
		t.Fatal(err)
	}
	reg, err := model.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	r, err := Init(ctx, auth.Principal{ID: "user:test"}, Options{Blobs: mem.New(), Keys: kr, Registry: reg, Authorizer: auth.AllowAll{}})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	rw, ok := r.Chunks().(chunk.RawWriter)
	if !ok {
		t.Fatalf("repo.Chunks() is not a chunk.RawWriter: a host's tree nodes all go through Put and the zstd encoder")
	}
	text := func(tag string) []byte {
		return []byte(strings.Repeat("compressible text "+tag+" ", 4096/20)[:4000])
	}
	node, user := text("node"), text("user")
	hn, err := rw.PutRaw(ctx, node)
	if err != nil {
		t.Fatal(err)
	}
	hu, err := r.Chunks().Put(ctx, user)
	if err != nil {
		t.Fatal(err)
	}
	// A publish, so both chunks have a location: creating a branch swaps
	// the root and carries the pending pack with it.
	me := auth.Principal{ID: "user:test"}
	head, err := r.Head(ctx, me, vcs.MainBranch)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.CreateBranch(ctx, me, "publish", head.Hash); err != nil {
		t.Fatal(err)
	}
	_, _, nu, ok := r.chunks.Location(hu)
	if !ok || nu >= int64(len(user)) {
		t.Fatalf("positive control: a %d-byte compressible user chunk put through repo.Chunks().Put is stored in %d bytes (found %v): it should still be compressed", len(user), nu, ok)
	}
	_, _, nn, ok := r.chunks.Location(hn)
	if !ok || nn != int64(len(node))+pack.FrameOverhead {
		t.Fatalf("a %d-byte chunk put raw through repo.Chunks() is stored in %d bytes (found %v), want a raw frame of %d: the host's tree nodes pay the zstd encoder on every flush", len(node), nn, ok, len(node)+pack.FrameOverhead)
	}
}
