//go:build slow

package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
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

// #40: a host that batches writes many small objects through one
// namespace editor and commits once, and then the write of each object is
// its ceiling. A 100-byte object cost about 200 µs of CPU (tools/
// commitbench's batch run: 5,000 to 6,000 writes a second on either
// backend) and costs about 7 µs since: its buffers are sized to it and it
// is cut on the caller's goroutine. This is a regression guard, not a
// red: the bar, 50 µs an object, is seven times what the i7-1360P this was
// measured on takes and a quarter of what it took before, so a slower
// machine passes and a return of either cost fails.
func TestSlowSmallObjectsWriteInMicroseconds(t *testing.T) {
	const objects, size = 10000, 100
	const bar = 50 * time.Microsecond
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
	ws, err := r.WorkingSet(ctx, me, vcs.MainBranch)
	if err != nil {
		t.Fatal(err)
	}
	n, err := r.Namespace(ctx, ws.Working)
	if err != nil {
		t.Fatal(err)
	}
	bodies := make([][]byte, objects) // generated first: the source must not be what is measured
	src := newRandom(40, objects*size)
	for i := range bodies {
		bodies[i] = make([]byte, size)
		if _, err := io.ReadFull(src, bodies[i]); err != nil {
			t.Fatal(err)
		}
	}
	geo := r.Config.Geometry.Stream()
	e := n.Editor()
	t0 := time.Now()
	for i, b := range bodies {
		root, err := blob.Write(ctx, r.Chunks(), bytes.NewReader(b), geo)
		if err != nil {
			t.Fatal(err)
		}
		if err := e.Put(fmt.Sprintf("batch/%06d", i), object.Ref{Model: blob.ID, Root: root}); err != nil {
			t.Fatal(err)
		}
	}
	write := time.Since(t0)
	if n, err = e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Commit(ctx, me, vcs.MainBranch, ws, n.Root(), "batch"); err != nil {
		t.Fatal(err)
	}
	per := write / objects
	t.Logf("%d %d-byte objects written in %v: %v an object, %.0f a second", objects, size, write.Round(time.Millisecond), per.Round(100*time.Nanosecond), objects/write.Seconds())

	// What was committed is what was written.
	head, err := r.Head(ctx, me, vcs.MainBranch)
	if err != nil {
		t.Fatal(err)
	}
	if n, err = r.Namespace(ctx, head.Namespace); err != nil {
		t.Fatal(err)
	}
	if n.Count() != objects {
		t.Fatalf("the commit holds %d objects, want %d", n.Count(), objects)
	}
	for _, i := range []int{0, objects / 2, objects - 1} {
		ref, _, ok, err := n.Get(ctx, fmt.Sprintf("batch/%06d", i))
		if err != nil || !ok {
			t.Fatalf("object %d is not in the commit: %v %v", i, ok, err)
		}
		rd, err := blob.Open(ctx, r.Chunks(), ref.Root)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(rd)
		if err != nil || !bytes.Equal(got, bodies[i]) {
			t.Fatalf("object %d reads back as %d bytes (%v), not the %d written", i, len(got), err, size)
		}
	}
	if per > bar {
		t.Fatalf("writing a %d-byte object took %v, over the %v bar (about 7 µs when #40 closed, 200 µs before): a host that batches is back to a few thousand writes a second", size, per.Round(100*time.Nanosecond), bar)
	}
}
