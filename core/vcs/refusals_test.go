package vcs_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/vcs"
)

// A working set names namespaces that are stored, and a branch or a tag
// names a stored commit. Anything else is refused, and nothing changes.
func TestRefsNameOnlyStoredObjects(t *testing.T) {
	f := newFixture(t)
	main := vcs.MainBranch
	head := f.head(main)
	ws, err := f.r.WorkingSet(ctx, alice, main)
	if err != nil {
		t.Fatal(err)
	}
	missing := hash.Sum([]byte("never stored"))
	for _, c := range []struct {
		name string
		next vcs.WorkingSet
	}{
		{"working", vcs.WorkingSet{Working: missing, Staged: ws.Staged}},
		{"staged", vcs.WorkingSet{Working: ws.Working, Staged: missing}},
	} {
		if _, err := f.r.UpdateWorkingSet(ctx, alice, main, ws, c.next); err == nil {
			t.Errorf("a working set whose %s namespace is not stored was accepted", c.name)
		}
	}
	if now, _ := f.r.WorkingSet(ctx, alice, main); now.Hash != ws.Hash {
		t.Fatal("a refused working set changed the branch's working set")
	}
	for _, c := range []struct {
		name string
		at   hash.Hash
		want error
	}{
		{"a hash never stored", missing, chunk.ErrNotFound},
		{"a working set", ws.Hash, chunk.ErrCorrupt},
	} {
		if err := f.r.CreateBranch(ctx, alice, "dev", c.at); !errors.Is(err, c.want) {
			t.Errorf("a branch at %s = %v, want %v", c.name, err, c.want)
		}
		if _, err := f.r.CreateTag(ctx, alice, "v1", c.at, "release"); !errors.Is(err, c.want) {
			t.Errorf("a tag of %s = %v, want %v", c.name, err, c.want)
		}
	}
	if bs, err := f.r.Branches(ctx, alice); err != nil || len(bs) != 1 {
		t.Fatalf("after the refused branches, Branches = %v (%v), want [main]", bs, err)
	}
	if err := f.r.CreateBranch(ctx, alice, "dev", head.Hash); err != nil {
		t.Fatalf("positive control: a branch at the head: %v", err)
	}
	if _, err := f.r.CreateTag(ctx, alice, "v1", head.Hash, "release"); err != nil {
		t.Fatalf("positive control: a tag of the head: %v", err)
	}
}

// Commit and tag messages are UTF-8 of at most 64 KiB. Others are refused,
// and nothing is committed or tagged.
func TestMessagesAreBoundedUTF8(t *testing.T) {
	f := newFixture(t)
	main := vcs.MainBranch
	head := f.head(main)
	for _, c := range []struct{ name, msg string }{
		{"over 64 KiB", strings.Repeat("m", vcs.MaxMessageLen+1)},
		{"not UTF-8", "caf\xe9"},
	} {
		if _, err := f.r.CommitWorkingSet(ctx, alice, main, c.msg); err == nil {
			t.Errorf("a commit message %s was accepted", c.name)
		}
		if _, err := f.r.CreateTag(ctx, alice, "v1", head.Hash, c.msg); err == nil {
			t.Errorf("a tag message %s was accepted", c.name)
		}
	}
	if got := f.head(main); got.Hash != head.Hash {
		t.Fatal("a refused commit moved the head")
	}
	longest := strings.Repeat("é", vcs.MaxMessageLen/2)
	if c, err := f.r.CommitWorkingSet(ctx, alice, main, longest); err != nil || c.Message != longest {
		t.Fatalf("positive control: a commit message of exactly 64 KiB: %v", err)
	}
	if back, err := f.r.Log(ctx, alice, f.head(main).Hash, 1); err != nil || back[0].Message != longest {
		t.Fatalf("positive control: reading back the 64 KiB message: %v", err)
	}
	if _, err := f.r.CreateTag(ctx, alice, "v1", head.Hash, longest); err != nil {
		t.Fatalf("positive control: a tag message of exactly 64 KiB: %v", err)
	}
}
