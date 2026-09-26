package blob_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"pgregory.net/rapid"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/mem"
)

// Refusals with their positive control: the repository's own layout is valid.
func TestValidName(t *testing.T) {
	for _, n := range []string{
		"config", "packs/" + strings.Repeat("ab", 32), "index/" + strings.Repeat("0f", 32),
		"probe/x", "a-b.c_d", strings.Repeat("a", blob.MaxSegmentLen),
		strings.Repeat("a/", blob.MaxDepth-1) + "a",
	} {
		if err := blob.ValidName(n); err != nil {
			t.Errorf("ValidName(%q) = %v, want nil", n, err)
		}
	}
	for _, n := range []string{
		"", "/", "/a", "a/", "a//b", ".", "..", "a/..", "../a", ".git", "a/.tmp", "A", "a b",
		"a\x00", "é", "a\\b", "a:b", strings.Repeat("a", blob.MaxSegmentLen+1),
		strings.Repeat("a/", blob.MaxDepth) + "a", strings.Repeat("abcdefgh/", 60) + "a",
	} {
		if err := blob.ValidName(n); !errors.Is(err, blob.ErrInvalidName) {
			t.Errorf("ValidName(%q) = %v, want ErrInvalidName", n, err)
		}
	}
}

// Property: a name made only of allowed segments within the limits is valid,
// and inserting any disallowed byte anywhere makes it invalid.
func TestValidNameProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		segs := rapid.SliceOfN(rapid.StringMatching(`[a-z0-9_-][a-z0-9._-]{0,20}`), 1, blob.MaxDepth).Draw(t, "segs")
		name := strings.Join(segs, "/")
		if err := blob.ValidName(name); err != nil {
			t.Fatalf("ValidName(%q) = %v for a name built from allowed segments", name, err)
		}
		bad := rapid.SampledFrom([]string{"\x00", " ", "A", "\\", ":", "\n", "é", "//"}).Draw(t, "bad")
		at := rapid.IntRange(0, len(name)).Draw(t, "at")
		mutated := name[:at] + bad + name[at:]
		if err := blob.ValidName(mutated); !errors.Is(err, blob.ErrInvalidName) {
			t.Fatalf("ValidName(%q) = %v after inserting %q", mutated, err, bad)
		}
	})
}

func TestValidPrefix(t *testing.T) {
	for _, p := range []string{"", "packs/", "packs/0", "a", "a/b/"} {
		if err := blob.ValidPrefix(p); err != nil {
			t.Errorf("ValidPrefix(%q) = %v", p, err)
		}
	}
	for _, p := range []string{"/", "/a", "a//", "..", "a/../", "A", ".h"} {
		if err := blob.ValidPrefix(p); !errors.Is(err, blob.ErrInvalidName) {
			t.Errorf("ValidPrefix(%q) = %v, want ErrInvalidName", p, err)
		}
	}
}

func TestNoDeleteForbidsOnlyDelete(t *testing.T) {
	ctx := context.Background()
	raw := mem.New()
	s := blob.NoDelete(raw)
	if err := s.Put(ctx, "x", strings.NewReader("x"), 1); err != nil {
		t.Fatalf("positive control: Put through NoDelete: %v", err)
	}
	if err := s.Delete(ctx, "x"); !errors.Is(err, blob.ErrDeleteForbidden) {
		t.Fatalf("Delete through NoDelete = %v, want ErrDeleteForbidden: the repository could delete packs", err)
	}
	if _, err := raw.Stat(ctx, "x"); err != nil {
		t.Fatalf("the object is gone after a forbidden Delete: %v", err)
	}
	if _, err := s.SwapRoot(ctx, blob.NoVersion, []byte("r")); err != nil {
		t.Fatalf("SwapRoot through NoDelete: %v", err)
	}
}

// A repository runs on NoDelete(store): the store's journal must stay
// reachable through it, or a journaled store would silently commit by
// publishing (#34).
func TestNoDeleteKeepsTheJournal(t *testing.T) {
	ctx := context.Background()
	s := blob.NoDelete(mem.New())
	j, ok := s.(blob.Journaler)
	if !ok {
		t.Fatal("NoDelete(a store with a journal) is no Journaler: the repository could not reach its journal")
	}
	jn, err := j.OpenJournal(ctx)
	if err != nil {
		t.Fatalf("OpenJournal through NoDelete: %v", err)
	}
	if err := jn.Append(ctx, []byte("through")); err != nil {
		t.Fatalf("Append through NoDelete: %v", err)
	}
	_ = jn.Close()
	if err := s.Delete(ctx, "x"); !errors.Is(err, blob.ErrDeleteForbidden) {
		t.Fatalf("Delete through NoDelete of a Journaler = %v, want ErrDeleteForbidden: the repository could delete packs", err)
	}
	if _, ok := blob.NoDelete(noJournal{mem.New()}).(blob.Journaler); ok {
		t.Fatal("NoDelete of a store without a journal claims one")
	}
}

// noJournal hides a store's journal.
type noJournal struct{ blob.BlobStore }
