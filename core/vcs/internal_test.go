package vcs

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/merge"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/object"
)

func goodCommit() Commit {
	return Commit{Parents: []hash.Hash{hash.Sum([]byte("p1")), hash.Sum([]byte("p2"))}, Namespace: hash.Sum([]byte("ns")),
		Height: 7, Time: time.Unix(0, 1_700_000_000_000_000_000).UTC(), Author: "user:alice", Message: "merge"}
}

func goodConflict() merge.Conflict {
	r := object.Ref{Model: 7, Root: model.Root{Hash: hash.Sum([]byte("o")), Size: 3, Format: 1}}
	return merge.Conflict{Path: "p", Kind: merge.BothChanged, Base: r, Ours: r, Theirs: r,
		Model: []model.Conflict{{Location: []byte("row 3"), Reason: "both changed"}}}
}

// Every rule the commit, working-set and conflict decoders enforce, one
// forgery each (hand-built from a good encoding).
func TestForgedChunksAreCorrupt(t *testing.T) {
	c := goodCommit()
	if back, err := DecodeCommit(c.Encode()); err != nil || back.Height != 7 || len(back.Parents) != 2 || !back.Time.Equal(c.Time) {
		t.Fatalf("positive control: %+v, %v", back, err)
	}
	three := c
	three.Parents = append(three.Parents, hash.Sum([]byte("p3")))
	same := c
	same.Parents = []hash.Hash{c.Parents[0], c.Parents[0]}
	rootAtHeight := c
	rootAtHeight.Parents = nil
	parentAtZero := c
	parentAtZero.Height = 0
	badUTF8 := c
	badUTF8.Message = "\xff"
	for name, b := range map[string][]byte{
		"three parents":          three.Encode(),
		"one parent twice":       same.Encode(),
		"a root above height 0":  rootAtHeight.Encode(),
		"parents at height 0":    parentAtZero.Encode(),
		"a message not UTF-8":    badUTF8.Encode(),
		"a byte past the end":    append(c.Encode(), 0),
		"a truncated commit":     c.Encode()[:40],
		"the working set's kind": append([]byte{kindWorkingSet}, c.Encode()[1:]...),
	} {
		if _, err := DecodeCommit(b); !errors.Is(err, chunk.ErrCorrupt) {
			t.Errorf("commit, %s: DecodeCommit = %v, want ErrCorrupt", name, err)
		}
	}

	ws := WorkingSet{Working: hash.Sum([]byte("w")), Staged: hash.Sum([]byte("s")), Merge: &MergeState{Base: hash.Sum([]byte("b"))}}
	if back, err := decodeWorkingSet(ws.encode()); err != nil || back.Merge == nil || back.Merge.Base != ws.Merge.Base {
		t.Fatalf("positive control: %+v, %v", back, err)
	}
	flagTwo := (WorkingSet{Working: ws.Working, Staged: ws.Staged}).encode()
	flagTwo[len(flagTwo)-1] = 2
	for name, b := range map[string][]byte{
		"merge flag 2":        flagTwo,
		"a byte past the end": append(ws.encode(), 0),
		"truncated":           ws.encode()[:70],
	} {
		if _, err := decodeWorkingSet(b); !errors.Is(err, chunk.ErrCorrupt) {
			t.Errorf("working set, %s: %v, want ErrCorrupt", name, err)
		}
	}

	good := encodeConflict(goodConflict())
	if back, err := decodeConflict("p", good); err != nil || back.Kind != merge.BothChanged || len(back.Model) != 1 {
		t.Fatalf("positive control: %+v, %v", back, err)
	}
	kind9 := bytes.Clone(good)
	kind9[0] = 9
	// No sides and no model conflicts, but the last side's flag is 2: the
	// rest would parse cleanly if the flag were not checked.
	sideFlag := []byte{byte(merge.DeleteEdit), 0, 0, 2, 0}
	for name, b := range map[string][]byte{
		"kind 9":              kind9,
		"a side flag of 2":    sideFlag,
		"a byte past the end": append(bytes.Clone(good), 0),
	} {
		if _, err := decodeConflict("p", b); !errors.Is(err, chunk.ErrCorrupt) {
			t.Errorf("conflict, %s: %v, want ErrCorrupt", name, err)
		}
	}
}

// Whatever decodes re-encodes to the same bytes, and hostile bytes never panic.
func FuzzDecodeCommit(f *testing.F) {
	f.Add(goodCommit().Encode())
	f.Fuzz(func(t *testing.T, b []byte) {
		c, err := DecodeCommit(b)
		if err == nil && !bytes.Equal(c.Encode(), b) {
			t.Fatal("a commit decoded that does not re-encode to itself")
		}
	})
}

func FuzzDecodeWorkingSet(f *testing.F) {
	f.Add(WorkingSet{Merge: &MergeState{}}.encode())
	f.Fuzz(func(t *testing.T, b []byte) {
		ws, err := decodeWorkingSet(b)
		if err == nil && !bytes.Equal(ws.encode(), b) {
			t.Fatal("a working set decoded that does not re-encode to itself")
		}
	})
}

func FuzzDecodeConflict(f *testing.F) {
	f.Add(encodeConflict(goodConflict()))
	f.Fuzz(func(t *testing.T, b []byte) {
		c, err := decodeConflict("p", b)
		if err == nil && !bytes.Equal(encodeConflict(c), b) {
			t.Fatal("a conflict decoded that does not re-encode to itself")
		}
	})
}

// Every rule the tag decoder enforces, one forgery each; the round trip is
// the positive control.
func TestForgedTagsAreCorrupt(t *testing.T) {
	good := Tag{Target: hash.Sum([]byte("c")), Time: time.Unix(0, 1_700_000_000_000_000_000).UTC(), Tagger: "user:alice", Message: "v1"}
	back, err := decodeTag(good.encode())
	if err != nil || back.Target != good.Target || !back.Time.Equal(good.Time) || back.Tagger != good.Tagger || back.Message != good.Message {
		t.Fatalf("positive control: a tag decodes as %+v (%v), want %+v", back, err, good)
	}
	b := good.encode()
	badUTF8 := good
	badUTF8.Message = "caf\xe9"
	for name, forged := range map[string][]byte{
		"another chunk's kind": append([]byte{kindCommit}, b[1:]...),
		"empty":                nil,
		"a byte past the end":  append(bytes.Clone(b), 0),
		"truncated":            b[:len(b)-1],
		"a message not UTF-8":  badUTF8.encode(),
	} {
		if _, err := decodeTag(forged); !errors.Is(err, chunk.ErrCorrupt) {
			t.Errorf("%s: decodeTag = %v, want ErrCorrupt", name, err)
		}
	}
}

// Whatever decodes as a tag re-encodes to the same bytes, and hostile bytes
// never panic.
func FuzzDecodeTag(f *testing.F) {
	f.Add(Tag{Target: hash.Sum([]byte("c")), Time: time.Unix(0, 1).UTC(), Tagger: "user:a", Message: "m"}.encode())
	f.Add([]byte{kindTag})
	f.Fuzz(func(t *testing.T, b []byte) {
		tag, err := decodeTag(b)
		if err != nil {
			return
		}
		if !bytes.Equal(tag.encode(), b) {
			t.Fatal("a tag decoded that does not re-encode to itself")
		}
	})
}
