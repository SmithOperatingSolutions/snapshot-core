package vcs

import (
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/SmithOperatingSolutions/snapshot-core/core/auth"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/merge"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/object"
	"github.com/SmithOperatingSolutions/snapshot-core/core/wire"
)

// Chunk kinds (docs/DESIGN.md §8).
const (
	kindCommit     = 0x03
	kindTag        = 0x04
	kindWorkingSet = 0x05
)

// Conflict record limits.
const (
	maxModelConflicts = 10_000
	maxLocationLen    = 4096
	maxReasonLen      = 1024
)

func corrupt(format string, args ...any) error {
	return fmt.Errorf("%w: vcs: %s", chunk.ErrCorrupt, fmt.Sprintf(format, args...))
}

func toTime(ns int64) time.Time { return time.Unix(0, ns).UTC() }

// Encode is the DESIGN §8 commit chunk: 0x03 · parents u8 · parent [32] ×
// parents · namespace [32] · height uvarint · time i64 · author · message.
func (c Commit) Encode() []byte {
	var w wire.Writer
	w.U8(kindCommit)
	w.U8(uint8(len(c.Parents)))
	for _, p := range c.Parents {
		w.Raw(p[:])
	}
	w.Raw(c.Namespace[:])
	w.Uvarint(c.Height)
	w.U64(uint64(c.Time.UnixNano()))
	w.LenBytes([]byte(c.Author))
	w.LenBytes([]byte(c.Message))
	return w.Bytes()
}

// DecodeCommit parses a commit chunk (chunk.ErrCorrupt when malformed).
func DecodeCommit(b []byte) (Commit, error) {
	r := wire.NewReader(b)
	var c Commit
	kind, n := r.U8(), r.U8()
	if r.Err() != nil || kind != kindCommit || n > 2 {
		return Commit{}, corrupt("commit header")
	}
	for i := uint8(0); i < n; i++ {
		var p hash.Hash
		copy(p[:], r.Fixed(hash.Size))
		c.Parents = append(c.Parents, p)
	}
	copy(c.Namespace[:], r.Fixed(hash.Size))
	c.Height = r.Uvarint()
	c.Time = toTime(int64(r.U64()))
	c.Author = string(r.LenBytes(auth.MaxIDLen))
	c.Message = string(r.LenBytes(MaxMessageLen))
	switch {
	case r.Done() != nil:
		return Commit{}, corrupt("commit: %v", r.Done())
	case n == 2 && c.Parents[0] == c.Parents[1]:
		return Commit{}, corrupt("a commit's two parents are one commit")
	case (n == 0) != (c.Height == 0):
		return Commit{}, corrupt("a commit of %d parents at height %d", n, c.Height)
	case !utf8.ValidString(c.Author) || !utf8.ValidString(c.Message):
		return Commit{}, corrupt("a commit's author or message is not UTF-8")
	}
	return c, nil
}

// Tag chunk: 0x04 · target [32] · time i64 · tagger · message.
func (t Tag) encode() []byte {
	var w wire.Writer
	w.U8(kindTag)
	w.Raw(t.Target[:])
	w.U64(uint64(t.Time.UnixNano()))
	w.LenBytes([]byte(t.Tagger))
	w.LenBytes([]byte(t.Message))
	return w.Bytes()
}

// decodeTag parses a tag chunk (chunk.ErrCorrupt when malformed).
func decodeTag(b []byte) (Tag, error) {
	r := wire.NewReader(b)
	var t Tag
	if r.U8() != kindTag {
		return Tag{}, corrupt("tag header")
	}
	copy(t.Target[:], r.Fixed(hash.Size))
	t.Time = toTime(int64(r.U64()))
	t.Tagger = string(r.LenBytes(auth.MaxIDLen))
	t.Message = string(r.LenBytes(MaxMessageLen))
	switch {
	case r.Done() != nil:
		return Tag{}, corrupt("tag: %v", r.Done())
	case !utf8.ValidString(t.Tagger) || !utf8.ValidString(t.Message):
		return Tag{}, corrupt("a tag's tagger or message is not UTF-8")
	}
	return t, nil
}

// Working set chunk: 0x05 · working [32] · staged [32] · merging u8 ·
// [base [32] · theirs [32] · conflicts [32] · working before [32] ·
// staged before [32]].
func (ws WorkingSet) encode() []byte {
	var w wire.Writer
	w.U8(kindWorkingSet)
	w.Raw(ws.Working[:])
	w.Raw(ws.Staged[:])
	if ws.Merge == nil {
		w.U8(0)
		return w.Bytes()
	}
	w.U8(1)
	w.Raw(ws.Merge.Base[:])
	w.Raw(ws.Merge.Theirs[:])
	w.Raw(ws.Merge.Conflicts[:])
	w.Raw(ws.Merge.PreWorking[:])
	w.Raw(ws.Merge.PreStaged[:])
	return w.Bytes()
}

func decodeWorkingSet(b []byte) (WorkingSet, error) {
	r := wire.NewReader(b)
	var ws WorkingSet
	if r.U8() != kindWorkingSet {
		return WorkingSet{}, corrupt("working set header")
	}
	copy(ws.Working[:], r.Fixed(hash.Size))
	copy(ws.Staged[:], r.Fixed(hash.Size))
	switch merging := r.U8(); {
	case r.Err() != nil || merging > 1:
		return WorkingSet{}, corrupt("working set merge flag")
	case merging == 1:
		ws.Merge = &MergeState{}
		copy(ws.Merge.Base[:], r.Fixed(hash.Size))
		copy(ws.Merge.Theirs[:], r.Fixed(hash.Size))
		copy(ws.Merge.Conflicts[:], r.Fixed(hash.Size))
		copy(ws.Merge.PreWorking[:], r.Fixed(hash.Size))
		copy(ws.Merge.PreStaged[:], r.Fixed(hash.Size))
	}
	if err := r.Done(); err != nil {
		return WorkingSet{}, corrupt("working set: %v", err)
	}
	return ws, nil
}

// Conflict record (the value of a working set's conflicts map, keyed by
// path): kind u8 · base, ours, theirs (each present u8 · object reference) ·
// model conflicts uvarint · (location · reason) × that many.
func encodeConflict(c merge.Conflict) []byte {
	var w wire.Writer
	w.U8(uint8(c.Kind))
	for _, r := range []object.Ref{c.Base, c.Ours, c.Theirs} {
		if r == (object.Ref{}) {
			w.U8(0)
			continue
		}
		w.U8(1)
		w.Raw(r.Encode())
	}
	w.Uvarint(uint64(len(c.Model)))
	for _, mc := range c.Model {
		w.LenBytes(mc.Location)
		w.LenBytes([]byte(mc.Reason))
	}
	return w.Bytes()
}

// recordable refuses a conflict larger than its record holds, under the
// limits decodeConflict reads it back with: written, it would leave a merge
// state whose conflicts do not read. The message names neither the path
// nor the model's text.
func recordable(c merge.Conflict) error {
	if len(c.Model) > maxModelConflicts {
		return fmt.Errorf("%w: %d model conflicts at one path, limit %d", ErrConflictTooLarge, len(c.Model), maxModelConflicts)
	}
	for i, mc := range c.Model {
		if len(mc.Location) > maxLocationLen {
			return fmt.Errorf("%w: model conflict %d locates itself in %d bytes, limit %d", ErrConflictTooLarge, i, len(mc.Location), maxLocationLen)
		}
		if len(mc.Reason) > maxReasonLen {
			return fmt.Errorf("%w: model conflict %d gives a %d-byte reason, limit %d", ErrConflictTooLarge, i, len(mc.Reason), maxReasonLen)
		}
	}
	return nil
}

func decodeConflict(path string, b []byte) (merge.Conflict, error) {
	r := wire.NewReader(b)
	c := merge.Conflict{Path: path, Kind: merge.Kind(r.U8())}
	if c.Kind < merge.BothChanged || c.Kind > merge.ModelChange {
		return merge.Conflict{}, corrupt("conflict kind %d", c.Kind)
	}
	for _, dst := range []*object.Ref{&c.Base, &c.Ours, &c.Theirs} {
		switch present := r.U8(); {
		case r.Err() != nil || present > 1:
			return merge.Conflict{}, corrupt("conflict side")
		case present == 1:
			ref, err := object.DecodeRef(r.Fixed(object.RefSize))
			if err != nil {
				return merge.Conflict{}, err
			}
			*dst = ref
		}
	}
	n := r.Uvarint()
	if r.Err() != nil || n > maxModelConflicts {
		return merge.Conflict{}, corrupt("conflict: %d model conflicts", n)
	}
	for i := uint64(0); i < n; i++ {
		loc, reason := r.LenBytes(maxLocationLen), r.LenBytes(maxReasonLen)
		if r.Err() != nil {
			break // refused below for what the bytes hold, not for the claim
		}
		c.Model = append(c.Model, model.Conflict{Location: append([]byte(nil), loc...), Reason: string(reason)})
	}
	if err := r.Done(); err != nil {
		return merge.Conflict{}, corrupt("conflict: %v", err)
	}
	return c, nil
}
