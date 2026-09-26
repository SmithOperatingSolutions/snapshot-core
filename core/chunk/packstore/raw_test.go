package packstore_test

import (
	"strings"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/mem"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/pack"
)

// A tree's nodes are stored without trying zstd (#42, the per-caller
// hint): a chunk put through PutRaw is a raw frame, while a user chunk of
// the same size and kind put through Put is still compressed, and both
// read back as written.
func TestAChunkPutRawIsStoredRawAndAPutOneCompressed(t *testing.T) {
	bs := mem.New()
	s := open(t, bs, keyring(t))
	var _ chunk.RawWriter = s
	text := func(tag string) []byte {
		return []byte(strings.Repeat("compressible text "+tag+" ", 4096/20)[:4000])
	}
	node, user := text("node"), text("user")
	hn, err := s.PutRaw(ctx, node)
	if err != nil {
		t.Fatal(err)
	}
	if hn != hash.Sum(node) {
		t.Fatalf("PutRaw named the chunk %s, want its hash %s", hn.Short(), hash.Sum(node).Short())
	}
	hu, err := s.Put(ctx, user)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CompareAndSetRoot(ctx, hash.Hash{}, hu); err != nil {
		t.Fatal(err)
	}
	_, _, nu, ok := s.Location(hu)
	if !ok {
		t.Fatal("the user chunk has no location after the publish")
	}
	if nu >= int64(len(user)) {
		t.Fatalf("positive control: a %d-byte compressible user chunk put through Put is stored in %d bytes: it should still be compressed", len(user), nu)
	}
	_, _, nn, ok := s.Location(hn)
	if !ok {
		t.Fatal("the node chunk has no location after the publish")
	}
	if nn != int64(len(node))+pack.FrameOverhead {
		t.Fatalf("a %d-byte chunk put through PutRaw is stored in %d bytes, want a raw frame of %d: a tree node would pay the zstd encoder on every flush",
			len(node), nn, len(node)+pack.FrameOverhead)
	}
	for _, c := range []struct {
		h    hash.Hash
		want []byte
	}{{hn, node}, {hu, user}} {
		got, err := s.Get(ctx, c.h)
		if err != nil || string(got) != string(c.want) {
			t.Fatalf("chunk %s reads back as %d bytes (%v), want the %d written", c.h.Short(), len(got), err, len(c.want))
		}
	}
}
