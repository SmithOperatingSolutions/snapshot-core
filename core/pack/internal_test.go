package pack

import (
	"bytes"
	"errors"
	"testing"

	"github.com/klauspost/compress/zstd"

	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
)

// A frame that authenticates can still lie: a zstd payload that expands past
// the chunk limit (a decompression bomb) or to a length other than the index
// says must be refused before it costs memory.
func TestHostileFramesAreRefused(t *testing.T) {
	kr, err := seal.NewKeyring()
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	salt, _ := seal.NewSalt()
	keys, err := DeriveKeys(kr, seal.RepoID{1}, salt)
	if err != nil {
		t.Fatal(err)
	}
	enc, _ := zstd.NewWriter(nil)
	bomb := enc.EncodeAll(make([]byte, 8*MaxChunkSize), nil) // 8 MiB of zeros, a few hundred bytes compressed
	h := hash.Sum(make([]byte, 8*MaxChunkSize))
	sealedBomb, err := keys.chunk.Seal(h[:], bomb)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenFrame(keys, c, Entry{Hash: h, RawLen: MaxChunkSize, Codec: CodecZstd, StoredLen: uint32(len(sealedBomb))}, sealedBomb); !errors.Is(err, ErrCorrupt) {
		t.Errorf("a decompression bomb opened (err=%v): one frame could exhaust memory", err)
	}

	honest := []byte(bytes.Repeat([]byte("abc"), 1000))
	hh := hash.Sum(honest)
	sealed, err := keys.chunk.Seal(hh[:], enc.EncodeAll(honest, nil))
	if err != nil {
		t.Fatal(err)
	}
	good := Entry{Hash: hh, RawLen: uint32(len(honest)), Codec: CodecZstd, StoredLen: uint32(len(sealed))}
	if got, err := OpenFrame(keys, c, good, sealed); err != nil || !bytes.Equal(got, honest) {
		t.Fatalf("positive control: %v", err)
	}
	lying := good
	lying.RawLen--
	if _, err := OpenFrame(keys, c, lying, sealed); !errors.Is(err, ErrCorrupt) {
		t.Errorf("a frame whose length disagrees with the index opened (err=%v)", err)
	}
	unknown := good
	unknown.Codec = 9
	if _, err := OpenFrame(keys, c, unknown, sealed); !errors.Is(err, ErrCorrupt) {
		t.Errorf("an unknown codec opened (err=%v)", err)
	}
	// Right bytes, wrong claimed identity: the SHA-256 check is the last word.
	wrongHash := good
	wrongHash.Hash = hash.Sum([]byte("something else"))
	sealedUnderWrong, _ := keys.chunk.Seal(wrongHash.Hash[:], enc.EncodeAll(honest, nil))
	wrongHash.StoredLen = uint32(len(sealedUnderWrong))
	if _, err := OpenFrame(keys, c, wrongHash, sealedUnderWrong); !errors.Is(err, ErrCorrupt) {
		t.Errorf("a frame whose bytes do not hash to its identity opened (err=%v)", err)
	}
}
