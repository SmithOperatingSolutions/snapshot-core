package pack

import (
	"bytes"
	"encoding/binary"
	"errors"
	"runtime"
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

	honest := bytes.Repeat([]byte("abc"), 1000)
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

// Whatever decodes as an index re-encodes to the same bytes: the index has
// one encoding per value.
func FuzzDecodeIndex(f *testing.F) {
	f.Add(encodeIndex([]Entry{{Hash: hash.Sum([]byte("a")), Offset: HeaderSize, StoredLen: 40, RawLen: 12, Codec: CodecRaw}}), uint64(1000))
	f.Add([]byte("SCPI"), uint64(0))
	f.Fuzz(func(t *testing.T, b []byte, indexOffset uint64) {
		es, err := decodeIndex(b, indexOffset)
		if err != nil {
			return
		}
		if !bytes.Equal(encodeIndex(es), b) {
			t.Fatal("an index decoded that does not re-encode to itself")
		}
	})
}

// reseal replaces a pack's index with entries sealed properly under the
// pack's own index key: the forgery authenticates, so only the index's own
// validation can refuse it.
func reseal(t *testing.T, kr *seal.Keyring, repo seal.RepoID, b Built, entries []Entry) []byte {
	t.Helper()
	keys, err := DeriveKeys(kr, repo, b.Info.Salt)
	if err != nil {
		t.Fatal(err)
	}
	indexOffset := binary.LittleEndian.Uint64(b.Bytes[len(b.Bytes)-TrailerSize:])
	out := bytes.Clone(b.Bytes[:indexOffset])
	sealed, err := keys.index.Seal(out[:HeaderSize], encodeIndex(entries))
	if err != nil {
		t.Fatal(err)
	}
	out = append(out, sealed...)
	out = binary.LittleEndian.AppendUint64(out, indexOffset)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(sealed)))
	return append(out, trailerMagic...)
}

func TestForgedIndexesAreRefused(t *testing.T) {
	kr, _ := seal.NewKeyring()
	c, _ := NewCodec()
	defer c.Close()
	repo := seal.RepoID{2}
	w, err := NewWriter(kr, repo, c, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range [][]byte{bytes.Repeat([]byte("x"), 3000), []byte("small one"), []byte("another")} {
		if err := w.Add(hash.Sum(d), d); err != nil {
			t.Fatal(err)
		}
	}
	b, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	good := b.Info.Entries
	if again := reseal(t, kr, repo, b, good); true {
		if _, err := ReadInfo(Name(again), again, kr, repo); err != nil {
			t.Fatalf("positive control: a resealed honest index is refused: %v", err)
		}
	}
	forge := func(f func(es []Entry) []Entry) []byte {
		es := append([]Entry(nil), good...)
		return reseal(t, kr, repo, b, f(es))
	}
	var zstdIdx, rawIdx, smallest int
	for i, e := range good {
		if e.Codec == CodecZstd {
			zstdIdx = i
		} else {
			rawIdx = i
		}
		if e.StoredLen < good[smallest].StoredLen {
			smallest = i
		}
	}
	if good[smallest].StoredLen > HeaderSize {
		t.Fatalf("the smallest frame is %d bytes; it cannot fit inside the header to forge", good[smallest].StoredLen)
	}
	indexOffset := binary.LittleEndian.Uint64(b.Bytes[len(b.Bytes)-TrailerSize:])
	for name, bad := range map[string][]byte{
		"overlapping frames": forge(func(es []Entry) []Entry { es[1].Offset = es[0].Offset + 1; return es }),
		// Lengths stay consistent with each codec, so only the placement
		// checks can refuse these: a frame wholly inside the header, and one
		// that starts past the index.
		"frame in the header": forge(func(es []Entry) []Entry {
			es[smallest].Offset = 0
			return es
		}),
		"frame past the index": forge(func(es []Entry) []Entry { es[0].Offset = uint32(indexOffset) + 1; return es }),
		"unsorted": forge(func(es []Entry) []Entry {
			es[0], es[1] = es[1], es[0]
			return es
		}),
		"duplicate hash":         forge(func(es []Entry) []Entry { es[1].Hash = es[0].Hash; return es }),
		"raw length lie":         forge(func(es []Entry) []Entry { es[rawIdx].RawLen++; return es }),
		"zstd not smaller":       forge(func(es []Entry) []Entry { es[zstdIdx].RawLen = es[zstdIdx].StoredLen - 28; return es }),
		"chunk over the limit":   forge(func(es []Entry) []Entry { es[zstdIdx].RawLen = MaxChunkSize + 1; return es }),
		"unknown codec":          forge(func(es []Entry) []Entry { es[0].Codec = 7; return es }),
		"frame shorter than tag": forge(func(es []Entry) []Entry { es[rawIdx].StoredLen = 10; return es }),
	} {
		if _, err := ReadInfo(Name(bad), bad, kr, repo); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: ReadInfo accepted a forged index (err=%v)", name, err)
		}
	}
}

// The decoder's memory bound is what stops a bomb, not the length check
// after it: measured in bytes allocated.
func TestDecompressionIsBoundedInMemory(t *testing.T) {
	kr, _ := seal.NewKeyring()
	c, _ := NewCodec()
	defer c.Close()
	salt, _ := seal.NewSalt()
	keys, err := DeriveKeys(kr, seal.RepoID{3}, salt)
	if err != nil {
		t.Fatal(err)
	}
	enc, _ := zstd.NewWriter(nil)
	zeros := make([]byte, 64<<20)
	bomb := enc.EncodeAll(zeros, nil)
	h := hash.Sum(zeros[:MaxChunkSize])
	sealed, err := keys.chunk.Seal(h[:], bomb)
	if err != nil {
		t.Fatal(err)
	}
	zeros = nil
	e := Entry{Hash: h, RawLen: MaxChunkSize, Codec: CodecZstd, StoredLen: uint32(len(sealed))}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err = OpenFrame(keys, c, e, sealed)
	runtime.ReadMemStats(&after)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("a 64 MiB bomb opened (err=%v)", err)
	}
	if grew := after.TotalAlloc - before.TotalAlloc; grew > 16<<20 {
		t.Fatalf("opening a 64 MiB decompression bomb allocated %d MiB: the decoder is not bounded", grew>>20)
	}
}
