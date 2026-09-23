package cdc_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/cdc"
)

// stream is n bytes of SHA-256 counter output from seed, as a reader, so a
// 1 GiB stream never has to be held in memory.
type stream struct {
	seed      string
	remaining int64
	ctr       uint64
	buf       []byte
}

func newStream(seed string, n int64) *stream { return &stream{seed: seed, remaining: n} }

func (s *stream) Read(p []byte) (int, error) {
	if s.remaining == 0 {
		return 0, io.EOF
	}
	n := 0
	for n < len(p) && s.remaining > 0 {
		if len(s.buf) == 0 {
			var c [8]byte
			binary.BigEndian.PutUint64(c[:], s.ctr)
			s.ctr++
			h := sha256.Sum256(append([]byte(s.seed), c[:]...))
			s.buf = h[:]
		}
		k := copy(p[n:], s.buf)
		if int64(k) > s.remaining {
			k = int(s.remaining)
		}
		s.buf = s.buf[k:]
		n += k
		s.remaining -= int64(k)
	}
	return n, nil
}

// insertByte yields r with one extra byte spliced in at offset.
func insertByte(r io.Reader, offset int64, b byte) io.Reader {
	return io.MultiReader(io.LimitReader(r, offset), bytes.NewReader([]byte{b}), r)
}

func chunkHashes(t *testing.T, r io.Reader, g cdc.Geometry) [][32]byte {
	t.Helper()
	c, err := cdc.New(r, g)
	if err != nil {
		t.Fatal(err)
	}
	var hs [][32]byte
	for {
		b, err := c.Next()
		if errors.Is(err, io.EOF) {
			return hs
		}
		if err != nil {
			t.Fatal(err)
		}
		if len(b) > g.Max {
			t.Fatalf("chunk %d is %d bytes, above the geometry's hard max %d", len(hs), len(b), g.Max)
		}
		hs = append(hs, sha256.Sum256(b))
	}
}

// changedChunks counts chunks of b that a store holding a would have to write.
func changedChunks(a, b [][32]byte) int {
	have := make(map[[32]byte]bool, len(a))
	for _, h := range a {
		have[h] = true
	}
	n := 0
	for _, h := range b {
		if !have[h] {
			n++
		}
	}
	return n
}

func TestDefaultGeometryIsValidAndDisknexusProven(t *testing.T) {
	g := cdc.DefaultGeometry()
	if g != (cdc.Geometry{Min: 16 << 10, Max: 512 << 10, Mask: 0xFFFF}) {
		t.Fatalf("default geometry %+v, want 16 KiB / 512 KiB / 0xFFFF", g)
	}
	if err := g.Validate(); err != nil {
		t.Fatalf("the default geometry does not validate: %v", err)
	}
}

func TestValidateRefusesUnusableGeometry(t *testing.T) {
	for name, g := range map[string]cdc.Geometry{
		"max zero (a chunk per byte)": {Min: 16 << 10, Max: 0, Mask: 0xFFFF},
		"min above max":               {Min: 64 << 10, Max: 32 << 10, Mask: 0xFFFF},
		"min equals max":              {Min: 64 << 10, Max: 64 << 10, Mask: 0xFFFF},
		"min tiny":                    {Min: 32, Max: 64 << 10, Mask: 0xFFFF},
		"max over the chunk limit":    {Min: 16 << 10, Max: cdc.MaxChunkSize + 1, Mask: 0xFFFF},
		"mask zero":                   {Min: 16 << 10, Max: 512 << 10, Mask: 0},
		"mask not 2^n-1":              {Min: 16 << 10, Max: 512 << 10, Mask: 0xF0F0},
		"mask too small":              {Min: 16 << 10, Max: 512 << 10, Mask: 0x7F},
		"mask too large":              {Min: 16 << 10, Max: 512 << 10, Mask: 1<<31 - 1},
	} {
		if err := g.Validate(); !errors.Is(err, cdc.ErrGeometry) {
			t.Errorf("%s: Validate(%+v) = %v, want ErrGeometry", name, g, err)
		}
		if _, err := cdc.New(bytes.NewReader(nil), g); !errors.Is(err, cdc.ErrGeometry) {
			t.Errorf("%s: New accepted an invalid geometry (err=%v)", name, err)
		}
	}
	// Boundaries of the valid range are valid.
	for _, g := range []cdc.Geometry{
		{Min: 64, Max: 65, Mask: 0xFF},
		{Min: 16 << 10, Max: cdc.MaxChunkSize, Mask: 1<<30 - 1},
	} {
		if err := g.Validate(); err != nil {
			t.Errorf("positive control: %+v refused: %v", g, err)
		}
	}
}

func TestChunksReassembleAndAreDeterministic(t *testing.T) {
	g := cdc.DefaultGeometry()
	var src bytes.Buffer
	if _, err := io.Copy(&src, newStream("reassemble", 8<<20)); err != nil {
		t.Fatal(err)
	}
	c, err := cdc.New(bytes.NewReader(src.Bytes()), g)
	if err != nil {
		t.Fatal(err)
	}
	var joined []byte
	var kept [][]byte
	for {
		b, err := c.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		kept = append(kept, b)
		joined = append(joined, b...)
	}
	if len(kept) < 50 {
		t.Fatalf("8 MiB produced %d chunks; the fixture cannot distinguish anything", len(kept))
	}
	if !bytes.Equal(joined, src.Bytes()) {
		t.Fatal("chunks do not reassemble to the stream: a file read back would differ")
	}
	again := chunkHashes(t, bytes.NewReader(src.Bytes()), g)
	if len(again) != len(kept) {
		t.Fatalf("a second pass cut %d chunks, the first %d: chunking is not deterministic", len(again), len(kept))
	}
	for i, b := range kept {
		if sha256.Sum256(b) != again[i] {
			t.Fatalf("chunk %d differs between two passes over the same bytes", i)
		}
	}
}

// Storage Core Spec: "inserting 1 byte near the start of a 1 GiB file changes
// at most 3 chunks". The property is local, so it is checked here at 64 MiB on
// every run and at the full 1 GiB under -tags slow (TestSlow...).
func TestOneByteInsertChangesAtMostThreeChunks64MiB(t *testing.T) {
	checkInsert(t, 64<<20)
}

func checkInsert(t *testing.T, size int64) {
	g := cdc.DefaultGeometry()
	orig := chunkHashes(t, newStream("insert", size), g)
	if len(orig) < 100 {
		t.Fatalf("%d bytes produced %d chunks; the fixture cannot distinguish anything", size, len(orig))
	}
	for _, at := range []int64{1, 1000, 100 << 10} {
		mod := chunkHashes(t, insertByte(newStream("insert", size), at, 0x5A), g)
		if n := changedChunks(orig, mod); n > 3 || n == 0 {
			t.Errorf("inserting one byte at offset %d of a %d-byte stream changed %d chunks (want 1..3): "+
				"an edit near the start of a file would re-store far more than the edit", at, size, n)
		}
	}
}
