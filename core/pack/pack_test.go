package pack_test

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/pack"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
)

var repo = seal.RepoID{7}

func fixture(t *testing.T) (*seal.Keyring, *pack.Codec) {
	t.Helper()
	kr, err := seal.NewKeyring()
	if err != nil {
		t.Fatal(err)
	}
	c, err := pack.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return kr, c
}

func randomish(seed string, n int) []byte {
	out := make([]byte, 0, n+32)
	for i := 0; len(out) < n; i++ {
		h := sha256.Sum256([]byte(fmt.Sprintf("%s/%d", seed, i)))
		out = append(out, h[:]...)
	}
	return out[:n]
}

type chunk struct {
	h    hash.Hash
	data []byte
}

func chunks() []chunk {
	var cs []chunk
	for _, d := range [][]byte{
		[]byte(strings.Repeat("compressible text ", 2000)),
		randomish("random", 70000),
		{},
		[]byte("tiny"),
		randomish("max", pack.MaxChunkSize),
	} {
		cs = append(cs, chunk{h: hash.Sum(d), data: d})
	}
	return cs
}

func build(t *testing.T, kr *seal.Keyring, c *pack.Codec, cs []chunk) pack.Built {
	t.Helper()
	w, err := pack.NewWriter(kr, repo, c, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	for _, ch := range cs {
		if err := w.Add(ch.h, ch.data); err != nil {
			t.Fatalf("Add(%d bytes): %v", len(ch.data), err)
		}
	}
	b, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func entryFor(t *testing.T, info pack.Info, h hash.Hash) pack.Entry {
	t.Helper()
	for _, e := range info.Entries {
		if e.Hash == h {
			return e
		}
	}
	t.Fatalf("no entry for %s", h.Short())
	return pack.Entry{}
}

func TestRoundTripEveryChunkThroughTheIndex(t *testing.T) {
	kr, c := fixture(t)
	cs := chunks()
	b := build(t, kr, c, cs)
	info, err := pack.ReadInfo(b.Name, b.Bytes, kr, repo)
	if err != nil {
		t.Fatalf("ReadInfo of a fresh pack: %v", err)
	}
	if info.Name != b.Name || info.Size != int64(len(b.Bytes)) || len(info.Entries) != len(cs) {
		t.Fatalf("ReadInfo = %s/%d bytes/%d entries, want %s/%d/%d", info.Name, info.Size, len(info.Entries),
			b.Name, len(b.Bytes), len(cs))
	}
	if info.Salt != b.Info.Salt {
		t.Fatal("ReadInfo recovered a different salt than the writer used")
	}
	keys, err := pack.DeriveKeys(kr, repo, info.Salt)
	if err != nil {
		t.Fatal(err)
	}
	for _, ch := range cs {
		e := entryFor(t, info, ch.h)
		frame := b.Bytes[e.Offset : e.Offset+e.StoredLen]
		got, err := pack.OpenFrame(keys, c, e, frame)
		if err != nil || !bytes.Equal(got, ch.data) {
			t.Fatalf("chunk of %d bytes read back as %d bytes (%v): a stored chunk would not read as written",
				len(ch.data), len(got), err)
		}
	}
	for i := 1; i < len(info.Entries); i++ {
		if info.Entries[i-1].Hash.Compare(info.Entries[i].Hash) >= 0 {
			t.Fatal("index entries are not sorted by hash")
		}
	}
}

func TestNameIsTheShardedHashOfTheBytes(t *testing.T) {
	kr, c := fixture(t)
	b := build(t, kr, c, chunks()[:2])
	sum := sha256.Sum256(b.Bytes)
	want := fmt.Sprintf("packs/%x/%x", sum[:1], sum)
	if b.Name != want || pack.Name(b.Bytes) != want {
		t.Fatalf("pack name %q, want %q (SHA-256 of the pack bytes, sharded by the first byte)", b.Name, want)
	}
}

func TestCompressesOnlyWhenItHelps(t *testing.T) {
	kr, c := fixture(t)
	cs := chunks()
	b := build(t, kr, c, cs[:2])
	text := entryFor(t, b.Info, cs[0].h)
	random := entryFor(t, b.Info, cs[1].h)
	if text.Codec != pack.CodecZstd || text.StoredLen >= text.RawLen/4 {
		t.Errorf("36 KB of repeated text stored as codec %d, %d bytes: it should compress", text.Codec, text.StoredLen)
	}
	if random.Codec != pack.CodecRaw || random.StoredLen != random.RawLen+28 {
		t.Errorf("random bytes stored as codec %d, %d bytes for %d: incompressible data should be stored raw",
			random.Codec, random.StoredLen, random.RawLen)
	}
}

// Security table: "a test asserts no plaintext hash appears on disk". Neither
// a chunk's hash nor its bytes may appear in the pack.
func TestNoPlaintextHashOrBytesInThePack(t *testing.T) {
	kr, c := fixture(t)
	cs := chunks()
	b := build(t, kr, c, cs)
	// An empty pack trivially leaks nothing: first prove this one holds them.
	if info, err := pack.ReadInfo(b.Name, b.Bytes, kr, repo); err != nil || len(info.Entries) != len(cs) {
		t.Fatalf("positive control: the pack must hold all %d chunks before absence means anything (err=%v)", len(cs), err)
	}
	checked := 0
	for _, ch := range cs {
		if bytes.Contains(b.Bytes, ch.h[:]) {
			t.Errorf("chunk hash %s appears in the pack bytes", ch.h.Short())
		}
		if len(ch.data) >= 16 && bytes.Contains(b.Bytes, ch.data[:16]) {
			t.Errorf("the first 16 bytes of a %d-byte chunk appear in the pack", len(ch.data))
		}
		checked++
	}
	if checked != len(cs) {
		t.Fatal("checked nothing")
	}
}

func TestTamperingAnywhereIsCorrupt(t *testing.T) {
	kr, c := fixture(t)
	cs := chunks()[:3]
	b := build(t, kr, c, cs)
	keys, err := pack.DeriveKeys(kr, repo, b.Info.Salt)
	if err != nil {
		t.Fatal(err)
	}
	e := entryFor(t, b.Info, cs[0].h)
	frame := bytes.Clone(b.Bytes[e.Offset : e.Offset+e.StoredLen])
	if _, err := pack.OpenFrame(keys, c, e, frame); err != nil {
		t.Fatalf("positive control: %v", err)
	}
	frame[len(frame)/2] ^= 1
	if _, err := pack.OpenFrame(keys, c, e, frame); !errors.Is(err, pack.ErrCorrupt) {
		t.Errorf("a frame with a flipped byte = %v, want ErrCorrupt", err)
	}
	// A frame presented as another chunk fails: frames are bound to hashes.
	other := entryFor(t, b.Info, cs[1].h)
	swapped := other
	swapped.Offset, swapped.StoredLen, swapped.Codec, swapped.RawLen = e.Offset, e.StoredLen, e.Codec, e.RawLen
	if _, err := pack.OpenFrame(keys, c, swapped, b.Bytes[e.Offset:e.Offset+e.StoredLen]); !errors.Is(err, pack.ErrCorrupt) {
		t.Errorf("chunk A's frame opened as chunk B (err=%v): an index entry could be pointed at the wrong frame", err)
	}
	// Each tampered pack is offered under the name its bytes hash to, so the
	// name check passes and the header, index and trailer checks must catch it.
	for name, i := range map[string]int{
		"magic":        0,
		"version":      4,
		"flags":        6,
		"header salt":  10,
		"index":        len(b.Bytes) - pack.TrailerSize - 5,
		"trailer":      len(b.Bytes) - 1,
		"index offset": len(b.Bytes) - 16,
		"index length": len(b.Bytes) - 8,
	} {
		bad := bytes.Clone(b.Bytes)
		bad[i] ^= 1
		if _, err := pack.ReadInfo(pack.Name(bad), bad, kr, repo); !errors.Is(err, pack.ErrCorrupt) {
			t.Errorf("a pack with its %s flipped: ReadInfo = %v, want ErrCorrupt", name, err)
		}
	}
	for _, n := range []int{0, 10, pack.HeaderSize, pack.HeaderSize + pack.TrailerSize, len(b.Bytes) - 1} {
		cut := b.Bytes[:n]
		if _, err := pack.ReadInfo(pack.Name(cut), cut, kr, repo); !errors.Is(err, pack.ErrCorrupt) {
			t.Errorf("a pack truncated to %d bytes: ReadInfo = %v, want ErrCorrupt", n, err)
		}
	}
	if _, err := pack.ReadInfo("packs/00/"+strings.Repeat("0", 64), b.Bytes, kr, repo); !errors.Is(err, pack.ErrCorrupt) {
		t.Errorf("a pack whose bytes do not hash to its name: ReadInfo = %v, want ErrCorrupt", err)
	}
}

func TestPacksAreBoundToTheirRepoAndKey(t *testing.T) {
	kr, c := fixture(t)
	b := build(t, kr, c, chunks()[:2])
	if _, err := pack.ReadInfo(b.Name, b.Bytes, kr, repo); err != nil {
		t.Fatalf("positive control: %v", err)
	}
	if _, err := pack.ReadInfo(b.Name, b.Bytes, kr, seal.RepoID{8}); !errors.Is(err, pack.ErrCorrupt) {
		t.Errorf("another repository's keys read this pack's index (err=%v)", err)
	}
	other, _ := seal.NewKeyring()
	if _, err := pack.ReadInfo(b.Name, b.Bytes, other, repo); !errors.Is(err, pack.ErrCorrupt) {
		t.Errorf("another master key read this pack's index (err=%v)", err)
	}
}

func TestWriterLimits(t *testing.T) {
	kr, c := fixture(t)
	w, err := pack.NewWriter(kr, repo, c, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	big := make([]byte, pack.MaxChunkSize+1)
	if err := w.Add(hash.Sum(big), big); !errors.Is(err, pack.ErrTooLarge) {
		t.Errorf("a %d-byte chunk = %v, want ErrTooLarge (Engine Spec: 1 MiB limit)", len(big), err)
	}
	d := []byte("once")
	if err := w.Add(hash.Sum(d), d); err != nil {
		t.Fatal(err)
	}
	if err := w.Add(hash.Sum(d), d); !errors.Is(err, pack.ErrDup) {
		t.Errorf("the same chunk twice = %v, want ErrDup", err)
	}
	if w.Count() != 1 || !w.Has(hash.Sum(d)) {
		t.Errorf("Count = %d, Has = %v after one chunk", w.Count(), w.Has(hash.Sum(d)))
	}
	if got, ok, err := w.Get(hash.Sum(d)); err != nil || !ok || string(got) != "once" {
		t.Errorf("reading back a chunk from the unfinished pack = %q, %v, %v", got, ok, err)
	}
	small, err := pack.NewWriter(kr, repo, c, 4096)
	if err != nil {
		t.Fatal(err)
	}
	r := randomish("fill", 3000)
	if err := small.Add(hash.Sum(r), r); err != nil {
		t.Fatalf("positive control: %v", err)
	}
	r2 := randomish("fill2", 3000)
	if err := small.Add(hash.Sum(r2), r2); !errors.Is(err, pack.ErrFull) {
		t.Errorf("a chunk past the pack's size limit = %v, want ErrFull (start another pack)", err)
	}
	if small.Size() > 4096 {
		t.Errorf("the pack grew to %d bytes past its 4096 limit", small.Size())
	}
	if _, err := pack.NewWriter(kr, repo, c, pack.MaxPackSize+1); err == nil {
		t.Error("a writer larger than MaxPackSize was accepted")
	}
}

// A finished writer takes nothing more: its chunks are in the pack it
// returned, and a second Finish would be a second pack under the first one's
// salt, and so under the same keys.
func TestAFinishedWriterRefusesMore(t *testing.T) {
	kr, c := fixture(t)
	w, err := pack.NewWriter(kr, repo, c, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	d := []byte("in the pack")
	if err := w.Add(hash.Sum(d), d); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Finish(); err != nil {
		t.Fatalf("positive control: %v", err)
	}
	late := []byte("after Finish")
	if err := w.Add(hash.Sum(late), late); err == nil {
		t.Error("a finished writer accepted another chunk, which no pack holds")
	}
	if b, err := w.Finish(); err == nil {
		t.Errorf("a finished writer finished again, a second pack (%s) under the first one's salt", b.Name)
	}
}

func TestSaltsAreFreshPerPack(t *testing.T) {
	kr, c := fixture(t)
	a := build(t, kr, c, chunks()[:1])
	b := build(t, kr, c, chunks()[:1])
	if a.Info.Salt == b.Info.Salt || a.Name == b.Name {
		t.Fatal("two packs share a salt: their keys would be the same")
	}
}

func FuzzReadInfo(f *testing.F) {
	kr, _ := seal.KeyringFromBytes(bytes.Repeat([]byte{3}, 32))
	c, _ := pack.NewCodec()
	w, err := pack.NewWriter(kr, repo, c, 1<<20)
	if err == nil {
		_ = w.Add(hash.Sum([]byte("seed")), []byte("seed"))
		if b, err := w.Finish(); err == nil {
			f.Add(b.Bytes)
		}
	}
	f.Add([]byte("SCPK"))
	f.Fuzz(func(t *testing.T, b []byte) {
		_, _ = pack.ReadInfo(pack.Name(b), b, kr, repo) // never panics
	})
}

// D13: a chunk under RawBelow is stored raw even when zstd would shrink it,
// and one at RawBelow is compressed; a pack holding both kinds of frame
// reads back, as the v1 golden pack (raw and zstd frames) still does.
func TestAChunkUnderTheRawCutoffIsStoredRaw(t *testing.T) {
	kr, c := fixture(t)
	text := func(n int) []byte { return []byte(strings.Repeat("compressible text ", n/18+1)[:n]) }
	under, at := text(pack.RawBelow-1), text(pack.RawBelow)
	cs := []chunk{
		{hash.Sum(under), under},
		{hash.Sum(at), at},
		{hash.Sum([]byte(strings.Repeat("compressible text ", 2000))), []byte(strings.Repeat("compressible text ", 2000))},
		{hash.Sum(randomish("mixed", 5000)), randomish("mixed", 5000)},
	}
	b := build(t, kr, c, cs)
	u, a := entryFor(t, b.Info, cs[0].h), entryFor(t, b.Info, cs[1].h)
	if a.Codec != pack.CodecZstd || a.StoredLen >= a.RawLen {
		t.Fatalf("%d bytes of repeated text (at the cutoff) stored as codec %d, %d bytes: a chunk at the cutoff should still compress",
			a.RawLen, a.Codec, a.StoredLen)
	}
	if u.Codec != pack.CodecRaw || u.StoredLen != u.RawLen+pack.FrameOverhead {
		t.Errorf("%d bytes of repeated text (under the cutoff) stored as codec %d, %d bytes: a tiny chunk should be stored raw, not pay the zstd encoder",
			u.RawLen, u.Codec, u.StoredLen)
	}
	// The packstore's Prepare compresses through Codec.Compress directly.
	if _, codec := c.Compress(under); codec != pack.CodecRaw {
		t.Errorf("Compress(%d bytes) chose codec %d: the prepare path would compress a tiny chunk", len(under), codec)
	}
	if _, codec := c.Compress(at); codec != pack.CodecZstd {
		t.Errorf("Compress(%d bytes) chose codec %d: the prepare path would stop compressing at the cutoff", len(at), codec)
	}

	info, err := pack.ReadInfo(b.Name, b.Bytes, kr, repo)
	if err != nil {
		t.Fatalf("a pack mixing raw and zstd frames does not read: %v", err)
	}
	keys, err := pack.DeriveKeys(kr, repo, info.Salt)
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[uint8]int{}
	for _, ch := range cs {
		e := entryFor(t, info, ch.h)
		kinds[e.Codec]++
		got, err := pack.OpenFrame(keys, c, e, b.Bytes[e.Offset:e.Offset+e.StoredLen])
		if err != nil || !bytes.Equal(got, ch.data) {
			t.Fatalf("a %d-byte chunk in a mixed pack reads as %d bytes (%v)", len(ch.data), len(got), err)
		}
	}
	if kinds[pack.CodecRaw] == 0 || kinds[pack.CodecZstd] == 0 {
		t.Fatalf("the pack holds frames of codecs %v: the fixture must mix raw and zstd", kinds)
	}

	// The v1 golden pack, written before the cutoff, also holds both kinds.
	g, err := os.ReadFile(filepath.Join("testdata", "pack_v1.bin"))
	if err != nil {
		t.Fatal(err)
	}
	gi, err := pack.ReadInfo(pack.Name(g), g, goldenKeyring(t), goldenRepo)
	if err != nil {
		t.Fatalf("the v1 golden pack does not read: %v", err)
	}
	gk := map[uint8]int{}
	for _, e := range gi.Entries {
		gk[e.Codec]++
	}
	if gk[pack.CodecRaw] == 0 || gk[pack.CodecZstd] == 0 {
		t.Fatalf("the v1 golden pack holds codecs %v: it no longer proves an old reader's two frame kinds", gk)
	}
}
