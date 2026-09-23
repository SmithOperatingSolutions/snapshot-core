package pack_test

import (
	"bytes"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/pack"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
)

var update = flag.Bool("update", false, "rewrite testdata/pack_v1.bin")

// The golden pack's key and contents are fixed here; its bytes (random salt
// and nonces) were written once and are checked in.
func goldenChunks() [][]byte {
	return [][]byte{
		[]byte(strings.Repeat("a v1 pack must read forever ", 300)),
		randomish("golden", 5000),
		{},
	}
}

func goldenKeyring(t *testing.T) *seal.Keyring {
	t.Helper()
	kr, err := seal.KeyringFromBytes(bytes.Repeat([]byte{0x42}, 32))
	if err != nil {
		t.Fatal(err)
	}
	return kr
}

var goldenRepo = seal.RepoID{0x60, 0x0d}

// A pack written by v1 reads with this code, byte for byte (on-disk formats
// never change incompatibly). A change to the format that the writer and
// reader make together passes every round-trip test; only a stored pack sees it.
func TestAV1PackStillReads(t *testing.T) {
	kr := goldenKeyring(t)
	path := filepath.Join("testdata", "pack_v1.bin")
	if *update {
		c, err := pack.NewCodec()
		if err != nil {
			t.Fatal(err)
		}
		w, err := pack.NewWriter(kr, goldenRepo, c, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range goldenChunks() {
			if err := w.Add(hash.Sum(d), d); err != nil {
				t.Fatal(err)
			}
		}
		b, err := w.Finish()
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, b.Bytes, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("golden pack missing (%v): generate with `go test ./core/pack -run TestAV1PackStillReads -update`", err)
	}
	info, err := pack.ReadInfo(pack.Name(b), b, kr, goldenRepo)
	if err != nil {
		t.Fatalf("a v1 pack no longer reads (%v): every repository written so far would be unreadable", err)
	}
	c, err := pack.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	keys, err := pack.DeriveKeys(kr, goldenRepo, info.Salt)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range goldenChunks() {
		e := entryFor(t, info, hash.Sum(d))
		got, err := pack.OpenFrame(keys, c, e, b[e.Offset:e.Offset+e.StoredLen])
		if err != nil || !bytes.Equal(got, d) {
			t.Fatalf("a v1 chunk of %d bytes reads as %d bytes (%v)", len(d), len(got), err)
		}
	}
}

// A trailer pointing past the end must be corrupt, never a slice out of range.
func TestTrailerCannotPointOutsideThePack(t *testing.T) {
	kr, c := fixture(t)
	b := build(t, kr, c, chunks()[:2])
	for name, i := range map[string]int{
		"index offset high byte": len(b.Bytes) - pack.TrailerSize + 7,
		"index length high byte": len(b.Bytes) - pack.TrailerSize + 11,
	} {
		bad := bytes.Clone(b.Bytes)
		bad[i] ^= 0x40
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("%s: ReadInfo panicked (%v): a crafted pack could crash every reader", name, r)
				}
			}()
			if _, err := pack.ReadInfo(pack.Name(bad), bad, kr, repo); !errors.Is(err, pack.ErrCorrupt) {
				t.Errorf("%s: ReadInfo = %v, want ErrCorrupt", name, err)
			}
		}()
	}
}
