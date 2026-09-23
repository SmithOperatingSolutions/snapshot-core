package dedup_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/dedup"
	"github.com/SmithOperatingSolutions/snapshot-core/core/pack"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
)

var update = flag.Bool("update", false, "rewrite testdata/index_v1.bin")

func goldenKeyring(t *testing.T) *seal.Keyring {
	t.Helper()
	kr, err := seal.KeyringFromBytes(bytes.Repeat([]byte{0x43}, 32))
	if err != nil {
		t.Fatal(err)
	}
	return kr
}

var goldenRepo = seal.RepoID{0x1d, 0x5}

// The records the golden object holds, fixed here.
func goldenPacks() []pack.Info {
	return []pack.Info{{
		Name: dedup.PackName(sha256.Sum256([]byte("golden pack"))),
		Salt: seal.Salt{1, 2, 3},
		Size: 4096,
		Entries: []pack.Entry{
			{Hash: [32]byte{0x10}, Offset: 40, StoredLen: 100, RawLen: 72, Codec: pack.CodecRaw},
			{Hash: [32]byte{0x20}, Offset: 140, StoredLen: 60, RawLen: 500, Codec: pack.CodecZstd},
		},
	}}
}

// An index object written by v1 reads with this code, forever.
func TestAV1IndexObjectStillReads(t *testing.T) {
	kr := goldenKeyring(t)
	path := filepath.Join("testdata", "index_v1.bin")
	if *update {
		_, blob, err := dedup.EncodeObject(kr, goldenRepo, goldenPacks())
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, blob, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	blob, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("golden index object missing (%v): generate with -run TestAV1IndexObjectStillReads -update", err)
	}
	sum := sha256.Sum256(blob)
	got, err := dedup.DecodeObject(kr, goldenRepo, "index/"+hex.EncodeToString(sum[:]), blob)
	if err != nil {
		t.Fatalf("a v1 index object no longer reads (%v): every repository's chunk locations would be lost", err)
	}
	sameInfos(t, got, goldenPacks())
}
