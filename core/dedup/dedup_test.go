package dedup_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/dedup"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/pack"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
)

var repo = seal.RepoID{9}

func keyring(t *testing.T) *seal.Keyring {
	t.Helper()
	kr, err := seal.NewKeyring()
	if err != nil {
		t.Fatal(err)
	}
	return kr
}

// realPacks builds actual packs, so the records are ones a store would write.
func realPacks(t *testing.T, kr *seal.Keyring, n, per int) []pack.Built {
	t.Helper()
	c, err := pack.NewCodec()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var out []pack.Built
	for p := 0; p < n; p++ {
		w, err := pack.NewWriter(kr, repo, c, 16<<20)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < per; i++ {
			d := []byte(fmt.Sprintf("pack %d chunk %d %s", p, i, strings.Repeat("x", i)))
			if err := w.Add(hash.Sum(d), d); err != nil {
				t.Fatal(err)
			}
		}
		b, err := w.Finish()
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, b)
	}
	return out
}

func infos(bs []pack.Built) []pack.Info {
	var is []pack.Info
	for _, b := range bs {
		is = append(is, b.Info)
	}
	return is
}

func sameInfos(t *testing.T, got, want []pack.Info) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("decoded %d pack records, want %d", len(got), len(want))
	}
	byName := map[string]pack.Info{}
	for _, g := range got {
		byName[g.Name] = g
	}
	for _, w := range want {
		g, ok := byName[w.Name]
		if !ok {
			t.Fatalf("pack %s missing from the decoded object", w.Name)
		}
		if g.Salt != w.Salt || g.Size != w.Size || len(g.Entries) != len(w.Entries) {
			t.Fatalf("pack %s decoded as salt-equal=%v size %d/%d entries %d/%d", w.Name, g.Salt == w.Salt,
				g.Size, w.Size, len(g.Entries), len(w.Entries))
		}
		for i := range w.Entries {
			if g.Entries[i] != w.Entries[i] {
				t.Fatalf("pack %s entry %d = %+v, want %+v", w.Name, i, g.Entries[i], w.Entries[i])
			}
		}
	}
}

func TestIndexObjectRoundTrip(t *testing.T) {
	kr := keyring(t)
	packs := infos(realPacks(t, kr, 3, 20))
	name, blob, err := dedup.EncodeObject(kr, repo, packs)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(blob)
	if name != "index/"+hex.EncodeToString(sum[:]) {
		t.Fatalf("index object name %q, want index/<sha256 of its bytes>", name)
	}
	got, err := dedup.DecodeObject(kr, repo, name, blob)
	if err != nil {
		t.Fatalf("DecodeObject of a fresh object: %v", err)
	}
	sameInfos(t, got, packs)
}

func TestPackNameFromHash(t *testing.T) {
	kr := keyring(t)
	b := realPacks(t, kr, 1, 1)[0]
	if got := dedup.PackName(sha256.Sum256(b.Bytes)); got != b.Name {
		t.Fatalf("PackName = %q, want %q (the pack's own name)", got, b.Name)
	}
}

// Security table: "the core encrypts its own index files; a test asserts no
// plaintext hash appears on disk".
func TestNoPlaintextHashInAnIndexObject(t *testing.T) {
	kr := keyring(t)
	bs := realPacks(t, kr, 2, 30)
	packs := infos(bs)
	name, blob, err := dedup.EncodeObject(kr, repo, packs)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := dedup.DecodeObject(kr, repo, name, blob); err != nil || len(got) != 2 {
		t.Fatalf("positive control: the object must hold both packs (err=%v)", err)
	}
	checked := 0
	for _, p := range packs {
		for _, e := range p.Entries {
			if bytes.Contains(blob, e.Hash[:]) {
				t.Fatalf("chunk hash %s appears in the index object in the clear", e.Hash.Short())
			}
			checked++
		}
		if bytes.Contains(blob, p.Salt[:]) {
			t.Fatal("a pack's salt appears in the index object in the clear")
		}
	}
	if checked != 60 {
		t.Fatalf("checked %d hashes, want 60", checked)
	}
}

func TestIndexObjectsRefuseTamperingAndStrangers(t *testing.T) {
	kr := keyring(t)
	packs := infos(realPacks(t, kr, 1, 5))
	name, blob, err := dedup.EncodeObject(kr, repo, packs)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := dedup.DecodeObject(kr, repo, name, blob); err != nil || len(got) != 1 || len(blob) < 64 {
		t.Fatalf("positive control: a fresh %d-byte index object must decode to its pack (err=%v)", len(blob), err)
	}
	for _, i := range []int{0, 5, 30, len(blob) / 2, len(blob) - 1} {
		bad := bytes.Clone(blob)
		bad[i] ^= 1
		sum := sha256.Sum256(bad)
		if _, err := dedup.DecodeObject(kr, repo, "index/"+hex.EncodeToString(sum[:]), bad); !errors.Is(err, dedup.ErrCorrupt) {
			t.Errorf("byte %d flipped (offered under its own hash): %v, want ErrCorrupt", i, err)
		}
	}
	if _, err := dedup.DecodeObject(kr, repo, "index/"+strings.Repeat("0", 64), blob); !errors.Is(err, dedup.ErrCorrupt) {
		t.Errorf("a blob that does not hash to its name: %v, want ErrCorrupt", err)
	}
	if _, err := dedup.DecodeObject(kr, seal.RepoID{10}, name, blob); !errors.Is(err, dedup.ErrCorrupt) {
		t.Errorf("another repository opened the index object (err=%v)", err)
	}
	if _, err := dedup.DecodeObject(keyring(t), repo, name, blob); !errors.Is(err, dedup.ErrCorrupt) {
		t.Errorf("another master key opened the index object (err=%v)", err)
	}
}

func TestIndexLocatesEveryChunkOnce(t *testing.T) {
	kr := keyring(t)
	bs := realPacks(t, kr, 3, 10)
	x := dedup.New()
	for _, b := range bs {
		x.Add(b.Info)
	}
	if x.Len() != 30 || len(x.Packs()) != 3 {
		t.Fatalf("Len = %d, Packs = %d; want 30 and 3", x.Len(), len(x.Packs()))
	}
	for _, b := range bs {
		for _, e := range b.Info.Entries {
			loc, ok := x.Lookup(e.Hash)
			if !ok || !x.Has(e.Hash) {
				t.Fatalf("chunk %s is not located", e.Hash.Short())
			}
			if loc.Pack.Name != b.Name || loc.Pack.Salt != b.Info.Salt || loc.Pack.Size != b.Info.Size || loc.Entry != e {
				t.Fatalf("chunk %s located in %s at %+v, want %s at %+v", e.Hash.Short(), loc.Pack.Name, loc.Entry, b.Name, e)
			}
		}
	}
	if _, ok := x.Lookup(hash.Sum([]byte("never stored"))); ok || x.Has(hash.Sum([]byte("never stored"))) {
		t.Fatal("a chunk that was never stored is located")
	}
}

// The same chunk in two packs (two writers raced) is one chunk, located in
// the pack that was added first.
func TestDuplicateChunksKeepTheirFirstLocation(t *testing.T) {
	kr := keyring(t)
	a := realPacks(t, kr, 1, 3)[0]
	b := realPacks(t, kr, 1, 3)[0] // the same three chunks, another pack
	x := dedup.New()
	x.Add(a.Info)
	x.Add(b.Info)
	if x.Len() != 3 {
		t.Fatalf("three chunks in two packs counted as %d", x.Len())
	}
	for _, e := range a.Info.Entries {
		if loc, _ := x.Lookup(e.Hash); loc.Pack.Name != a.Name {
			t.Fatalf("a duplicated chunk moved to the later pack %s", loc.Pack.Name)
		}
	}
}

func FuzzDecodeObject(f *testing.F) {
	kr, _ := seal.KeyringFromBytes(bytes.Repeat([]byte{5}, 32))
	if name, blob, err := dedup.EncodeObject(kr, repo, nil); err == nil && name != "" {
		f.Add(blob)
	}
	f.Add([]byte("SCIX"))
	f.Fuzz(func(t *testing.T, b []byte) {
		sum := sha256.Sum256(b)
		_, _ = dedup.DecodeObject(kr, repo, "index/"+hex.EncodeToString(sum[:]), b) // never panics
	})
}
