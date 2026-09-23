package packstore

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/internal/wire"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
)

var update = flag.Bool("update", false, "rewrite testdata/manifest_v1.bin")

// The golden manifest's key and contents are fixed here; its bytes (random
// salt and nonce) were written once and are checked in. It carries what a
// manifest can: several index objects and condemned objects of both kinds,
// which nothing writes until GC (C4) does.
func goldenManifest() manifest {
	idx := [][32]byte{sha256.Sum256([]byte("index c")), sha256.Sum256([]byte("index a")), sha256.Sum256([]byte("index b"))}
	sort.Slice(idx, func(i, j int) bool { return bytes.Compare(idx[i][:], idx[j][:]) < 0 })
	return manifest{
		seq:     7,
		gcGen:   3,
		root:    hash.Sum([]byte("golden root")),
		indexes: idx,
		condemned: []condemned{
			{kind: 1, sum: sha256.Sum256([]byte("a pack")), at: 1_700_000_000_000_000_000},
			{kind: 2, sum: sha256.Sum256([]byte("an index object")), at: 1_700_000_000_500_000_000},
		},
	}
}

func goldenKeyring(t *testing.T) *seal.Keyring {
	t.Helper()
	kr, err := seal.KeyringFromBytes(bytes.Repeat([]byte{0x5c}, 32))
	if err != nil {
		t.Fatal(err)
	}
	return kr
}

var goldenRepo = seal.RepoID{0x3a, 0x11}

func sameManifest(a, b manifest) bool {
	if a.seq != b.seq || a.gcGen != b.gcGen || a.root != b.root ||
		len(a.indexes) != len(b.indexes) || len(a.condemned) != len(b.condemned) {
		return false
	}
	for i := range a.indexes {
		if a.indexes[i] != b.indexes[i] {
			return false
		}
	}
	for i := range a.condemned {
		if a.condemned[i] != b.condemned[i] {
			return false
		}
	}
	return true
}

// sealPlain seals any plaintext as a manifest under the right key: the
// forgery authenticates, so only the plaintext's own validation can refuse it.
func sealPlain(t *testing.T, kr *seal.Keyring, repo seal.RepoID, plain []byte) []byte {
	t.Helper()
	salt, err := seal.NewSalt()
	if err != nil {
		t.Fatal(err)
	}
	key, err := kr.Key(seal.Refs, repo, salt)
	if err != nil {
		t.Fatal(err)
	}
	defer key.Destroy()
	var w wire.Writer
	w.Raw([]byte(manifestMagic))
	w.U16(manifestV1)
	w.Raw(salt[:])
	sealed, err := key.Seal(w.Bytes(), plain)
	if err != nil {
		t.Fatal(err)
	}
	w.Raw(sealed)
	return w.Bytes()
}

// A manifest written by v1 opens with this code, and its plaintext
// re-encodes byte for byte (on-disk formats never change incompatibly). A
// change the encoder and decoder make together passes every round trip;
// only a stored manifest sees it.
func TestAV1ManifestStillOpens(t *testing.T) {
	kr := goldenKeyring(t)
	path := filepath.Join("testdata", "manifest_v1.bin")
	if *update {
		b, err := goldenManifest().seal(kr, goldenRepo)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	m, err := openManifest(b, kr, goldenRepo)
	if err != nil {
		t.Fatalf("the v1 manifest does not open: %v", err)
	}
	if !sameManifest(m, goldenManifest()) {
		t.Fatalf("the v1 manifest opened as %+v, want %+v", m, goldenManifest())
	}
	var salt seal.Salt
	copy(salt[:], b[6:headerLen])
	key, err := kr.Key(seal.Refs, goldenRepo, salt)
	if err != nil {
		t.Fatal(err)
	}
	defer key.Destroy()
	plain, err := key.Open(b[:headerLen], b[headerLen:])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(m.encodePlain(), plain) {
		t.Fatal("the v1 plaintext does not re-encode to the stored bytes")
	}
}

// plainParts spells out a manifest plaintext field by field, so a forgery
// can say exactly what it breaks.
type plainParts struct {
	magic      string
	version    uint16
	nIndexes   uint64
	indexes    [][32]byte
	nCondemned uint64
	condemned  []condemned
	trim       int // bytes cut from the end
	tail       []byte
}

func partsOf(m manifest) plainParts {
	return plainParts{magic: plainMagic, version: manifestV1,
		nIndexes: uint64(len(m.indexes)), indexes: append([][32]byte(nil), m.indexes...),
		nCondemned: uint64(len(m.condemned)), condemned: append([]condemned(nil), m.condemned...)}
}

func (p plainParts) bytes(m manifest) []byte {
	var w wire.Writer
	w.Raw([]byte(p.magic))
	w.U16(p.version)
	w.U64(m.seq)
	w.U64(m.gcGen)
	w.Raw(m.root[:])
	w.Uvarint(p.nIndexes)
	for _, s := range p.indexes {
		w.Raw(s[:])
	}
	w.Uvarint(p.nCondemned)
	for _, c := range p.condemned {
		w.U8(c.kind)
		w.Raw(c.sum[:])
		w.U64(uint64(c.at))
	}
	b := w.Bytes()
	return append(b[:len(b)-p.trim], p.tail...)
}

func TestForgedManifestsAreRefused(t *testing.T) {
	kr, repo, g := goldenKeyring(t), goldenRepo, goldenManifest()
	honest := partsOf(g)
	if m, err := openManifest(sealPlain(t, kr, repo, honest.bytes(g)), kr, repo); err != nil || !sameManifest(m, g) {
		t.Fatalf("positive control: an honest plaintext sealed by the test does not open (%v)", err)
	}
	forge := func(f func(p *plainParts)) []byte {
		p := partsOf(g)
		f(&p)
		return sealPlain(t, kr, repo, p.bytes(g))
	}
	for name, b := range map[string][]byte{
		"plaintext magic":          forge(func(p *plainParts) { p.magic = "SCMX" }),
		"plaintext version 2":      forge(func(p *plainParts) { p.version = 2 }),
		"too many index objects":   forge(func(p *plainParts) { p.nIndexes = maxIndexes + 1 }),
		"index count past the end": forge(func(p *plainParts) { p.nIndexes++ }),
		"unsorted index objects":   forge(func(p *plainParts) { p.indexes[0], p.indexes[1] = p.indexes[1], p.indexes[0] }),
		"duplicate index object":   forge(func(p *plainParts) { p.indexes[1] = p.indexes[0] }),
		"too many condemned":       forge(func(p *plainParts) { p.nCondemned = maxCondemned + 1 }),
		"condemned kind 0":         forge(func(p *plainParts) { p.condemned[0].kind = 0 }),
		"condemned kind 5":         forge(func(p *plainParts) { p.condemned[1].kind = 5 }),
		"truncated condemned":      forge(func(p *plainParts) { p.trim = 1 }),
		"a byte past the end":      forge(func(p *plainParts) { p.tail = []byte{0} }),
	} {
		if m, err := openManifest(b, kr, repo); !errors.Is(err, ErrManifest) {
			t.Errorf("%s: opened as %+v (err=%v), want ErrManifest", name, m, err)
		}
	}
	good := sealPlain(t, kr, repo, honest.bytes(g))
	altered := bytes.Clone(good)
	altered[len(altered)-1] ^= 1
	for name, b := range map[string][]byte{
		"shorter than the header": good[:headerLen-1],
		"outer magic":             append([]byte("SCMX"), good[4:]...),
		"ciphertext altered":      altered,
	} {
		if _, err := openManifest(b, kr, repo); !errors.Is(err, ErrManifest) {
			t.Errorf("%s: err=%v, want ErrManifest", name, err)
		}
	}
}

// Whatever decodes as a manifest plaintext re-encodes to the same bytes:
// the plaintext has one encoding per value.
func FuzzDecodePlain(f *testing.F) {
	f.Add(goldenManifest().encodePlain())
	f.Add(manifest{}.encodePlain())
	f.Add([]byte(plainMagic))
	f.Fuzz(func(t *testing.T, b []byte) {
		m, err := decodePlain(b)
		if err != nil {
			return
		}
		if !bytes.Equal(m.encodePlain(), b) {
			t.Fatal("a manifest plaintext decoded that does not re-encode to itself")
		}
	})
}

// openManifest never panics on hostile bytes, and whatever opens is a
// manifest that re-encodes canonically.
func FuzzOpenManifest(f *testing.F) {
	kr, err := seal.KeyringFromBytes(bytes.Repeat([]byte{0x5c}, 32))
	if err != nil {
		f.Fatal(err)
	}
	if b, err := os.ReadFile(filepath.Join("testdata", "manifest_v1.bin")); err == nil {
		f.Add(b)
	}
	f.Add([]byte(manifestMagic))
	f.Fuzz(func(t *testing.T, b []byte) {
		m, err := openManifest(b, kr, goldenRepo)
		if err != nil {
			return
		}
		if _, err := decodePlain(m.encodePlain()); err != nil {
			t.Fatalf("an opened manifest does not re-decode: %v", err)
		}
	})
}
