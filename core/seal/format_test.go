package seal_test

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"golang.org/x/crypto/argon2"

	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
)

// These tests re-derive seal's formats with the standard library and x/crypto
// alone and open seal's real output with them. They are the authority for the
// key derivation and associated data (both part of the on-disk contract), and
// they are what sees each layer of domain separation on its own: the domain
// is in the HKDF info AND in the associated data, so a behavioral test passes
// with either layer removed.

func gcmOpen(t *testing.T, key, sealed, aad []byte) ([]byte, error) {
	t.Helper()
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}
	if len(sealed) < 28 {
		t.Fatalf("sealed value is %d bytes, shorter than nonce + tag", len(sealed))
	}
	return gcm.Open(nil, sealed[:12], sealed[12:], aad)
}

func TestObjectKeyDerivationAndAssociatedDataArePinned(t *testing.T) {
	master := bytes.Repeat([]byte{0x33}, 32)
	kr, err := seal.KeyringFromBytes(master)
	if err != nil {
		t.Fatal(err)
	}
	repo := seal.RepoID{9, 8, 7}
	salt := seal.Salt{1, 2, 3}
	for _, d := range domains {
		sealed, err := mustKey(t, kr, d, repo, salt).Seal([]byte("ctx"), []byte("pinned"))
		if err != nil {
			t.Fatal(err)
		}
		key, err := hkdf.Key(sha256.New, master, salt[:], "snapshot-core/object-key/v1\x00"+d.Tag()+"\x00"+string(repo[:]), 32)
		if err != nil {
			t.Fatal(err)
		}
		pt, err := gcmOpen(t, key, sealed, []byte(d.Tag()+"\x00ctx"))
		if err != nil || string(pt) != "pinned" {
			t.Errorf("%s: an independent derivation cannot open seal's output (%v): the object-key derivation "+
				"or associated data changed, and every stored object would become unreadable", d.Tag(), err)
		}
	}
	wantID, err := hkdf.Key(sha256.New, master, nil, "snapshot-core/key-id/v1", 32)
	if err != nil {
		t.Fatal(err)
	}
	if id := kr.ID(); !bytes.Equal(id[:], wantID) {
		t.Errorf("key ID %x, want HKDF(master, \"snapshot-core/key-id/v1\") %x: every repo config names its key by this", id[:6], wantID[:6])
	}
}

func TestKeyFileFormatIsPinned(t *testing.T) {
	master := bytes.Repeat([]byte{0x44}, 32)
	kr, err := seal.KeyringFromBytes(master)
	if err != nil {
		t.Fatal(err)
	}
	f, err := seal.NewKeyFile(kr, []byte("pinned passphrase"), fast)
	if err != nil {
		t.Fatal(err)
	}
	if len(f) != keyFileLen || string(f[:4]) != "SCKF" || binary.LittleEndian.Uint16(f[4:]) != 1 {
		t.Fatalf("key file header % x: want magic SCKF, version 1, %d bytes", f[:6], keyFileLen)
	}
	time := binary.LittleEndian.Uint32(f[offTime:])
	memory := binary.LittleEndian.Uint32(f[offMemory:])
	threads := f[offThreads]
	if time != fast.Time || memory != fast.Memory || threads != fast.Threads {
		t.Fatalf("recorded params %d/%d/%d, want %+v", time, memory, threads, fast)
	}
	kek := argon2.IDKey([]byte("pinned passphrase"), f[offSalt:offKeyID], time, memory, threads, 32)
	got, err := gcmOpen(t, kek, f[offSealed:], append([]byte("vdb/keyfile/v1\x00"), f[:offSealed]...))
	if err != nil || !bytes.Equal(got, master) {
		t.Fatalf("an independent Argon2id + AES-GCM cannot open the key file (%v): the key-file format "+
			"changed, and every stored key file would stop opening", err)
	}
	if id := kr.ID(); !bytes.Equal(f[offKeyID:offSealed], id[:]) {
		t.Fatal("the key file's key id field is not the key's ID")
	}
}
