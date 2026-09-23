package seal_test

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal/contract"
)

var (
	repoA = seal.RepoID{1}
	repoB = seal.RepoID{2}
	// The cheapest parameters seal accepts; tests pay ~40 ms per derivation.
	fast = seal.Argon2Params{Time: 2, Memory: 19 * 1024, Threads: 1}
)

var domains = []seal.Domain{seal.Chunk, seal.PackIndex, seal.Index, seal.Refs, seal.Config}

func mustKeyring(t *testing.T) *seal.Keyring {
	t.Helper()
	kr, err := seal.NewKeyring()
	if err != nil {
		t.Fatal(err)
	}
	return kr
}

func mustSalt(t *testing.T) seal.Salt {
	t.Helper()
	s, err := seal.NewSalt()
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func mustKey(t *testing.T, kr *seal.Keyring, d seal.Domain, repo seal.RepoID, salt seal.Salt) *seal.Key {
	t.Helper()
	k, err := kr.Key(d, repo, salt)
	if err != nil {
		t.Fatalf("Key(%v): %v", d, err)
	}
	return k
}

func TestDomainTagsAreTheDocumentedOnes(t *testing.T) {
	want := map[seal.Domain]string{
		seal.Chunk: "vdb/chunk/v1", seal.PackIndex: "vdb/pack-index/v1", seal.Index: "vdb/index/v1",
		seal.Refs: "vdb/refs/v1", seal.Config: "vdb/config/v1",
	}
	for d, tag := range want {
		if d.Tag() != tag {
			t.Errorf("domain %d tag = %q, want %q (tags are part of the on-disk format)", d, d.Tag(), tag)
		}
	}
	if seal.Domain(0).Tag() != "" || seal.Domain(200).Tag() != "" {
		t.Error("an invalid domain has a tag")
	}
}

func TestSealOpenRoundTripInEveryDomain(t *testing.T) {
	kr := mustKeyring(t)
	salt := mustSalt(t)
	for _, d := range domains {
		k := mustKey(t, kr, d, repoA, salt)
		pt := []byte("plaintext for " + d.Tag())
		sealed, err := k.Seal([]byte("ctx"), pt)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(sealed, pt) {
			t.Fatalf("%s: the sealed output contains the plaintext", d.Tag())
		}
		if len(sealed) != len(pt)+28 {
			t.Fatalf("%s: sealed %d bytes for %d of plaintext, want +28 (nonce and tag)", d.Tag(), len(sealed), len(pt))
		}
		got, err := k.Open([]byte("ctx"), sealed)
		if err != nil || !bytes.Equal(got, pt) {
			t.Fatalf("%s: Open(Seal(x)) = %q, %v", d.Tag(), got, err)
		}
	}
}

// Same master, same repo, same salt: only the domain differs.
func TestDomainsAreSeparated(t *testing.T) {
	kr := mustKeyring(t)
	salt := mustSalt(t)
	sealed, err := mustKey(t, kr, seal.Chunk, repoA, salt).Seal(nil, []byte("chunk"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mustKey(t, kr, seal.Chunk, repoA, salt).Open(nil, sealed); err != nil {
		t.Fatalf("positive control: the same domain did not open: %v", err)
	}
	for _, d := range domains[1:] {
		if _, err := mustKey(t, kr, d, repoA, salt).Open(nil, sealed); !errors.Is(err, seal.ErrAuth) {
			t.Errorf("a chunk sealed value opened under %s (err=%v): a ciphertext could be replayed across contexts",
				d.Tag(), err)
		}
	}
}

func TestContextIsBound(t *testing.T) {
	k := mustKey(t, mustKeyring(t), seal.Chunk, repoA, mustSalt(t))
	sealed, err := k.Seal([]byte("chunk A"), []byte("payload"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.Open([]byte("chunk A"), sealed); err != nil {
		t.Fatalf("positive control: %v", err)
	}
	if _, err := k.Open([]byte("chunk B"), sealed); !errors.Is(err, seal.ErrAuth) {
		t.Fatalf("a frame sealed for chunk A opened as chunk B (err=%v)", err)
	}
}

// Derivation is deterministic in (master, domain, repo, salt) and changes
// with each of them.
func TestKeysAreBoundToMasterRepoAndSalt(t *testing.T) {
	kr := mustKeyring(t)
	salt := mustSalt(t)
	sealed, err := mustKey(t, kr, seal.Refs, repoA, salt).Seal(nil, []byte("root"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mustKey(t, kr, seal.Refs, repoA, salt).Open(nil, sealed); err != nil {
		t.Fatalf("positive control: re-deriving the same key does not open: %v", err)
	}
	if _, err := mustKey(t, kr, seal.Refs, repoB, salt).Open(nil, sealed); !errors.Is(err, seal.ErrAuth) {
		t.Errorf("another repo's key opened this repo's root (err=%v)", err)
	}
	if _, err := mustKey(t, kr, seal.Refs, repoA, mustSalt(t)).Open(nil, sealed); !errors.Is(err, seal.ErrAuth) {
		t.Errorf("a key with another salt opened the value (err=%v): per-object keys are not per object", err)
	}
	if _, err := mustKey(t, mustKeyring(t), seal.Refs, repoA, salt).Open(nil, sealed); !errors.Is(err, seal.ErrAuth) {
		t.Errorf("another master key opened the value (err=%v)", err)
	}
}

// The master key is an HKDF input only: sealing with it directly must not be
// what an object key does. Checked by opening an object's output with an
// AES-GCM keyed by the raw master: it must fail.
func TestMasterKeyIsNeverTheAEADKey(t *testing.T) {
	master := bytes.Repeat([]byte{0x42}, 32)
	kr, err := seal.KeyringFromBytes(master)
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := mustKey(t, kr, seal.Chunk, repoA, seal.Salt{}).Seal(nil, []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	if len(sealed) != 1+28 {
		t.Fatalf("sealed 1 byte into %d; want nonce || ciphertext || tag (29 bytes)", len(sealed))
	}
	block, _ := aes.NewCipher(master)
	gcm, _ := cipher.NewGCM(block)
	for _, aad := range [][]byte{nil, []byte("vdb/chunk/v1"), []byte("vdb/chunk/v1\x00")} {
		if _, err := gcm.Open(nil, sealed[:12], sealed[12:], aad); err == nil {
			t.Fatal("an object's ciphertext opened under the raw master key: object keys are not derived")
		}
	}
}

func TestInvalidDomainsAreRefused(t *testing.T) {
	kr := mustKeyring(t)
	for _, d := range []seal.Domain{0, 6, 200} {
		if _, err := kr.Key(d, repoA, seal.Salt{}); !errors.Is(err, seal.ErrDomain) {
			t.Errorf("Key(domain %d) = %v, want ErrDomain: a value sealed in no context can be replayed into any", d, err)
		}
	}
}

func TestAKeyRefusesPastItsBudget(t *testing.T) {
	k := mustKey(t, mustKeyring(t), seal.Chunk, repoA, mustSalt(t))
	k.SetSealLimit(3)
	for i := 0; i < 3; i++ {
		if _, err := k.Seal(nil, []byte{byte(i)}); err != nil {
			t.Fatalf("seal %d of a 3-seal budget: %v", i+1, err)
		}
	}
	if _, err := k.Seal(nil, []byte("fourth")); !errors.Is(err, seal.ErrKeyExhausted) {
		t.Fatalf("a fourth seal under a 3-seal budget: %v, want ErrKeyExhausted", err)
	}
	if seal.MaxSealsPerKey > 1<<24 {
		t.Fatalf("MaxSealsPerKey = %d; the guard must stay far below GCM's 2^32 random-nonce bound", seal.MaxSealsPerKey)
	}
}

func TestDestroyedKeysRefuse(t *testing.T) {
	kr := mustKeyring(t)
	k := mustKey(t, kr, seal.Chunk, repoA, mustSalt(t))
	k.Destroy()
	if _, err := k.Seal(nil, []byte("x")); !errors.Is(err, seal.ErrDestroyed) {
		t.Errorf("Seal after Destroy: %v", err)
	}
	kr.Destroy()
	if _, err := kr.Key(seal.Chunk, repoA, seal.Salt{}); !errors.Is(err, seal.ErrDestroyed) {
		t.Errorf("Key after Keyring.Destroy: %v", err)
	}
}

func TestKeyIDNamesTheKeyWithoutRevealingIt(t *testing.T) {
	master := bytes.Repeat([]byte{0x17}, 32)
	a, _ := seal.KeyringFromBytes(master)
	b, _ := seal.KeyringFromBytes(master)
	if a.ID() != b.ID() {
		t.Fatal("the same master key has two IDs")
	}
	if a.ID() == mustKeyring(t).ID() {
		t.Fatal("two different master keys share an ID")
	}
	id := a.ID()
	if bytes.Equal(id[:], master) || id == seal.KeyID(sha256.Sum256(master)) {
		t.Fatal("the key ID is the key, or its bare SHA-256: it is not domain-separated")
	}
	if _, err := seal.KeyringFromBytes(make([]byte, 31)); !errors.Is(err, seal.ErrCorrupt) {
		t.Errorf("a 31-byte master key was accepted: %v", err)
	}
}

// --- key files ---

func TestKeyFileRoundTripAndRotation(t *testing.T) {
	kr := mustKeyring(t)
	salt := mustSalt(t)
	sealed, err := mustKey(t, kr, seal.Config, repoA, salt).Seal(nil, []byte("config"))
	if err != nil {
		t.Fatal(err)
	}
	f1, err := seal.NewKeyFile(kr, []byte("first passphrase"), fast)
	if err != nil {
		t.Fatal(err)
	}
	f2, err := seal.NewKeyFile(kr, []byte("rotated passphrase"), fast)
	if err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]struct{ file, pass []byte }{
		"original": {f1, []byte("first passphrase")},
		"rotated":  {f2, []byte("rotated passphrase")},
	} {
		got, err := seal.OpenKeyFile(c.file, c.pass)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got.ID() != kr.ID() {
			t.Fatalf("%s key file opened to another key", name)
		}
		if _, err := mustKey(t, got, seal.Config, repoA, salt).Open(nil, sealed); err != nil {
			t.Fatalf("%s: data sealed before wrapping does not open with the unwrapped key: %v", name, err)
		}
	}
	if bytes.Equal(f1, f2) {
		t.Fatal("two key files for the same key are identical: salt or nonce is not fresh")
	}
}

func TestKeyFileWrongPassphrase(t *testing.T) {
	f, err := seal.NewKeyFile(mustKeyring(t), []byte("right"), fast)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := seal.OpenKeyFile(f, []byte("wrong")); !errors.Is(err, seal.ErrWrongPassphrase) {
		t.Fatalf("wrong passphrase: %v, want ErrWrongPassphrase", err)
	}
}

func TestKeyFileNeverHoldsTheKey(t *testing.T) {
	master := make([]byte, 32)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	kr, _ := seal.KeyringFromBytes(master)
	f, err := seal.NewKeyFile(kr, []byte("pw-pw-pw"), fast)
	if err != nil {
		t.Fatal(err)
	}
	// An empty or short file trivially lacks the key; the fixture must be a
	// real key file that opens to this key, or the check proves nothing.
	if len(f) != keyFileLen {
		t.Fatalf("key file is %d bytes, want %d", len(f), keyFileLen)
	}
	if got, err := seal.OpenKeyFile(f, []byte("pw-pw-pw")); err != nil || got.ID() != kr.ID() {
		t.Fatalf("positive control: the key file does not open to its key (err=%v)", err)
	}
	if bytes.Contains(f, master) {
		t.Fatal("the key file contains the master key in the clear")
	}
}

// Layout offsets of the v1 key file, pinned here because the format is a
// compatibility contract (docs/DESIGN.md): magic 0, version 4, time 6,
// memory 10, threads 14, salt 15, key id 47, sealed key 79..139.
const (
	offTime, offMemory, offThreads, offSalt, offKeyID, offSealed, keyFileLen = 6, 10, 14, 15, 47, 79, 139
)

func TestKeyFileTamperingNeverOpens(t *testing.T) {
	f, err := seal.NewKeyFile(mustKeyring(t), []byte("passphrase"), fast)
	if err != nil {
		t.Fatal(err)
	}
	if len(f) != keyFileLen {
		t.Fatalf("key file is %d bytes, want %d", len(f), keyFileLen)
	}
	if _, err := seal.OpenKeyFile(f, []byte("passphrase")); err != nil {
		t.Fatalf("positive control: %v", err)
	}
	for _, i := range []int{0, 4, offTime, offThreads, offSalt, offKeyID, offSealed, offSealed + 20, keyFileLen - 1} {
		bad := bytes.Clone(f)
		bad[i] ^= 0x01
		if _, err := seal.OpenKeyFile(bad, []byte("passphrase")); err == nil {
			t.Errorf("a key file with byte %d flipped opened", i)
		}
	}
	// A downgrade that stays within the accepted range must still fail: the
	// parameters are bound into the wrap.
	weaker := bytes.Clone(f)
	binary.LittleEndian.PutUint32(weaker[offTime:], fast.Time+1)
	if _, err := seal.OpenKeyFile(weaker, []byte("passphrase")); err == nil {
		t.Error("a key file whose Argon2 time was rewritten still opened: parameters are not authenticated")
	}
	if _, err := seal.OpenKeyFile(f[:len(f)-1], []byte("passphrase")); !errors.Is(err, seal.ErrCorrupt) {
		t.Errorf("a truncated key file: %v, want ErrCorrupt", err)
	}
	if _, err := seal.OpenKeyFile(append(bytes.Clone(f), 0), []byte("passphrase")); !errors.Is(err, seal.ErrCorrupt) {
		t.Errorf("a key file with a trailing byte: %v, want ErrCorrupt", err)
	}
}

func TestKeyFileParameterBounds(t *testing.T) {
	kr := mustKeyring(t)
	for name, p := range map[string]seal.Argon2Params{
		"time 1":            {Time: 1, Memory: fast.Memory, Threads: 1},
		"memory under 19MiB": {Time: 2, Memory: 19*1024 - 1, Threads: 1},
		"threads 0":         {Time: 2, Memory: fast.Memory, Threads: 0},
		"memory over 4GiB":  {Time: 2, Memory: 4*1024*1024 + 1, Threads: 1},
		"time over 64":      {Time: 65, Memory: fast.Memory, Threads: 1},
	} {
		if _, err := seal.NewKeyFile(kr, []byte("passphrase"), p); !errors.Is(err, seal.ErrParams) {
			t.Errorf("%s: NewKeyFile accepted %+v (err=%v)", name, p, err)
		}
	}
	// A crafted file demanding absurd memory is refused before any derivation:
	// opening an attacker's key file must not be a way to exhaust memory.
	f, err := seal.NewKeyFile(kr, []byte("passphrase"), fast)
	if err != nil {
		t.Fatal(err)
	}
	if len(f) != keyFileLen {
		t.Fatalf("key file is %d bytes, want %d", len(f), keyFileLen)
	}
	binary.LittleEndian.PutUint32(f[offMemory:], 0xFFFFFFFF)
	if _, err := seal.OpenKeyFile(f, []byte("passphrase")); !errors.Is(err, seal.ErrParams) {
		t.Errorf("a key file demanding 4 TiB of Argon2 memory: %v, want ErrParams", err)
	}
	if d := seal.DefaultArgon2Params(); d != (seal.Argon2Params{Time: 3, Memory: 64 * 1024, Threads: 4}) {
		t.Errorf("default params %+v, want disknexus's 3 / 64 MiB / 4", d)
	}
}

func TestPassphraseBounds(t *testing.T) {
	kr := mustKeyring(t)
	if _, err := seal.NewKeyFile(kr, nil, fast); !errors.Is(err, seal.ErrPassphrase) {
		t.Errorf("an empty passphrase was accepted: %v", err)
	}
	if _, err := seal.NewKeyFile(kr, bytes.Repeat([]byte("x"), 1025), fast); !errors.Is(err, seal.ErrPassphrase) {
		t.Errorf("a 1025-byte passphrase was accepted: %v", err)
	}
}

// --- KMS wrapping ---

// fakeKMS stands in for a cloud KMS: AES-GCM under its own key.
type fakeKMS struct{ gcm cipher.AEAD }

func newFakeKMS(t *testing.T) *fakeKMS {
	t.Helper()
	k := make([]byte, 32)
	if _, err := rand.Read(k); err != nil {
		t.Fatal(err)
	}
	block, _ := aes.NewCipher(k)
	gcm, _ := cipher.NewGCM(block)
	return &fakeKMS{gcm: gcm}
}

func (f *fakeKMS) Wrap(_ context.Context, secret []byte) ([]byte, error) {
	if len(secret) == 0 {
		return nil, errors.New("fake kms: empty secret")
	}
	nonce := make([]byte, 12)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return f.gcm.Seal(nonce, nonce, secret, nil), nil
}

func (f *fakeKMS) Unwrap(_ context.Context, w []byte) ([]byte, error) {
	if len(w) < 12 {
		return nil, errors.New("fake kms: short")
	}
	return f.gcm.Open(nil, w[:12], w[12:], nil)
}

func TestX25519WrapperPassesTheWrapperContract(t *testing.T) {
	contract.RunWrapper(t, func(t *testing.T) contract.Pair {
		a, _, _, err := seal.GenerateX25519Wrapper()
		if err != nil {
			t.Fatal(err)
		}
		b, _, _, err := seal.GenerateX25519Wrapper()
		if err != nil {
			t.Fatal(err)
		}
		return contract.Pair{A: a, B: b}
	})
}

func TestX25519WrapOnlyWithoutPrivateKey(t *testing.T) {
	_, pub, priv, err := seal.GenerateX25519Wrapper()
	if err != nil {
		t.Fatal(err)
	}
	wrapOnly, err := seal.NewX25519Wrapper(pub, nil)
	if err != nil {
		t.Fatal(err)
	}
	w, err := wrapOnly.Wrap(context.Background(), []byte("secret"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wrapOnly.Unwrap(context.Background(), w); err == nil {
		t.Fatal("a wrapper holding only the public key unwrapped a secret")
	}
	full, err := seal.NewX25519Wrapper(pub, priv)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := full.Unwrap(context.Background(), w); err != nil || string(got) != "secret" {
		t.Fatalf("positive control: %q, %v", got, err)
	}
}

func TestWrapKeyringRoundTripAndWrongKMS(t *testing.T) {
	ctx := context.Background()
	kr := mustKeyring(t)
	kms := newFakeKMS(t)
	env, err := seal.WrapKeyring(ctx, kms, kr)
	if err != nil {
		t.Fatal(err)
	}
	got, err := seal.UnwrapKeyring(ctx, kms, env)
	if err != nil || got.ID() != kr.ID() {
		t.Fatalf("UnwrapKeyring: %v (same key: %v)", err, err == nil && got.ID() == kr.ID())
	}
	if _, err := seal.UnwrapKeyring(ctx, newFakeKMS(t), env); err == nil {
		t.Fatal("another KMS unwrapped the keyring")
	}
	// A KMS that returns a DIFFERENT valid key must be caught by ID.
	other := mustKeyring(t)
	liar := &constantKMS{secret: masterOf(t, other)}
	if _, err := seal.UnwrapKeyring(ctx, liar, env); !errors.Is(err, seal.ErrWrongKey) {
		t.Fatalf("a KMS returning the wrong 32 bytes: %v, want ErrWrongKey", err)
	}
}

// constantKMS unwraps anything to a fixed secret.
type constantKMS struct{ secret []byte }

func (c *constantKMS) Wrap(context.Context, []byte) ([]byte, error)   { return []byte("x"), nil }
func (c *constantKMS) Unwrap(context.Context, []byte) ([]byte, error) { return c.secret, nil }

// masterOf recovers a keyring's raw key through a KMS that records what it wraps.
func masterOf(t *testing.T, kr *seal.Keyring) []byte {
	t.Helper()
	rec := &recordingKMS{}
	if _, err := seal.WrapKeyring(context.Background(), rec, kr); err != nil {
		t.Fatal(err)
	}
	if len(rec.got) != 32 {
		t.Fatalf("WrapKeyring handed the KMS %d bytes, want the 32-byte master key", len(rec.got))
	}
	return rec.got
}

type recordingKMS struct{ got []byte }

func (r *recordingKMS) Wrap(_ context.Context, s []byte) ([]byte, error) {
	r.got = bytes.Clone(s)
	return []byte("wrapped"), nil
}
func (r *recordingKMS) Unwrap(context.Context, []byte) ([]byte, error) { return nil, errors.New("no") }

func FuzzOpenKeyFile(f *testing.F) {
	kr, _ := seal.KeyringFromBytes(bytes.Repeat([]byte{1}, 32))
	good, err := seal.NewKeyFile(kr, []byte("pw"), fast)
	if err == nil {
		f.Add(good)
	}
	f.Add([]byte("SCKF"))
	f.Fuzz(func(t *testing.T, b []byte) {
		// Opening arbitrary bytes never panics and never yields a key unless
		// the bytes are a real key file for this passphrase.
		kr, err := seal.OpenKeyFile(b, []byte("pw"))
		if err == nil && kr == nil {
			t.Fatal("nil keyring with nil error")
		}
	})
}

func FuzzUnwrapKeyring(f *testing.F) {
	f.Add([]byte("SCKW\x01\x00"))
	f.Fuzz(func(t *testing.T, b []byte) {
		_, _ = seal.UnwrapKeyring(context.Background(), &constantKMS{secret: make([]byte, 32)}, b)
	})
}

func ExampleNewKeyFile() {
	kr, _ := seal.NewKeyring()
	file, _ := seal.NewKeyFile(kr, []byte("a long passphrase"), seal.Argon2Params{Time: 2, Memory: 19 * 1024, Threads: 1})
	opened, _ := seal.OpenKeyFile(file, []byte("a long passphrase"))
	fmt.Println(opened.ID() == kr.ID())
	// Output: true
}
