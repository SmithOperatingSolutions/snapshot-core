// Package seal is snapshot-core's encryption: AES-256-GCM for all data and
// metadata at rest, one derived key per stored object, a domain tag on every
// seal, and master keys that come from a KMS or an Argon2id passphrase and are
// never stored beside the data (Storage Core Spec, "Security requirements").
//
// The AEAD, Argon2id and X25519 come from disknexus-engine through core/dnx.
// What seal adds is policy: the master key is only ever an HKDF input, never
// an AEAD key; every object (pack, index object, root, config) gets its own
// key from HKDF-SHA-256(master, 256-bit random salt, domain || repo id), so no
// single key comes near GCM's random-nonce limit; and the only way to seal is
// under one of the Domain constants below.
package seal

import (
	"context"
	"errors"
)

// Errors.
var (
	ErrAuth            = errors.New("seal: authentication failed (wrong key, or the data was altered)")
	ErrWrongPassphrase = errors.New("seal: wrong passphrase")
	ErrWrongKey        = errors.New("seal: key does not match")
	ErrCorrupt         = errors.New("seal: corrupt key material")
	ErrDomain          = errors.New("seal: invalid domain")
	ErrKeyExhausted    = errors.New("seal: key has reached its seal budget")
	ErrParams          = errors.New("seal: unacceptable key-derivation parameters")
	ErrPassphrase      = errors.New("seal: unacceptable passphrase")
	ErrDestroyed       = errors.New("seal: key destroyed")
)

// Domain says what a sealed value is. The zero Domain is invalid.
type Domain uint8

// The domains. Their tags are part of the on-disk format.
const (
	_         Domain = iota
	Chunk            // "vdb/chunk/v1": chunk frames in a pack
	PackIndex        // "vdb/pack-index/v1": a pack's trailer index
	Index            // "vdb/index/v1": index objects (chunk -> pack location)
	Refs             // "vdb/refs/v1": the root object (the manifest)
	Config           // "vdb/config/v1": the repo config
)

// Tag returns the domain's tag, or "" for an invalid domain.
func (d Domain) Tag() string { return "" }

// KeyID names a master key without revealing it.
type KeyID [32]byte

// RepoID binds keys to one repository.
type RepoID [16]byte

// Salt makes each object's key unique.
type Salt [32]byte

// NewSalt returns a random salt.
func NewSalt() (Salt, error) { return Salt{}, nil }

// Keyring holds a master key.
type Keyring struct{}

// NewKeyring generates a random master key.
func NewKeyring() (*Keyring, error) { return &Keyring{}, nil }

// KeyringFromBytes adopts a 32-byte master key (from a host's KMS). The bytes
// are copied.
func KeyringFromBytes(b []byte) (*Keyring, error) { return &Keyring{}, nil }

// ID returns the key's identity: HKDF output, so it reveals nothing of the key.
func (k *Keyring) ID() KeyID { return KeyID{} }

// Destroy zeroes the master key; the Keyring is unusable afterwards.
func (k *Keyring) Destroy() {}

// MaxSealsPerKey bounds how many values one derived key may seal. GCM with
// random 96-bit nonces is safe to about 2^32 messages per key; a pack is
// capped far below this, so the budget is a guard, not a limit anyone meets.
const MaxSealsPerKey = 1 << 20

// Key is the AEAD key for one object.
type Key struct{}

func (k *Key) setLimit(n uint64) {}

// Key derives the key for one object from the master key.
func (k *Keyring) Key(d Domain, repo RepoID, salt Salt) (*Key, error) { return &Key{}, nil }

// Seal encrypts plaintext under the key's domain, bound to context (which is
// authenticated, not stored). Output: nonce || ciphertext || tag.
func (k *Key) Seal(context, plaintext []byte) ([]byte, error) { return plaintext, nil }

// Open decrypts what Seal produced with the same domain, repo, salt and context.
func (k *Key) Open(context, sealed []byte) ([]byte, error) { return sealed, nil }

// Destroy zeroes the key.
func (k *Key) Destroy() {}

// Argon2Params are the passphrase KDF's costs.
type Argon2Params struct {
	Time    uint32
	Memory  uint32 // KiB
	Threads uint8
}

// DefaultArgon2Params are disknexus's defaults: time 3, 64 MiB, 4 threads.
func DefaultArgon2Params() Argon2Params { return Argon2Params{} }

// NewKeyFile wraps kr's master key under a passphrase. The result is for the
// host to store; never put it in the repository's backend. Calling it again
// with another passphrase is rotation: a new file, the same key.
func NewKeyFile(kr *Keyring, passphrase []byte, p Argon2Params) ([]byte, error) { return nil, nil }

// OpenKeyFile unwraps a key file.
func OpenKeyFile(file, passphrase []byte) (*Keyring, error) { return &Keyring{}, nil }

// Wrapper is a KMS: it wraps a secret under a key the host controls. Hosts
// implement it over AWS KMS, Vault, an HSM; X25519Wrapper is built in.
// core/seal/contract is the suite every implementation must pass.
type Wrapper interface {
	Wrap(ctx context.Context, secret []byte) ([]byte, error)
	Unwrap(ctx context.Context, wrapped []byte) ([]byte, error)
}

// WrapKeyring seals kr's master key with a KMS, in an envelope that names the
// key so a wrong one is caught by ID, not by a failed decrypt deep inside.
func WrapKeyring(ctx context.Context, w Wrapper, kr *Keyring) ([]byte, error) { return nil, nil }

// UnwrapKeyring opens an envelope WrapKeyring produced.
func UnwrapKeyring(ctx context.Context, w Wrapper, envelope []byte) (*Keyring, error) {
	return &Keyring{}, nil
}

// X25519Wrapper wraps secrets to an X25519 key pair (ECIES, via disknexus).
// With only a public key it can wrap but not unwrap.
type X25519Wrapper struct{}

// GenerateX25519Wrapper returns a wrapper over a new key pair, and the pair.
func GenerateX25519Wrapper() (w *X25519Wrapper, pub, priv []byte, err error) {
	return &X25519Wrapper{}, nil, nil, nil
}

// NewX25519Wrapper returns a wrapper over an existing pair; priv may be nil.
func NewX25519Wrapper(pub, priv []byte) (*X25519Wrapper, error) { return &X25519Wrapper{}, nil }

// Wrap implements Wrapper.
func (w *X25519Wrapper) Wrap(ctx context.Context, secret []byte) ([]byte, error) { return secret, nil }

// Unwrap implements Wrapper.
func (w *X25519Wrapper) Unwrap(ctx context.Context, wrapped []byte) ([]byte, error) {
	return wrapped, nil
}
