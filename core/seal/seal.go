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
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"

	"github.com/SmithOperatingSolutions/snapshot-core/core/dnx"
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
	ErrWrapOnly        = errors.New("seal: this wrapper holds only a public key and cannot unwrap")
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

var tags = [...]string{
	Chunk:     "vdb/chunk/v1",
	PackIndex: "vdb/pack-index/v1",
	Index:     "vdb/index/v1",
	Refs:      "vdb/refs/v1",
	Config:    "vdb/config/v1",
}

// Tag returns the domain's tag, or "" for an invalid domain.
func (d Domain) Tag() string {
	if d == 0 || int(d) >= len(tags) {
		return ""
	}
	return tags[d]
}

// KeyID names a master key without revealing it.
type KeyID [32]byte

// RepoID binds keys to one repository.
type RepoID [16]byte

// Salt makes each object's key unique.
type Salt [32]byte

// NewSalt returns a random salt.
func NewSalt() (Salt, error) {
	var s Salt
	_, err := rand.Read(s[:])
	return s, err
}

// Keyring holds a master key.
type Keyring struct {
	mu        sync.RWMutex
	master    [32]byte
	id        KeyID
	destroyed bool
}

// NewKeyring generates a random master key.
func NewKeyring() (*Keyring, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return nil, err
	}
	defer clear(b[:])
	return KeyringFromBytes(b[:])
}

// KeyringFromBytes adopts a 32-byte master key (from a host's KMS). The bytes
// are copied.
func KeyringFromBytes(b []byte) (*Keyring, error) {
	if len(b) != 32 {
		return nil, fmt.Errorf("%w: master key is %d bytes, want 32", ErrCorrupt, len(b))
	}
	k := &Keyring{}
	copy(k.master[:], b)
	id, err := hkdf.Key(sha256.New, k.master[:], nil, "snapshot-core/key-id/v1", len(k.id))
	if err != nil {
		return nil, err
	}
	copy(k.id[:], id)
	return k, nil
}

// ID returns the key's identity: HKDF output, so it reveals nothing of the key.
func (k *Keyring) ID() KeyID { return k.id }

// Destroy zeroes the master key; the Keyring is unusable afterwards.
func (k *Keyring) Destroy() {
	k.mu.Lock()
	defer k.mu.Unlock()
	clear(k.master[:])
	k.destroyed = true
}

// withMaster runs f with the master key, refusing a destroyed keyring.
func (k *Keyring) withMaster(f func(master []byte) error) error {
	k.mu.RLock()
	defer k.mu.RUnlock()
	if k.destroyed {
		return ErrDestroyed
	}
	return f(k.master[:])
}

// MaxSealsPerKey bounds how many values one derived key may seal. GCM with
// random 96-bit nonces is safe to about 2^32 messages per key; a pack is
// capped far below this, so the budget is a guard, not a limit anyone meets.
const MaxSealsPerKey = 1 << 20

// Key is the AEAD key for one object.
type Key struct {
	mu        sync.Mutex
	aead      *dnx.AEAD
	tag       string
	seals     uint64
	limit     uint64
	destroyed bool
}

func (k *Key) setLimit(n uint64) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.limit = n
}

// Key derives the key for one object from the master key.
func (k *Keyring) Key(d Domain, repo RepoID, salt Salt) (*Key, error) {
	tag := d.Tag()
	if tag == "" {
		return nil, fmt.Errorf("%w: %d", ErrDomain, d)
	}
	info := "snapshot-core/object-key/v1\x00" + tag + "\x00" + string(repo[:])
	var aead *dnx.AEAD
	err := k.withMaster(func(master []byte) error {
		raw, err := hkdf.Key(sha256.New, master, salt[:], info, dnx.KeySize)
		if err != nil {
			return err
		}
		defer clear(raw)
		aead, err = dnx.NewAEAD(raw)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &Key{aead: aead, tag: tag, limit: MaxSealsPerKey}, nil
}

// aad is the associated data: the domain tag, a separator, then the context.
func (k *Key) aad(context []byte) []byte {
	a := make([]byte, 0, len(k.tag)+1+len(context))
	a = append(a, k.tag...)
	a = append(a, 0)
	return append(a, context...)
}

// Seal encrypts plaintext under the key's domain, bound to context (which is
// authenticated, not stored). Output: nonce || ciphertext || tag.
func (k *Key) Seal(context, plaintext []byte) ([]byte, error) {
	k.mu.Lock()
	switch {
	case k.destroyed:
		k.mu.Unlock()
		return nil, ErrDestroyed
	case k.seals >= k.limit:
		k.mu.Unlock()
		return nil, ErrKeyExhausted
	}
	k.seals++
	k.mu.Unlock()
	return k.aead.Seal(plaintext, k.aad(context))
}

// Open decrypts what Seal produced with the same domain, repo, salt and context.
func (k *Key) Open(context, sealed []byte) ([]byte, error) {
	k.mu.Lock()
	destroyed := k.destroyed
	k.mu.Unlock()
	if destroyed {
		return nil, ErrDestroyed
	}
	pt, err := k.aead.Open(sealed, k.aad(context))
	if err != nil {
		return nil, ErrAuth
	}
	return pt, nil
}

// Destroy zeroes the key.
func (k *Key) Destroy() {
	k.mu.Lock()
	defer k.mu.Unlock()
	if !k.destroyed {
		k.aead.Destroy()
		k.destroyed = true
	}
}

// Wrapper is a KMS: it wraps a secret under a key the host controls. Hosts
// implement it over AWS KMS, Vault, an HSM; X25519Wrapper is built in.
// core/seal/contract is the suite every implementation must pass.
type Wrapper interface {
	Wrap(ctx context.Context, secret []byte) ([]byte, error)
	Unwrap(ctx context.Context, wrapped []byte) ([]byte, error)
}
