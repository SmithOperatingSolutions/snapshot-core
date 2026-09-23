// Package dnx is the only package allowed to import disknexus-engine
// (Storage Core Spec, dependency rule 2; enforced by depguard). Everything
// else codes against the types here, so a disknexus release can never leak
// into the object model. What we take: the byte-stream chunker, chunk
// identity, the AEAD, Argon2id and X25519 secret wrapping. The facts we rely
// on are pinned by the compat suite in core/dnx/compat.
package dnx

import (
	"errors"
	"fmt"
	"io"

	"github.com/SmithOperatingSolutions/disknexus-engine/core/chunker"
	"github.com/SmithOperatingSolutions/disknexus-engine/core/crypto"
	"github.com/SmithOperatingSolutions/disknexus-engine/core/hasher"
)

// ErrUntagged is returned for a seal or open without a domain tag.
var ErrUntagged = errors.New("dnx: every seal and open needs a domain tag")

// Geometry is the byte-stream chunker's configuration. It is validated by
// core/cdc, not here: dnx passes it through untouched.
type Geometry struct {
	Min  int    // below this, boundaries use the hard mask (rare, not impossible)
	Max  int    // a boundary is forced here
	Mask uint64 // the base Buzhash mask; the chunker derives its hard and easy masks from it
}

// Chunk is one content-defined piece of a byte stream.
type Chunk struct {
	Data   []byte // the exact stream bytes; owned by the caller
	Offset int64  // offset of Data[0] in the stream
}

// Chunker cuts a byte stream at content-defined boundaries. It is not safe
// for concurrent use: boundaries are only deterministic single-threaded.
type Chunker struct{ c *chunker.Chunker }

// NewChunker returns a chunker reading r with geometry g.
func NewChunker(r io.Reader, g Geometry) *Chunker {
	return &Chunker{c: chunker.New(r, chunker.WithMinSize(g.Min), chunker.WithMaxSize(g.Max), chunker.WithMask(g.Mask))}
}

// Next returns the next chunk, or io.EOF after the last.
func (c *Chunker) Next() (Chunk, error) {
	ch, err := c.c.Next()
	if err != nil {
		return Chunk{}, err
	}
	return Chunk{Data: ch.Data, Offset: ch.Offset}, nil
}

// ChunkID is a chunk's identity: the SHA-256 of its bytes, plus a cheap
// non-cryptographic hint usable only as a filter.
type ChunkID struct {
	Strong     [32]byte
	FilterHint uint64
}

// Identify computes a chunk's identity over exactly the bytes given.
func Identify(b []byte) ChunkID {
	id := hasher.Sum(b)
	return ChunkID{Strong: id.StrongHash, FilterHint: id.WeakHash}
}

// AEAD sizes.
const (
	KeySize   = 32
	NonceSize = 12
	TagSize   = 16
	Overhead  = NonceSize + TagSize
)

// AEAD is AES-256-GCM bound to one key. Every call requires a non-empty
// associated-data domain tag: the untagged disknexus calls are not exposed.
type AEAD struct{ mk *crypto.MasterKey }

// NewAEAD returns an AEAD for a 32-byte key. The key bytes are copied.
func NewAEAD(key []byte) (*AEAD, error) {
	mk, err := crypto.MasterKeyFromBytes(key)
	if err != nil {
		return nil, fmt.Errorf("dnx: %w", err)
	}
	return &AEAD{mk: mk}, nil
}

// Seal encrypts plaintext bound to aad. Output: nonce || ciphertext || tag.
func (a *AEAD) Seal(plaintext, aad []byte) ([]byte, error) {
	if len(aad) == 0 {
		return nil, ErrUntagged
	}
	return a.mk.EncryptWithAAD(plaintext, aad)
}

// Open decrypts what Seal produced under the same aad.
func (a *AEAD) Open(sealed, aad []byte) ([]byte, error) {
	if len(aad) == 0 {
		return nil, ErrUntagged
	}
	return a.mk.DecryptWithAAD(sealed, aad)
}

// Destroy zeroes the key.
func (a *AEAD) Destroy() { a.mk.Destroy() }

// Argon2Params are Argon2id cost parameters.
type Argon2Params struct {
	Time    uint32
	Memory  uint32 // KiB
	Threads uint8
}

// DefaultArgon2Params are disknexus's defaults (time 3, 64 MiB, 4 threads).
func DefaultArgon2Params() Argon2Params {
	return Argon2Params{Time: crypto.DefaultArgonTime, Memory: crypto.DefaultArgonMemory, Threads: crypto.DefaultArgonThreads}
}

// DeriveKEK derives a 32-byte key-encryption key from a passphrase with
// Argon2id. Unusable parameters are refused, never passed on to panic.
func DeriveKEK(passphrase, salt []byte, p Argon2Params) ([]byte, error) {
	cp := crypto.Argon2Params{Time: p.Time, Memory: p.Memory, Threads: p.Threads}
	if err := cp.Validate(); err != nil {
		return nil, fmt.Errorf("dnx: %w", err)
	}
	return crypto.DeriveKEK(string(passphrase), salt, cp), nil
}

// SecretWrapOverhead is what WrapSecret adds to a secret's length.
const SecretWrapOverhead = 32 + NonceSize + TagSize

// GenerateX25519 returns a new X25519 key pair as raw 32-byte keys.
func GenerateX25519() (pub, priv []byte, err error) {
	pk, sk, err := crypto.GenerateKeyPair()
	if err != nil {
		return nil, nil, err
	}
	return pk.Bytes(), sk.Bytes(), nil
}

// WrapSecret seals secret to an X25519 public key (ECIES: ephemeral X25519,
// HKDF-SHA-256, AES-256-GCM).
func WrapSecret(pub, secret []byte) ([]byte, error) {
	pk, err := crypto.PublicKeyFromBytes(pub)
	if err != nil {
		return nil, fmt.Errorf("dnx: %w", err)
	}
	return crypto.WrapSecretAsymmetric(pk, secret)
}

// UnwrapSecret opens what WrapSecret produced, with the matching private key.
func UnwrapSecret(priv, wrapped []byte) ([]byte, error) {
	sk, err := crypto.PrivateKeyFromBytes(priv)
	if err != nil {
		return nil, fmt.Errorf("dnx: %w", err)
	}
	defer sk.Destroy()
	return crypto.UnwrapSecretAsymmetric(sk, wrapped)
}
