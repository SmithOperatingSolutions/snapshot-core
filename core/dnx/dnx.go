// Package dnx is the only package allowed to import disknexus-engine
// (Storage Core Spec, dependency rule 2; enforced by depguard). Everything
// else codes against the types here, so a disknexus release can never leak
// into the object model. What we take: the byte-stream chunker, chunk
// identity, the AEAD, Argon2id and X25519 secret wrapping. The facts we rely
// on are pinned by the compat suite in core/dnx/compat.
package dnx

import (
	"errors"
	"io"
)

var errNotImplemented = errors.New("dnx: not implemented")

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
type Chunker struct{}

// NewChunker returns a chunker reading r with geometry g.
func NewChunker(r io.Reader, g Geometry) *Chunker { return &Chunker{} }

// Next returns the next chunk, or io.EOF after the last.
func (c *Chunker) Next() (Chunk, error) { return Chunk{}, io.EOF }

// ChunkID is a chunk's identity: the SHA-256 of its bytes, plus a cheap
// non-cryptographic hint usable only as a filter.
type ChunkID struct {
	Strong     [32]byte
	FilterHint uint64
}

// Identify computes a chunk's identity over exactly the bytes given.
func Identify(b []byte) ChunkID { return ChunkID{} }

// AEAD sizes.
const (
	KeySize   = 32
	NonceSize = 12
	TagSize   = 16
	Overhead  = NonceSize + TagSize
)

// AEAD is AES-256-GCM bound to one key. Every call requires a non-empty
// associated-data domain tag: the untagged disknexus calls are not exposed.
type AEAD struct{}

// NewAEAD returns an AEAD for a 32-byte key.
func NewAEAD(key []byte) (*AEAD, error) { return nil, errNotImplemented }

// Seal encrypts plaintext bound to aad. Output: nonce || ciphertext || tag.
func (a *AEAD) Seal(plaintext, aad []byte) ([]byte, error) { return nil, errNotImplemented }

// Open decrypts what Seal produced under the same aad.
func (a *AEAD) Open(sealed, aad []byte) ([]byte, error) { return nil, errNotImplemented }

// Destroy zeroes the key.
func (a *AEAD) Destroy() {}

// Argon2Params are Argon2id cost parameters.
type Argon2Params struct {
	Time    uint32
	Memory  uint32 // KiB
	Threads uint8
}

// DefaultArgon2Params are disknexus's defaults (time 3, 64 MiB, 4 threads).
func DefaultArgon2Params() Argon2Params { return Argon2Params{} }

// DeriveKEK derives a 32-byte key-encryption key from a passphrase with
// Argon2id. Unusable parameters are refused, never passed on to panic.
func DeriveKEK(passphrase, salt []byte, p Argon2Params) ([]byte, error) {
	return nil, errNotImplemented
}

// SecretWrapOverhead is what WrapSecret adds to a secret's length.
const SecretWrapOverhead = 32 + NonceSize + TagSize

// GenerateX25519 returns a new X25519 key pair as raw 32-byte keys.
func GenerateX25519() (pub, priv []byte, err error) { return nil, nil, errNotImplemented }

// WrapSecret seals secret to an X25519 public key (ECIES: ephemeral X25519,
// HKDF-SHA-256, AES-256-GCM).
func WrapSecret(pub, secret []byte) ([]byte, error) { return nil, errNotImplemented }

// UnwrapSecret opens what WrapSecret produced, with the matching private key.
func UnwrapSecret(priv, wrapped []byte) ([]byte, error) { return nil, errNotImplemented }
