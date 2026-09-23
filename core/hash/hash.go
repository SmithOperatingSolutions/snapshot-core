// Package hash is chunk identity: the SHA-256 of a chunk's exact stored bytes
// (Storage Core Spec: "identity is always SHA-256 of the stored plaintext
// bytes"). Computed through core/dnx, whose compat suite pins it.
package hash

import "errors"

// Size is the length of a Hash in bytes.
const Size = 32

// Hash identifies a chunk.
type Hash [Size]byte

// ErrInvalid is returned when parsing text that is not a hash.
var ErrInvalid = errors.New("hash: invalid")

// Sum returns the identity of b.
func Sum(b []byte) Hash { return Hash{} }

// String returns the lowercase hex form.
func (h Hash) String() string { return "" }

// Short returns the first 12 hex digits, for logs and messages.
func (h Hash) Short() string { return "" }

// IsZero reports whether h is the zero hash (never the identity of any bytes).
func (h Hash) IsZero() bool { return false }

// Compare orders hashes bytewise: -1, 0 or +1.
func (h Hash) Compare(o Hash) int { return 0 }

// Parse reads the lowercase hex form String writes. It accepts nothing else:
// no uppercase, no prefix, no whitespace.
func Parse(s string) (Hash, error) { return Hash{}, nil }

// FromBytes copies a 32-byte slice into a Hash.
func FromBytes(b []byte) (Hash, error) { return Hash{}, nil }
