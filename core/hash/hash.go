// Package hash is chunk identity: the SHA-256 of a chunk's exact stored bytes
// (Storage Core Spec: "identity is always SHA-256 of the stored plaintext
// bytes"). Computed through core/dnx, whose compat suite pins it.
package hash

import (
	"bytes"
	"encoding/hex"
	"errors"

	"github.com/SmithOperatingSolutions/snapshot-core/core/dnx"
)

// Size is the length of a Hash in bytes.
const Size = 32

// Hash identifies a chunk.
type Hash [Size]byte

// ErrInvalid is returned when parsing text that is not a hash.
var ErrInvalid = errors.New("hash: invalid")

// Sum returns the identity of b.
func Sum(b []byte) Hash { return Hash(dnx.Identify(b).Strong) }

// String returns the lowercase hex form.
func (h Hash) String() string { return hex.EncodeToString(h[:]) }

// Short returns the first 12 hex digits, for logs and messages.
func (h Hash) Short() string { return hex.EncodeToString(h[:6]) }

// IsZero reports whether h is the zero hash (never the identity of any bytes).
func (h Hash) IsZero() bool { return h == Hash{} }

// Compare orders hashes bytewise: -1, 0 or +1.
func (h Hash) Compare(o Hash) int { return bytes.Compare(h[:], o[:]) }

// Parse reads the lowercase hex form String writes. It accepts nothing else:
// no uppercase, no prefix, no whitespace.
func Parse(s string) (Hash, error) {
	var h Hash
	if len(s) != 2*Size {
		return h, ErrInvalid
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return h, ErrInvalid
		}
	}
	if _, err := hex.Decode(h[:], []byte(s)); err != nil {
		return Hash{}, ErrInvalid
	}
	return h, nil
}

// FromBytes copies a 32-byte slice into a Hash.
func FromBytes(b []byte) (Hash, error) {
	if len(b) != Size {
		return Hash{}, ErrInvalid
	}
	return Hash(b), nil
}
