package seal

import (
	"bytes"
	"context"
	"errors"

	"github.com/SmithOperatingSolutions/snapshot-core/core/dnx"
)

// X25519Wrapper wraps secrets to an X25519 key pair (ECIES, via disknexus).
// With only a public key it can wrap but not unwrap.
type X25519Wrapper struct{ pub, priv []byte }

// GenerateX25519Wrapper returns a wrapper over a new key pair, and the pair.
func GenerateX25519Wrapper() (w *X25519Wrapper, pub, priv []byte, err error) {
	pub, priv, err = dnx.GenerateX25519()
	if err != nil {
		return nil, nil, nil, err
	}
	return &X25519Wrapper{pub: bytes.Clone(pub), priv: bytes.Clone(priv)}, pub, priv, nil
}

// NewX25519Wrapper returns a wrapper over an existing pair; priv may be nil.
func NewX25519Wrapper(pub, priv []byte) (*X25519Wrapper, error) {
	if len(pub) != 32 || (priv != nil && len(priv) != 32) {
		return nil, errors.New("seal: X25519 keys are 32 bytes")
	}
	return &X25519Wrapper{pub: bytes.Clone(pub), priv: bytes.Clone(priv)}, nil
}

// Wrap implements Wrapper.
func (w *X25519Wrapper) Wrap(_ context.Context, secret []byte) ([]byte, error) {
	if len(secret) == 0 {
		return nil, errors.New("seal: refusing to wrap an empty secret")
	}
	return dnx.WrapSecret(w.pub, secret)
}

// Unwrap implements Wrapper.
func (w *X25519Wrapper) Unwrap(_ context.Context, wrapped []byte) ([]byte, error) {
	if w.priv == nil {
		return nil, errors.New("seal: this X25519 wrapper holds only a public key")
	}
	return dnx.UnwrapSecret(w.priv, wrapped)
}
