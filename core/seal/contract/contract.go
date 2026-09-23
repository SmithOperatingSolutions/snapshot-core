// Package contract is the suite every seal.Wrapper (KMS) implementation must
// pass (Engine Spec boundary rule 4: every port ships a contract suite).
package contract

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
)

// Pair is two independent wrappers of the same implementation: neither may
// unwrap what the other wrapped.
type Pair struct{ A, B seal.Wrapper }

func random(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

// RunWrapper runs the Wrapper contract.
func RunWrapper(t *testing.T, newPair func(t *testing.T) Pair) {
	ctx := context.Background()

	t.Run("RoundTrip", func(t *testing.T) {
		p := newPair(t)
		for _, n := range []int{1, 32, 1024} {
			secret := random(t, n)
			w, err := p.A.Wrap(ctx, secret)
			if err != nil {
				t.Fatalf("Wrap(%d bytes): %v", n, err)
			}
			got, err := p.A.Unwrap(ctx, w)
			if err != nil || !bytes.Equal(got, secret) {
				t.Fatalf("Unwrap(Wrap(%d bytes)) = %d bytes, %v: a repository wrapped with this KMS could never be opened",
					n, len(got), err)
			}
		}
	})

	t.Run("SecretNotInTheClear", func(t *testing.T) {
		p := newPair(t)
		secret := random(t, 32)
		w, err := p.A.Wrap(ctx, secret)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(w, secret) {
			t.Fatal("the wrapped output contains the secret in the clear")
		}
	})

	t.Run("Randomized", func(t *testing.T) {
		p := newPair(t)
		secret := random(t, 32)
		w1, err1 := p.A.Wrap(ctx, secret)
		w2, err2 := p.A.Wrap(ctx, secret)
		if err1 != nil || err2 != nil {
			t.Fatal(err1, err2)
		}
		if bytes.Equal(w1, w2) {
			t.Fatal("wrapping the same secret twice gave identical output: the wrap is deterministic, so equal keys are visible as equal")
		}
	})

	t.Run("OtherKeyRefused", func(t *testing.T) {
		p := newPair(t)
		w, err := p.A.Wrap(ctx, random(t, 32))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.A.Unwrap(ctx, w); err != nil {
			t.Fatalf("positive control: A cannot unwrap its own output: %v", err)
		}
		if got, err := p.B.Unwrap(ctx, w); err == nil {
			t.Fatalf("B unwrapped what A wrapped (%d bytes): the KMS key does not protect the secret", len(got))
		}
	})

	t.Run("TamperRefused", func(t *testing.T) {
		p := newPair(t)
		w, err := p.A.Wrap(ctx, random(t, 32))
		if err != nil {
			t.Fatal(err)
		}
		for _, i := range []int{0, len(w) / 2, len(w) - 1} {
			bad := bytes.Clone(w)
			bad[i] ^= 0x01
			if _, err := p.A.Unwrap(ctx, bad); err == nil {
				t.Errorf("a wrapped secret with byte %d flipped unwrapped", i)
			}
		}
		if _, err := p.A.Unwrap(ctx, w[:len(w)-1]); err == nil {
			t.Error("a truncated wrapped secret unwrapped")
		}
		if _, err := p.A.Unwrap(ctx, nil); err == nil {
			t.Error("an empty wrapped secret unwrapped")
		}
	})

	t.Run("EmptySecretRefused", func(t *testing.T) {
		p := newPair(t)
		if _, err := p.A.Wrap(ctx, nil); err == nil {
			t.Error("an empty secret was wrapped; a KMS asked to wrap nothing is being misused")
		}
	})
}
