package hash_test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
)

func TestSumIsSHA256OfTheExactBytes(t *testing.T) {
	for _, b := range [][]byte{{}, []byte("x"), make([]byte, 4096)} {
		if got, want := hash.Sum(b), hash.Hash(sha256.Sum256(b)); got != want {
			t.Errorf("Sum(%d bytes) = %s, want SHA-256 %x", len(b), got, want[:])
		}
	}
}

func TestStringParseRoundTrip(t *testing.T) {
	h := hash.Sum([]byte("round trip"))
	s := h.String()
	if len(s) != 64 || strings.ToLower(s) != s {
		t.Fatalf("String() = %q, want 64 lowercase hex digits", s)
	}
	back, err := hash.Parse(s)
	if err != nil || back != h {
		t.Fatalf("Parse(String()) = %s, %v; want %s", back, err, h)
	}
	if h.Short() != s[:12] {
		t.Fatalf("Short() = %q, want %q", h.Short(), s[:12])
	}
}

func TestParseAcceptsOnlyCanonicalHex(t *testing.T) {
	sum := sha256.Sum256([]byte("x"))
	good := hex.EncodeToString(sum[:]) // the authority, not the code under test
	if h, err := hash.Parse(good); err != nil || h != hash.Hash(sum) {
		t.Fatalf("positive control: Parse(canonical hex) = %s, %v; want %x", h, err, sum)
	}
	for name, s := range map[string]string{
		"empty":      "",
		"short":      good[:63],
		"long":       good + "0",
		"uppercase":  strings.ToUpper(good),
		"prefixed":   "0x" + good[2:],
		"whitespace": " " + good[1:],
		"non-hex":    "g" + good[1:],
	} {
		if _, err := hash.Parse(s); !errors.Is(err, hash.ErrInvalid) {
			t.Errorf("%s: Parse(%q) = %v, want ErrInvalid — a hash spelled two ways names two objects", name, s, err)
		}
	}
}

func TestFromBytesRequiresExactlyThirtyTwo(t *testing.T) {
	h := hash.Sum([]byte("x"))
	back, err := hash.FromBytes(h[:])
	if err != nil || back != h {
		t.Fatalf("positive control: FromBytes(32 bytes) = %s, %v", back, err)
	}
	for _, n := range []int{0, 31, 33} {
		if _, err := hash.FromBytes(make([]byte, n)); !errors.Is(err, hash.ErrInvalid) {
			t.Errorf("FromBytes(%d bytes) = %v, want ErrInvalid", n, err)
		}
	}
}

func TestZeroAndCompare(t *testing.T) {
	var zero hash.Hash
	if !zero.IsZero() || hash.Sum(nil).IsZero() {
		t.Fatal("IsZero must be true only for the zero hash (SHA-256 of nothing is not zero)")
	}
	a, b := hash.Hash{0: 1}, hash.Hash{0: 2}
	if a.Compare(b) != -1 || b.Compare(a) != 1 || a.Compare(a) != 0 {
		t.Fatalf("Compare is not bytewise: %d %d %d", a.Compare(b), b.Compare(a), a.Compare(a))
	}
	c := hash.Hash{31: 1}
	if zero.Compare(c) != -1 {
		t.Fatal("Compare must consider the last byte")
	}
}
