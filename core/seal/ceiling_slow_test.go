//go:build slow

package seal_test

import (
	"testing"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
)

// #13: a key file at the ceiling, a gibibyte and ten passes, is a key file
// a host can open: it derives and opens, in the seconds the ceiling is
// there to bound (the refusal test is the other side).
func TestSlowAKeyFileAtTheCeilingOpens(t *testing.T) {
	kr := mustKeyring(t)
	at := seal.Argon2Params{Time: 10, Memory: 1024 * 1024, Threads: 4}
	start := time.Now()
	f, err := seal.NewKeyFile(kr, []byte("passphrase"), at)
	if err != nil {
		t.Fatalf("a key file at the ceiling %+v is refused: %v", at, err)
	}
	opened, err := seal.OpenKeyFile(f, []byte("passphrase"))
	if err != nil {
		t.Fatalf("a key file at the ceiling does not open: %v", err)
	}
	if opened.ID() != kr.ID() {
		t.Fatalf("the key file at the ceiling opened to keyring %v, want %v", opened.ID(), kr.ID())
	}
	t.Logf("a key file at the ceiling derived twice in %v", time.Since(start).Round(time.Millisecond))
}
