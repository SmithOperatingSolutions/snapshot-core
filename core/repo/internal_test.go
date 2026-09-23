package repo

import (
	"bytes"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
)

// Hostile config bytes never panic, and whatever opens came from this key
// and names a geometry the core can use. The seeds are a good config and
// its truncations and forgeries.
func FuzzOpenConfig(f *testing.F) {
	kr, err := seal.KeyringFromBytes(bytes.Repeat([]byte{0x3c}, 32))
	if err != nil {
		f.Fatal(err)
	}
	good, err := Config{RepoID: seal.RepoID{1}, KeyID: kr.ID(), Geometry: DefaultGeometry()}.seal(kr)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(good)
	for _, n := range []int{0, 1, headerLen - 1, headerLen, headerLen + 1, len(good) - 1} {
		f.Add(good[:n])
	}
	f.Add(append(bytes.Clone(good), 0))
	f.Fuzz(func(t *testing.T, b []byte) {
		c, err := openConfig(b, kr)
		if err != nil {
			return
		}
		if c.KeyID != kr.ID() {
			t.Fatal("a config opened under a key it does not name")
		}
		if err := c.Geometry.validate(); err != nil {
			t.Fatalf("a config opened naming an unusable geometry: %v", err)
		}
	})
}
