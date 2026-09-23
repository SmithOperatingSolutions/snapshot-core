package packstore_test

import (
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/mem"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/packstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/pack"
)

// Open refuses options it cannot work with, at Open rather than at the
// first Put: no backend or no keys, and a pack size outside what the pack
// format allows.
func TestOpenRefusesUnusableOptions(t *testing.T) {
	kr := keyring(t)
	s, err := packstore.Open(ctx, packstore.Options{Blobs: mem.New(), Keys: kr, Repo: repo})
	if err != nil {
		t.Fatalf("positive control: %v", err)
	}
	_ = s.Close()
	for name, o := range map[string]packstore.Options{
		"no backend":          {Keys: kr, Repo: repo},
		"no keys":             {Blobs: mem.New(), Repo: repo},
		"pack size too small": {Blobs: mem.New(), Keys: kr, Repo: repo, PackSize: pack.HeaderSize + pack.TrailerSize - 1},
		"pack size too large": {Blobs: mem.New(), Keys: kr, Repo: repo, PackSize: pack.MaxPackSize + 1},
	} {
		if s, err := packstore.Open(ctx, o); err == nil {
			_ = s.Close()
			t.Errorf("%s: Open succeeded", name)
		}
	}
}
