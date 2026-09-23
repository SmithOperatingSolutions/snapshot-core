package packstore_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/mem"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/packstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
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

// An index object the manifest lists must be there and must authenticate:
// a missing one fails Open with an error naming it, an altered one with
// ErrCorrupt. Either way no store opens on a partial view of what was
// published, where published chunks would read as never stored.
func TestAMissingOrAlteredIndexObjectFailsOpen(t *testing.T) {
	bs, kr := mem.New(), keyring(t)
	s := open(t, bs, kr)
	h, _ := s.Put(ctx, []byte("located by the index object"))
	if err := s.CompareAndSetRoot(ctx, hash.Hash{}, h); err != nil {
		t.Fatal(err)
	}
	infos, err := bs.List(ctx, "index/", "", blob.MaxListPage)
	if err != nil || len(infos) != 1 {
		t.Fatalf("fixture: %d index objects (%v), want 1", len(infos), err)
	}
	name := infos[0].Name
	rc, err := bs.Get(ctx, name, 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	orig, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	o := packstore.Options{Blobs: bs, Keys: kr, Repo: repo}
	if re, err := packstore.Open(ctx, o); err != nil {
		t.Fatalf("positive control: %v", err)
	} else {
		_ = re.Close()
	}

	if err := bs.Delete(ctx, name); err != nil {
		t.Fatal(err)
	}
	if _, err := packstore.Open(ctx, o); !errors.Is(err, blob.ErrNotFound) || !strings.Contains(fmt.Sprint(err), name) {
		t.Errorf("Open with a listed index object missing = %v, want ErrNotFound naming %s", err, name)
	}
	altered := bytes.Clone(orig)
	altered[len(altered)-1] ^= 1
	if err := bs.Put(ctx, name, bytes.NewReader(altered), int64(len(altered))); err != nil {
		t.Fatal(err)
	}
	if _, err := packstore.Open(ctx, o); !errors.Is(err, chunk.ErrCorrupt) {
		t.Errorf("Open with an altered index object = %v, want ErrCorrupt", err)
	}
}
