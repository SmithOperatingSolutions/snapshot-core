package repo_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/auth"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/mem"
	"github.com/SmithOperatingSolutions/snapshot-core/core/boundary"
	"github.com/SmithOperatingSolutions/snapshot-core/core/cdc"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/packstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/repo"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
	"github.com/SmithOperatingSolutions/snapshot-core/core/vcs"
)

var (
	ctx   = context.Background()
	alice = auth.Principal{ID: "user:alice"}
)

type fake model.ID

func (f fake) ID() model.ID                                           { return model.ID(f) }
func (fake) FormatVersion() uint16                                    { return 1 }
func (fake) Validate(context.Context, model.Root, chunk.Reader) error { return nil }
func (fake) Diff(context.Context, model.Root, model.Root, chunk.Reader) (model.DiffIter, error) {
	return nil, errors.New("unused")
}
func (fake) Merge(context.Context, model.Root, model.Root, model.Root, chunk.ReadWriter) (model.MergeResult, error) {
	return model.MergeResult{}, errors.New("unused")
}

func options(t *testing.T, blobs blob.BlobStore, keys *seal.Keyring) repo.Options {
	t.Helper()
	reg, err := model.NewRegistry(fake(1), fake(2))
	if err != nil {
		t.Fatal(err)
	}
	return repo.Options{Blobs: blobs, Keys: keys, Registry: reg, Authorizer: auth.AllowAll{},
		Clock: func() time.Time { return time.Date(2026, 9, 23, 0, 0, 0, 0, time.UTC) }}
}

func keyring(t *testing.T) *seal.Keyring {
	t.Helper()
	k, err := seal.NewKeyring()
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func initRepo(t *testing.T, o repo.Options) *repo.Repo {
	t.Helper()
	r, err := repo.Init(ctx, alice, o)
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	return r
}

func TestInitThenOpen(t *testing.T) {
	o := options(t, mem.New(), keyring(t))
	r := initRepo(t, o)
	head, err := r.Head(ctx, alice, vcs.MainBranch)
	if err != nil || head.Hash.IsZero() {
		t.Fatalf("a new repository's main is %v, %v", head.Hash, err)
	}
	want := repo.DefaultGeometry()
	if r.Config.Geometry != want || r.Config.KeyID != o.Keys.ID() || r.Config.RepoID == (seal.RepoID{}) {
		t.Fatalf("Init recorded %+v; want the default geometry %+v, this key and a repo id", r.Config, want)
	}
	if want.InlineLimit != 256<<10 || want.PackSize != packstore.DefaultPackSize || want.CDC != cdc.DefaultGeometry() || want.Nodes != boundary.DefaultGeometry() {
		t.Fatalf("DefaultGeometry = %+v", want)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	re, err := repo.Open(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	defer re.Close()
	if again, err := re.Head(ctx, alice, vcs.MainBranch); err != nil || again.Hash != head.Hash || re.Config != r.Config {
		t.Fatalf("reopened: main %v (%v), config %+v; want %v, %+v", again.Hash, err, re.Config, head.Hash, r.Config)
	}
	if _, err := repo.Init(ctx, alice, o); !errors.Is(err, repo.ErrExists) {
		t.Fatalf("a second Init = %v, want ErrExists", err)
	}
}

func TestOpenTellsAWrongKeyFromADamagedConfig(t *testing.T) {
	blobs := mem.New()
	o := options(t, blobs, keyring(t))
	initRepo(t, o).Close()
	other := options(t, blobs, keyring(t))
	if _, err := repo.Open(ctx, other); !errors.Is(err, repo.ErrWrongKey) {
		t.Fatalf("Open with another key = %v, want ErrWrongKey", err)
	}
	cfg := readConfig(t, blobs)
	damaged := bytes.Clone(cfg)
	damaged[len(damaged)-1] ^= 1
	forged := bytes.Clone(cfg)
	otherID := other.Keys.ID()
	copy(forged[22:54], otherID[:]) // the header's key id, now the other key's
	for name, c := range map[string]struct {
		b []byte
		o repo.Options
	}{"a flipped sealed byte": {damaged, o}, "a forged key id": {forged, other}} {
		writeConfig(t, blobs, c.b)
		if _, err := repo.Open(ctx, c.o); !errors.Is(err, repo.ErrConfig) {
			t.Errorf("%s: Open = %v, want ErrConfig", name, err)
		}
	}
	writeConfig(t, blobs, cfg)
	if r, err := repo.Open(ctx, o); err != nil {
		t.Fatalf("positive control: the original config opens: %v", err)
	} else {
		r.Close()
	}
}

func readConfig(t *testing.T, blobs blob.BlobStore) []byte {
	t.Helper()
	rc, err := blobs.Get(ctx, "config", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func writeConfig(t *testing.T, blobs blob.BlobStore, b []byte) {
	t.Helper()
	if err := blobs.Delete(ctx, "config"); err != nil {
		t.Fatal(err)
	}
	if err := blobs.Put(ctx, "config", bytes.NewReader(b), int64(len(b))); err != nil {
		t.Fatal(err)
	}
}

func TestThereIsNoRepositoryWithoutAConfigOrAKey(t *testing.T) {
	o := options(t, mem.New(), keyring(t))
	if _, err := repo.Open(ctx, o); !errors.Is(err, repo.ErrNoRepo) {
		t.Fatalf("Open of an empty store = %v, want ErrNoRepo", err)
	}
	initRepo(t, o).Close()
	noKey := o
	noKey.Keys = nil
	if _, err := repo.Open(ctx, noKey); err == nil {
		t.Fatal("a repository opened without a key: there is no plaintext mode")
	}
	fresh := options(t, mem.New(), nil)
	if _, err := repo.Init(ctx, alice, fresh); err == nil {
		t.Fatal("a repository was created without a key")
	}
}

// The config object is the DESIGN §8 layout, read here by hand: the header
// in the clear, the geometry under the Config key.
func TestTheConfigIsTheDocumentedFormat(t *testing.T) {
	blobs := mem.New()
	o := options(t, blobs, keyring(t))
	o.Geometry = repo.Geometry{CDC: cdc.Geometry{Min: 4 << 10, Max: 256 << 10, Mask: 0x3FFF},
		Nodes: boundary.Geometry{Min: 256, Target: 2048, Max: 8192}, InlineLimit: 1000, PackSize: 1 << 20}
	r := initRepo(t, o)
	r.Close()
	b := readConfig(t, blobs)
	if string(b[:4]) != "SCRC" || binary.LittleEndian.Uint16(b[4:]) != 1 {
		t.Fatalf("the config starts %q version %d", b[:4], binary.LittleEndian.Uint16(b[4:]))
	}
	var id seal.RepoID
	var kid seal.KeyID
	var salt seal.Salt
	copy(id[:], b[6:22])
	copy(kid[:], b[22:54])
	copy(salt[:], b[54:86])
	if id != r.Config.RepoID || kid != o.Keys.ID() {
		t.Fatalf("the header names repo %x, key %x; want %x, %x", id, kid, r.Config.RepoID, o.Keys.ID())
	}
	key, err := o.Keys.Key(seal.Config, id, salt)
	if err != nil {
		t.Fatal(err)
	}
	p, err := key.Open(b[:86], b[86:])
	if err != nil {
		t.Fatalf("the config does not open under the Config key with the header as context: %v", err)
	}
	le := binary.LittleEndian
	got := repo.Geometry{
		CDC:         cdc.Geometry{Min: int(le.Uint32(p[6:])), Max: int(le.Uint32(p[10:])), Mask: le.Uint64(p[14:])},
		Nodes:       boundary.Geometry{Min: int(le.Uint32(p[22:])), Target: int(le.Uint32(p[26:])), Max: int(le.Uint32(p[30:]))},
		InlineLimit: int(le.Uint32(p[34:])), PackSize: int(le.Uint32(p[38:])),
	}
	if string(p[:4]) != "SCRP" || le.Uint16(p[4:]) != 1 || len(p) != 42 || got != o.Geometry {
		t.Fatalf("the plaintext is %q v%d (%d bytes) holding %+v; want SCRP v1, 42 bytes, %+v", p[:4], le.Uint16(p[4:]), len(p), got, o.Geometry)
	}
	re, err := repo.Open(ctx, o)
	if err != nil || re.Config.Geometry != o.Geometry {
		t.Fatalf("reopened with geometry %+v (%v), want %+v", re.Config.Geometry, err, o.Geometry)
	}
	re.Close()
}

// A geometry the core cannot work with is refused, and nothing is written.
func TestInitValidatesTheGeometry(t *testing.T) {
	good := options(t, mem.New(), keyring(t))
	good.Geometry = repo.Geometry{CDC: cdc.DefaultGeometry(), Nodes: boundary.DefaultGeometry(), InlineLimit: 100, PackSize: 1 << 20}
	initRepo(t, good).Close() // positive control
	for name, g := range map[string]repo.Geometry{
		"a negative inline limit": {CDC: cdc.DefaultGeometry(), Nodes: boundary.DefaultGeometry(), InlineLimit: -1, PackSize: 1 << 20},
		"unusable nodes":          {CDC: cdc.DefaultGeometry(), Nodes: boundary.Geometry{Min: 1, Target: 2, Max: 3}, InlineLimit: 100, PackSize: 1 << 20},
		"unusable CDC":            {CDC: cdc.Geometry{Min: 10, Max: 5, Mask: 1}, Nodes: boundary.DefaultGeometry(), InlineLimit: 100, PackSize: 1 << 20},
		"a pack under a header":   {CDC: cdc.DefaultGeometry(), Nodes: boundary.DefaultGeometry(), InlineLimit: 100, PackSize: 10},
	} {
		blobs := mem.New()
		o := options(t, blobs, keyring(t))
		o.Geometry = g
		if _, err := repo.Init(ctx, alice, o); err == nil {
			t.Errorf("%s: Init succeeded", name)
		}
		if infos, err := blobs.List(ctx, "", "", blob.MaxListPage); err != nil || len(infos) != 0 {
			t.Errorf("%s: a refused Init left %d objects", name, len(infos))
		}
	}
}
