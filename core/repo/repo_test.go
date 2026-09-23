package repo_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/auth"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/mem"
	"github.com/SmithOperatingSolutions/snapshot-core/core/boundary"
	"github.com/SmithOperatingSolutions/snapshot-core/core/cdc"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/memstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/packstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/object"
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

// A host writes objects into the repository's own chunk store, with the
// repository's geometry, and reads them back after the version graph next
// changes a ref and the repository reopens; the store it is handed cannot
// swap the root behind the version graph.
func TestHostsWriteObjectsThroughTheRepository(t *testing.T) {
	o := options(t, mem.New(), keyring(t))
	o.Geometry = repo.Geometry{CDC: cdc.Geometry{Min: 4 << 10, Max: 256 << 10, Mask: 0x3FFF},
		Nodes: boundary.Geometry{Min: 256, Target: 2048, Max: 8192}, InlineLimit: 1000, PackSize: 1 << 20}
	r := initRepo(t, o)
	c := r.Chunks()
	if c == nil {
		t.Fatal("the repository hands out no chunk store")
	}
	if _, swaps := c.(interface {
		CompareAndSetRoot(context.Context, hash.Hash, hash.Hash) error
	}); swaps {
		t.Fatal("the chunk store a host is handed can swap the root")
	}
	h, err := c.Put(ctx, []byte("an object's chunk"))
	if err != nil {
		t.Fatal(err)
	}
	head, err := r.Head(ctx, alice, vcs.MainBranch)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.CreateBranch(ctx, alice, "published", head.Hash); err != nil {
		t.Fatal(err) // any ref change publishes what was written
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	re, err := repo.Open(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	defer re.Close()
	if b, err := re.Chunks().Get(ctx, h); err != nil || string(b) != "an object's chunk" {
		t.Fatalf("after reopening the chunk reads %q, %v", b, err)
	}
	g := re.Config.Geometry
	if p := g.Prolly(); p.Nodes != o.Geometry.Nodes || p.InlineLimit != 1000 || p.Stream != g.Stream() {
		t.Fatalf("Prolly() = %+v", p)
	}
	if s := g.Stream(); s.CDC != o.Geometry.CDC || s.Nodes != o.Geometry.Nodes {
		t.Fatalf("Stream() = %+v", s)
	}
}

var errBackend = errors.New("injected: the backend went away")

// failingSwap is a blob store whose root swaps fail while fail is set.
type failingSwap struct {
	blob.BlobStore
	fail bool
}

func (s *failingSwap) SwapRoot(ctx context.Context, expected blob.Version, next []byte) (blob.Version, error) {
	if s.fail {
		return blob.NoVersion, errBackend
	}
	return s.BlobStore.SwapRoot(ctx, expected, next)
}

// An Init that stops after it has claimed the store with its config, but
// before the version graph is written (a crash, a backend gone away),
// leaves a store that Open reports as holding no repository and that the
// next Init with the same key finishes, on the geometry and repo id the
// config records. Another key cannot take it over. Before, the next Init
// found the config and called the store taken, while Open found no refs:
// the store was stuck for good.
func TestAnInitThatStoppedIsFinishedByTheNext(t *testing.T) {
	blobs := &failingSwap{BlobStore: mem.New(), fail: true}
	o := options(t, blobs, keyring(t))
	o.Geometry = repo.Geometry{CDC: cdc.DefaultGeometry(), Nodes: boundary.Geometry{Min: 256, Target: 2048, Max: 8192},
		InlineLimit: 1000, PackSize: 1 << 20}
	if _, err := repo.Init(ctx, alice, o); !errors.Is(err, errBackend) {
		t.Fatalf("fixture: an Init whose root swap fails = %v, want the backend's error", err)
	}
	var id seal.RepoID
	copy(id[:], readConfig(t, blobs)[6:22])
	blobs.fail = false
	if _, err := repo.Open(ctx, o); !errors.Is(err, repo.ErrNoRepo) {
		t.Errorf("Open after an Init that stopped = %v, want ErrNoRepo", err)
	}
	if _, err := repo.Init(ctx, alice, options(t, blobs, keyring(t))); !errors.Is(err, repo.ErrExists) {
		t.Errorf("an Init with another key, after one that stopped = %v, want ErrExists", err)
	}
	again := o
	again.Geometry = repo.Geometry{}
	r, err := repo.Init(ctx, alice, again)
	if err != nil {
		t.Fatalf("the Init after one that stopped = %v, want it to finish the repository", err)
	}
	if r.Config.Geometry != o.Geometry || r.Config.RepoID != id {
		t.Fatalf("the finished repository has geometry %+v and id %x, want the config's %+v and %x", r.Config.Geometry, r.Config.RepoID, o.Geometry, id)
	}
	head, err := r.Head(ctx, alice, vcs.MainBranch)
	if err != nil || head.Hash.IsZero() {
		t.Fatalf("the finished repository's main is %v (%v)", head.Hash, err)
	}
	if got, want := namespaceOf(t, r, head.Namespace), namespaceOf(t, nil, head.Namespace); got != want {
		t.Fatalf("the finished repository writes a namespace as %s, want %s: the geometry its config records", got, want)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	re, err := repo.Open(ctx, o)
	if err != nil {
		t.Fatalf("opening the finished repository: %v", err)
	}
	defer re.Close()
	if got, err := re.Head(ctx, alice, vcs.MainBranch); err != nil || got.Hash != head.Hash {
		t.Fatalf("reopened, main is %v (%v), want %v", got.Hash, err, head.Hash)
	}
	if _, err := repo.Init(ctx, alice, o); !errors.Is(err, repo.ErrExists) {
		t.Fatalf("an Init of the finished repository = %v, want ErrExists", err)
	}
}

// namespaceOf writes 500 objects into the namespace at root through r, or
// with nil through a fresh store with the geometry of the stopped Init's
// config, and returns the new root: it depends on the node geometry used.
func namespaceOf(t *testing.T, r *repo.Repo, root hash.Hash) hash.Hash {
	t.Helper()
	var n *object.Namespace
	var err error
	if r != nil {
		n, err = r.Namespace(ctx, root)
	} else {
		g := repo.Geometry{CDC: cdc.DefaultGeometry(), Nodes: boundary.Geometry{Min: 256, Target: 2048, Max: 8192},
			InlineLimit: 1000, PackSize: 1 << 20}
		n, err = object.New(ctx, memstore.New(), g.Prolly(), options(t, nil, nil).Registry)
	}
	if err != nil {
		t.Fatal(err)
	}
	e := n.Editor()
	for i := range 500 {
		ref := object.Ref{Model: 1, Root: model.Root{Hash: hash.Sum([]byte{byte(i), byte(i >> 8)}), Size: uint64(i), Format: 1}}
		if err := e.Put(fmt.Sprintf("objects/%04d", i), ref); err != nil {
			t.Fatal(err)
		}
	}
	if n, err = e.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	return n.Root()
}
