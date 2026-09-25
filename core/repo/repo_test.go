package repo_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"testing/iotest"
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

// configName is the name of the store's one config object.
func configName(t *testing.T, blobs blob.BlobStore) string {
	t.Helper()
	infos, err := blobs.List(ctx, "config/", "", blob.MaxListPage)
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 1 {
		t.Fatalf("the store holds %d configs, want one", len(infos))
	}
	return infos[0].Name
}

func readConfig(t *testing.T, blobs blob.BlobStore) []byte {
	t.Helper()
	rc, err := blobs.Get(ctx, configName(t, blobs), 0, -1)
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
	name := configName(t, blobs)
	if err := blobs.Delete(ctx, name); err != nil {
		t.Fatal(err)
	}
	if err := blobs.Put(ctx, name, bytes.NewReader(b), int64(len(b))); err != nil {
		t.Fatal(err)
	}
}

// overwriting is a store whose Put replaces an existing object instead of
// refusing it: what two Inits racing on an objects-only S3 store (#8) can
// do to each other's config, made certain.
type overwriting struct{ blob.BlobStore }

func (s overwriting) Put(ctx context.Context, name string, r io.Reader, size int64) error {
	if err := s.Delete(ctx, name); err != nil {
		return err
	}
	return s.BlobStore.Put(ctx, name, r, size)
}

// beforeFirstPut runs do once, before the store's first Put lands: another
// writer's whole Init, while this one is between reading the store and
// writing to it.
type beforeFirstPut struct {
	blob.BlobStore
	do func()
}

func (s *beforeFirstPut) Put(ctx context.Context, name string, r io.Reader, size int64) error {
	if do := s.do; do != nil {
		s.do = nil
		do()
	}
	return s.BlobStore.Put(ctx, name, r, size)
}

var stores = map[string]func() blob.BlobStore{
	"a store that refuses existing names": func() blob.BlobStore { return mem.New() },
	"a store whose puts overwrite":        func() blob.BlobStore { return overwriting{mem.New()} },
}

// countConfigs is how many configs the store holds.
func countConfigs(t *testing.T, bs blob.BlobStore) int {
	t.Helper()
	infos, err := bs.List(ctx, "config/", "", blob.MaxListPage)
	if err != nil {
		t.Fatal(err)
	}
	return len(infos)
}

// A second Init on a store, with another key, leaves the first key's
// repository as it was and writes nothing: it is ErrExists, its key is
// ErrWrongKey to Open, the store still holds the one config, and the first
// key opens and reads main. So on a store that refuses existing names, and
// so on one whose put-if-absent is best effort (#8): Init claims the store
// with the root's first swap.
func TestAnotherInitCannotTakeOverARepository(t *testing.T) {
	for name, mk := range stores {
		t.Run(name, func(t *testing.T) {
			bs := mk()
			o := options(t, bs, keyring(t))
			r := initRepo(t, o)
			first, err := r.Head(ctx, alice, vcs.MainBranch)
			if err != nil {
				t.Fatal(err)
			}
			r.Close()
			other := options(t, bs, keyring(t))
			if _, err := repo.Init(ctx, alice, other); !errors.Is(err, repo.ErrExists) {
				t.Fatalf("a second Init with another key = %v, want ErrExists", err)
			}
			if n := countConfigs(t, bs); n != 1 {
				t.Fatalf("after a refused Init the store holds %d configs, want the one", n)
			}
			if _, err := repo.Open(ctx, other); !errors.Is(err, repo.ErrWrongKey) {
				t.Fatalf("Open with the second key = %v, want ErrWrongKey", err)
			}
			re, err := repo.Open(ctx, o)
			if err != nil {
				t.Fatalf("after another key's Init, the first key no longer opens its repository: %v", err)
			}
			defer re.Close()
			if got, err := re.Head(ctx, alice, vcs.MainBranch); err != nil || got.Hash != first.Hash {
				t.Fatalf("after another key's Init, main is %v (%v), want %s", got.Hash, err, first.Hash.Short())
			}
		})
	}
}

// Two Inits with different keys under way at once: the second lands whole
// while the first is between reading the store and writing its config. The
// first is ErrExists and its key ErrWrongKey to Open; the second's
// repository is intact, its key opens it and reads main. So on a store
// whose put-if-absent is best effort too: each Init's config has a name of
// its own, so the overtaken one overwrote nothing (#8).
func TestInitsRacingDoNotOverwriteEachOther(t *testing.T) {
	for name, mk := range stores {
		t.Run(name, func(t *testing.T) {
			bs := mk()
			hooked := &beforeFirstPut{BlobStore: bs}
			o := options(t, hooked, keyring(t))
			other := options(t, bs, keyring(t))
			var landed *repo.Repo
			hooked.do = func() { landed = initRepo(t, other) }
			if _, err := repo.Init(ctx, alice, o); !errors.Is(err, repo.ErrExists) {
				t.Fatalf("an Init another Init overtook = %v, want ErrExists", err)
			}
			if landed == nil {
				t.Fatal("fixture: the overtaken Init never wrote, so the other never ran")
			}
			head, err := landed.Head(ctx, alice, vcs.MainBranch)
			if err != nil {
				t.Fatal(err)
			}
			landed.Close()
			if _, err := repo.Open(ctx, o); !errors.Is(err, repo.ErrWrongKey) {
				t.Fatalf("Open with the overtaken Init's key = %v, want ErrWrongKey", err)
			}
			re, err := repo.Open(ctx, other)
			if err != nil {
				t.Fatalf("the Init that landed no longer opens its repository: the overtaken one overwrote its config: %v", err)
			}
			defer re.Close()
			if got, err := re.Head(ctx, alice, vcs.MainBranch); err != nil || got.Hash != head.Hash {
				t.Fatalf("the repository that landed has main %v (%v), want %s", got.Hash, err, head.Hash.Short())
			}
		})
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

// #40: a host writing through the repository gets the write path the
// version graph's own store offers, not a narrower one: the chunk store it
// is handed prepares chunks apart from storing them (chunk.Preparer, so
// stream.Write hashes and compresses on every core) and can be told a
// stream is over (chunk.Flusher, so the pack holding a big write's last
// chunks uploads beside the host's next work, not inside its next commit).
// It still cannot swap the root.
func TestAHostWritingThroughTheRepositoryGetsTheParallelPathAndFlush(t *testing.T) {
	blobs := mem.New()
	o := options(t, blobs, keyring(t))
	o.Geometry = repo.Geometry{CDC: cdc.Geometry{Min: 4 << 10, Max: 256 << 10, Mask: 0x3FFF},
		Nodes: boundary.Geometry{Min: 256, Target: 2048, Max: 8192}, InlineLimit: 1000, PackSize: 1 << 20}
	r := initRepo(t, o)
	defer r.Close()
	c := r.Chunks()
	if _, swaps := c.(interface {
		CompareAndSetRoot(context.Context, hash.Hash, hash.Hash) error
	}); swaps {
		t.Fatal("the chunk store a host is handed can swap the root")
	}
	p, ok := c.(chunk.Preparer)
	if !ok {
		t.Fatal("the chunk store a host is handed is not a chunk.Preparer: every stream a host writes through the repository is hashed and compressed on one goroutine")
	}
	data := []byte("a chunk prepared on a host's goroutine")
	prep, err := p.Prepare(data)
	if err != nil {
		t.Fatal(err)
	}
	h, err := p.PutPrepared(ctx, prep)
	if err != nil {
		t.Fatal(err)
	}
	if h != hash.Sum(data) || prep.Hash() != h || prep.Len() != len(data) {
		t.Fatalf("a prepared chunk stored under %s (prepared %s, %d bytes), want %s, %d bytes", h.Short(), prep.Hash().Short(), prep.Len(), hash.Sum(data).Short(), len(data))
	}
	if got, err := c.Get(ctx, h); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("the prepared chunk reads back %q, %v", got, err)
	}

	f, ok := c.(chunk.Flusher)
	if !ok {
		t.Fatal("the chunk store a host is handed is not a chunk.Flusher: the pack holding a host's last chunks uploads only inside its next commit")
	}
	packs := func() int {
		infos, err := blobs.List(ctx, "packs/", "", blob.MaxListPage)
		if err != nil {
			t.Fatal(err)
		}
		return len(infos)
	}
	before := packs()
	// A quarter of a pack, incompressible: over the eighth Flush uploads,
	// under the size that sends a pack on its own.
	src := mrand.NewChaCha8([32]byte{40})
	for range 4 {
		b := make([]byte, 64<<10)
		_, _ = src.Read(b)
		if _, err := c.Put(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	if n := packs(); n != before {
		t.Fatalf("a quarter of a pack uploaded %d packs before any flush: the fixture does not hold the pack back", n-before)
	}
	if err := f.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for packs() == before {
		if time.Now().After(deadline) {
			t.Fatal("five seconds after a host flushed a quarter of a pack, no pack was uploaded: it waits for the next commit")
		}
		time.Sleep(5 * time.Millisecond)
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

// forgeConfig seals a config by hand, under keys and for repository id,
// with the header's magic and version and the plaintext as given: a
// config that authenticates, saying whatever a test needs it to.
func forgeConfig(t *testing.T, keys *seal.Keyring, id seal.RepoID, magic string, version uint16, plain []byte) []byte {
	t.Helper()
	salt, err := seal.NewSalt()
	if err != nil {
		t.Fatal(err)
	}
	kid := keys.ID()
	h := binary.LittleEndian.AppendUint16([]byte(magic), version)
	h = append(append(append(h, id[:]...), kid[:]...), salt[:]...)
	key, err := keys.Key(seal.Config, id, salt)
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := key.Seal(h, plain)
	if err != nil {
		t.Fatal(err)
	}
	return append(h, sealed...)
}

// plaintext is a config plaintext of the given magic and version.
func plaintext(magic string, version uint16, g repo.Geometry) []byte {
	le := binary.LittleEndian
	p := le.AppendUint16([]byte(magic), version)
	p = le.AppendUint32(le.AppendUint32(p, uint32(g.CDC.Min)), uint32(g.CDC.Max))
	p = le.AppendUint64(p, g.CDC.Mask)
	p = le.AppendUint32(le.AppendUint32(le.AppendUint32(p, uint32(g.Nodes.Min)), uint32(g.Nodes.Target)), uint32(g.Nodes.Max))
	return le.AppendUint32(le.AppendUint32(p, uint32(g.InlineLimit)), uint32(g.PackSize))
}

// A config is checked before anything is read by it. One whose header is
// another format or version, whose plaintext is another format or version
// or has a byte more, or that names a geometry the core cannot use, is
// ErrConfig, though each is sealed under the repository's own key; so is
// one too short to hold a header, or over 4 KiB. The positive control, a
// config sealed by hand the same way, opens.
func TestTheConfigIsCheckedBeforeItIsUsed(t *testing.T) {
	blobs := mem.New()
	o := options(t, blobs, keyring(t))
	initRepo(t, o).Close()
	cfg := readConfig(t, blobs)
	var id seal.RepoID
	copy(id[:], cfg[6:22])
	good := repo.DefaultGeometry()
	unusable := good
	unusable.InlineLimit = 600 << 10
	forge := func(magic string, version uint16, plain []byte) []byte {
		return forgeConfig(t, o.Keys, id, magic, version, plain)
	}
	for _, c := range []struct {
		name string
		b    []byte
	}{
		{"a header of another format", forge("SCRX", 1, plaintext("SCRP", 1, good))},
		{"a header of version 2", forge("SCRC", 2, plaintext("SCRP", 1, good))},
		{"a plaintext of another format", forge("SCRC", 1, plaintext("SCRX", 1, good))},
		{"a plaintext of version 2", forge("SCRC", 1, plaintext("SCRP", 2, good))},
		{"a plaintext with a byte more", forge("SCRC", 1, append(plaintext("SCRP", 1, good), 0))},
		{"an inline limit of 600 KiB", forge("SCRC", 1, plaintext("SCRP", 1, unusable))},
		{"85 bytes", cfg[:85]},
		{"4 KiB and a byte", append(bytes.Clone(cfg), make([]byte, 4<<10+1-len(cfg))...)},
	} {
		writeConfig(t, blobs, c.b)
		if _, err := repo.Open(ctx, o); !errors.Is(err, repo.ErrConfig) {
			t.Errorf("%s: Open = %v, want ErrConfig", c.name, err)
		}
	}
	writeConfig(t, blobs, forge("SCRC", 1, plaintext("SCRP", 1, good)))
	r, err := repo.Open(ctx, o)
	if err != nil {
		t.Fatalf("positive control: a config sealed by hand opens: %v", err)
	}
	r.Close()
}

// Init asks for admin before it writes anything, and a destroyed key seals
// nothing: a denied Init (a nil authorizer denies too), or one with a
// destroyed key, leaves the store as empty as it found it; a destroyed
// key opens nothing either.
func TestARefusedInitWritesNothing(t *testing.T) {
	for _, c := range []struct {
		name string
		set  func(o *repo.Options)
		want error
	}{
		{"denied", func(o *repo.Options) { o.Authorizer = auth.DenyAll{} }, auth.ErrDenied},
		{"with a nil authorizer", func(o *repo.Options) { o.Authorizer = nil }, auth.ErrDenied},
		{"with a destroyed key", func(o *repo.Options) { o.Keys.Destroy() }, seal.ErrDestroyed},
	} {
		blobs := mem.New()
		o := options(t, blobs, keyring(t))
		c.set(&o)
		if _, err := repo.Init(ctx, alice, o); !errors.Is(err, c.want) {
			t.Errorf("Init %s = %v, want %v", c.name, err, c.want)
		}
		if infos, err := blobs.List(ctx, "", "", blob.MaxListPage); err != nil || len(infos) != 0 {
			t.Errorf("Init %s left %d objects (%v)", c.name, len(infos), err)
		}
		if root, err := blobs.Root(ctx); err != nil || len(root.Value) != 0 {
			t.Errorf("Init %s left a root (%v)", c.name, err)
		}
	}
	o := options(t, mem.New(), keyring(t))
	initRepo(t, o).Close()
	o.Keys.Destroy()
	if _, err := repo.Open(ctx, o); !errors.Is(err, seal.ErrDestroyed) {
		t.Fatalf("Open with a destroyed key = %v, want ErrDestroyed", err)
	}
}

// The blob store calls faultyBlobs can fail; a read is of what a Get
// returned.
const (
	blobPut = iota
	blobGet
	blobRead
	blobStat
	blobList
	blobRoot
	blobSwap
	blobKinds
)

var blobCallNames = [blobKinds]string{"put", "get", "read", "stat", "list", "root read", "root swap"}

// faultyBlobs fails exactly one blob store call, the at-th of one kind
// (at 0: none), and passes everything else through.
type faultyBlobs struct {
	blob.BlobStore
	kind  int
	at    int64
	calls [blobKinds]atomic.Int64
}

func (s *faultyBlobs) fails(kind int) bool { return s.calls[kind].Add(1) == s.at && s.kind == kind }

func (s *faultyBlobs) arm(kind int, at int64) {
	s.kind, s.at = kind, at
	for i := range s.calls {
		s.calls[i].Store(0)
	}
}

func (s *faultyBlobs) counts() (c [blobKinds]int64) {
	for i := range c {
		c[i] = s.calls[i].Load()
	}
	return c
}

func (s *faultyBlobs) Put(ctx context.Context, name string, r io.Reader, size int64) error {
	if s.fails(blobPut) {
		return errBackend
	}
	return s.BlobStore.Put(ctx, name, r, size)
}

func (s *faultyBlobs) Get(ctx context.Context, name string, off, n int64) (io.ReadCloser, error) {
	if s.fails(blobGet) {
		return nil, errBackend
	}
	rc, err := s.BlobStore.Get(ctx, name, off, n)
	if err != nil || !s.fails(blobRead) {
		return rc, err
	}
	_ = rc.Close()
	return io.NopCloser(iotest.ErrReader(errBackend)), nil
}

func (s *faultyBlobs) Stat(ctx context.Context, name string) (blob.Info, error) {
	if s.fails(blobStat) {
		return blob.Info{}, errBackend
	}
	return s.BlobStore.Stat(ctx, name)
}

func (s *faultyBlobs) List(ctx context.Context, prefix, after string, limit int) ([]blob.Info, error) {
	if s.fails(blobList) {
		return nil, errBackend
	}
	return s.BlobStore.List(ctx, prefix, after, limit)
}

func (s *faultyBlobs) Root(ctx context.Context) (blob.Root, error) {
	if s.fails(blobRoot) {
		return blob.Root{}, errBackend
	}
	return s.BlobStore.Root(ctx)
}

func (s *faultyBlobs) SwapRoot(ctx context.Context, expected blob.Version, next []byte) (blob.Version, error) {
	if s.fails(blobSwap) {
		return blob.NoVersion, errBackend
	}
	return s.BlobStore.SwapRoot(ctx, expected, next)
}

// Every blob store call Init and Open make, failed in turn, is their
// error, never a missing repository or a damaged config; and after any
// one of them Init again makes a working repository, finishing whatever
// the failed Init began, and a failed Open leaves one that opens.
func TestEveryStoreFailureIsSurvivable(t *testing.T) {
	keys := keyring(t)
	b := &faultyBlobs{BlobStore: mem.New()}
	o := options(t, b, keys)
	initRepo(t, o).Close()
	failures := 0
	for k, n := range b.counts() {
		for at := int64(1); at <= n; at++ {
			b := &faultyBlobs{BlobStore: mem.New()}
			o := options(t, b, keys)
			b.arm(k, at)
			if _, err := repo.Init(ctx, alice, o); !errors.Is(err, errBackend) {
				t.Fatalf("Init with %s %d of %d failing = %v, want the backend's error", blobCallNames[k], at, n, err)
			}
			b.arm(0, 0)
			r, err := repo.Init(ctx, alice, o)
			if err != nil {
				t.Fatalf("Init again after %s %d of %d failed = %v, want a repository", blobCallNames[k], at, n, err)
			}
			head, err := r.Head(ctx, alice, vcs.MainBranch)
			r.Close()
			if err != nil {
				t.Fatalf("after %s %d of %d failed, the repository Init made again has no main: %v", blobCallNames[k], at, n, err)
			}
			re, err := repo.Open(ctx, o)
			if err != nil {
				t.Fatalf("after %s %d of %d failed, the repository does not open: %v", blobCallNames[k], at, n, err)
			}
			if again, err := re.Head(ctx, alice, vcs.MainBranch); err != nil || again.Hash != head.Hash {
				t.Fatalf("after %s %d of %d failed, reopened main is %v (%v), want %v", blobCallNames[k], at, n, again.Hash, err, head.Hash)
			}
			re.Close()
			failures++
		}
	}
	b.arm(0, 0)
	re, err := repo.Open(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	re.Close()
	for k, n := range b.counts() {
		for at := int64(1); at <= n; at++ {
			b.arm(k, at)
			if _, err := repo.Open(ctx, o); !errors.Is(err, errBackend) {
				t.Fatalf("Open with %s %d of %d failing = %v, want the backend's error", blobCallNames[k], at, n, err)
			}
			b.arm(0, 0)
			re, err := repo.Open(ctx, o)
			if err != nil {
				t.Fatalf("Open after %s %d of %d failed = %v", blobCallNames[k], at, n, err)
			}
			re.Close()
			failures++
		}
	}
	if failures == 0 {
		t.Fatal("fixture: Init and Open made no blob store calls")
	}
}

// garbage leaves packs nothing reaches: each branch made and deleted
// publishes a working set and refs nodes in a pack of its own.
func garbage(t *testing.T, r *repo.Repo, n int) {
	t.Helper()
	head, err := r.Head(ctx, alice, vcs.MainBranch)
	if err != nil {
		t.Fatal(err)
	}
	for i := range n {
		name := fmt.Sprintf("tmp%d", i)
		if err := r.CreateBranch(ctx, alice, name, head.Hash); err != nil {
			t.Fatal(err)
		}
		if err := r.DeleteBranch(ctx, alice, name); err != nil {
			t.Fatal(err)
		}
	}
}

func packs(t *testing.T, bs blob.BlobStore) int {
	t.Helper()
	infos, err := bs.List(ctx, "packs/", "", blob.MaxListPage)
	if err != nil {
		t.Fatal(err)
	}
	return len(infos)
}

// stamped is a blob store whose objects are stamped by the test's clock, as
// a backend's own clock would move with the time the test lets pass.
type stamped struct {
	blob.BlobStore
	now    func() time.Time
	mu     sync.Mutex
	stamps map[string]time.Time
}

func (s *stamped) Put(ctx context.Context, name string, r io.Reader, size int64) error {
	err := s.BlobStore.Put(ctx, name, r, size)
	if err == nil {
		s.mu.Lock()
		s.stamps[name] = s.now()
		s.mu.Unlock()
	}
	return err
}

func (s *stamped) restamp(infos []blob.Info) []blob.Info {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range infos {
		if t, ok := s.stamps[infos[i].Name]; ok {
			infos[i].ModTime = t
		}
	}
	return infos
}

func (s *stamped) Stat(ctx context.Context, name string) (blob.Info, error) {
	i, err := s.BlobStore.Stat(ctx, name)
	return s.restamp([]blob.Info{i})[0], err
}

func (s *stamped) List(ctx context.Context, prefix, after string, limit int) ([]blob.Info, error) {
	infos, err := s.BlobStore.List(ctx, prefix, after, limit)
	return s.restamp(infos), err
}

// A repository is collected through repo.GC (DESIGN §9): with admin, on
// the raw store, GC condemns and, a grace window later, deletes the packs
// nothing reaches, and the repository still opens with main where it was.
// Without admin (a nil authorizer denies too) it is ErrDenied; on a
// NoDelete store it condemns but cannot delete (ErrDeleteForbidden), and
// the next run on the raw store deletes what it could not; on a store
// holding no repository it is ErrNoRepo.
func TestAnAdminCollectsTheRepositoryOnTheRawStore(t *testing.T) {
	var jump time.Duration
	now := func() time.Time { return time.Now().Add(jump) }
	blobs := &stamped{BlobStore: mem.New(), now: now, stamps: map[string]time.Time{}}
	o := options(t, blobs, keyring(t))
	o.Clock = now
	r := initRepo(t, o)
	head, err := r.Head(ctx, alice, vcs.MainBranch)
	if err != nil {
		t.Fatal(err)
	}
	garbage(t, r, 3)
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	before := packs(t, blobs)

	for name, az := range map[string]auth.Authorizer{"DenyAll": auth.DenyAll{}, "nil": nil} {
		denied := o
		denied.Authorizer = az
		if _, err := repo.GC(ctx, alice, denied, time.Hour); !errors.Is(err, auth.ErrDenied) {
			t.Fatalf("GC with %s = %v, want ErrDenied", name, err)
		}
	}
	if _, err := repo.GC(ctx, alice, options(t, mem.New(), o.Keys), time.Hour); !errors.Is(err, repo.ErrNoRepo) {
		t.Fatalf("GC of an empty store = %v, want ErrNoRepo", err)
	}
	first, err := repo.GC(ctx, alice, o, time.Hour)
	if err != nil || first.Condemned == 0 || len(first.Deleted) != 0 {
		t.Fatalf("the first GC condemned %d, deleted %v (%v); want some condemned, nothing deleted", first.Condemned, first.Deleted, err)
	}
	jump = time.Hour + time.Minute
	guarded := o
	guarded.Blobs = blob.NoDelete(blobs)
	if _, err := repo.GC(ctx, alice, guarded, time.Hour); !errors.Is(err, blob.ErrDeleteForbidden) {
		t.Fatalf("GC on a NoDelete store = %v, want ErrDeleteForbidden", err)
	}
	if n := packs(t, blobs); n != before {
		t.Fatalf("GC on a NoDelete store left %d packs of %d", n, before)
	}
	if _, err := repo.GC(ctx, alice, o, time.Hour); err != nil {
		t.Fatal(err)
	}
	if n := packs(t, blobs); n >= before {
		t.Fatalf("GC on the raw store a grace window on left %d packs of %d", n, before)
	}
	re, err := repo.Open(ctx, o)
	if err != nil {
		t.Fatalf("after GC the repository does not open: %v", err)
	}
	defer re.Close()
	if got, err := re.Head(ctx, alice, vcs.MainBranch); err != nil || got.Hash != head.Hash {
		t.Fatalf("after GC main is %v (%v), want %v", got.Hash, err, head.Hash)
	}
}

// beforeFirstSwap runs do once, before the store's first root swap: another
// Init's whole run, while this one is about to claim.
type beforeFirstSwap struct {
	blob.BlobStore
	do func()
}

func (s *beforeFirstSwap) SwapRoot(ctx context.Context, expected blob.Version, next []byte) (blob.Version, error) {
	if do := s.do; do != nil {
		s.do = nil
		do()
	}
	return s.BlobStore.SwapRoot(ctx, expected, next)
}

// Two Inits with the same key finishing one stopped Init at once: both take
// its config, the one whose swap lands owns the store, the other is
// ErrExists, and the store still holds that one config; the key opens the
// repository and reads main.
func TestAStoppedInitFinishedTwiceAtOnceIsFinishedOnce(t *testing.T) {
	bs := mem.New()
	stopped := &failingSwap{BlobStore: bs, fail: true}
	keys := keyring(t)
	if _, err := repo.Init(ctx, alice, options(t, stopped, keys)); !errors.Is(err, errBackend) {
		t.Fatalf("fixture: an Init whose root swap fails = %v, want the backend's error", err)
	}
	hooked := &beforeFirstSwap{BlobStore: bs}
	other := options(t, bs, keys)
	var landed *repo.Repo
	hooked.do = func() { landed = initRepo(t, other) }
	if _, err := repo.Init(ctx, alice, options(t, hooked, keys)); !errors.Is(err, repo.ErrExists) {
		t.Fatalf("the finishing Init that lost the swap = %v, want ErrExists", err)
	}
	if landed == nil {
		t.Fatal("fixture: the losing Init never swapped, so the other never ran")
	}
	head, err := landed.Head(ctx, alice, vcs.MainBranch)
	if err != nil {
		t.Fatal(err)
	}
	landed.Close()
	if n := countConfigs(t, bs); n != 1 {
		t.Fatalf("the store holds %d configs, want the stopped Init's one", n)
	}
	re, err := repo.Open(ctx, other)
	if err != nil {
		t.Fatalf("the finished repository does not open: %v", err)
	}
	defer re.Close()
	if got, err := re.Head(ctx, alice, vcs.MainBranch); err != nil || got.Hash != head.Hash {
		t.Fatalf("main is %v (%v), want %s", got.Hash, err, head.Hash.Short())
	}
}

// Among configs that open under the key, Open takes the one whose repo id
// authenticates the root: here a config of the same key that sorts first
// (a race lost long ago) and a page of another key's, with the real one
// last on the second page. GC finds the repository the same way.
func TestOpenTakesTheConfigThatAuthenticatesTheRoot(t *testing.T) {
	bs := mem.New()
	keys := keyring(t)
	o := options(t, bs, keys)
	r := initRepo(t, o)
	real := r.Config
	head, err := r.Head(ctx, alice, vcs.MainBranch)
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
	stray := forgeConfig(t, keys, seal.RepoID{}, "SCRC", 1, plaintext("SCRP", 1, repo.DefaultGeometry()))
	if err := bs.Put(ctx, "config/"+fmt.Sprintf("%032x", 0), bytes.NewReader(stray), int64(len(stray))); err != nil { // sorts first
		t.Fatal(err)
	}
	foreign := keyring(t)
	for i := 0; i < blob.MaxListPage; i++ {
		var id seal.RepoID
		binary.BigEndian.PutUint32(id[:4], uint32(i+1))
		b := forgeConfig(t, foreign, id, "SCRC", 1, plaintext("SCRP", 1, repo.DefaultGeometry()))
		if err := bs.Put(ctx, "config/"+fmt.Sprintf("%x", id[:]), bytes.NewReader(b), int64(len(b))); err != nil {
			t.Fatal(err)
		}
	}
	re, err := repo.Open(ctx, o)
	if err != nil {
		t.Fatalf("Open among %d other configs: %v", blob.MaxListPage+1, err)
	}
	defer re.Close()
	if re.Config != real {
		t.Fatalf("Open took config %x, want the root's %x", re.Config.RepoID, real.RepoID)
	}
	if got, err := re.Head(ctx, alice, vcs.MainBranch); err != nil || got.Hash != head.Hash {
		t.Fatalf("main is %v (%v), want %s", got.Hash, err, head.Hash.Short())
	}
	if _, err := repo.GC(ctx, alice, o, time.Hour); err != nil {
		t.Fatalf("GC among the other configs: %v", err)
	}
}

// Options.Journal (#34): a repository opened with it commits into the
// backend's journal. Its own reads see the commit at once, another open
// sees the published head until the journal is published (at Close here),
// and GC runs beside it, as beside any writer.
func TestARepositoryOpenedWithTheJournalCommitsThroughIt(t *testing.T) {
	o := options(t, mem.New(), keyring(t))
	r := initRepo(t, o)
	before, err := r.Head(ctx, alice, vcs.MainBranch)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	oj := o
	oj.Journal, oj.JournalInterval = true, time.Hour
	rj, err := repo.Open(ctx, oj)
	if err != nil {
		t.Fatalf("Open with the journal: %v", err)
	}
	ws, err := rj.WorkingSet(ctx, alice, vcs.MainBranch)
	if err != nil {
		t.Fatal(err)
	}
	c, err := rj.Commit(ctx, alice, vcs.MainBranch, ws, ws.Working, "journaled")
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if head, err := rj.Head(ctx, alice, vcs.MainBranch); err != nil || head.Hash != c.Hash {
		t.Fatalf("the committing repository's head is %v (%v), want the commit it just made", head.Hash.Short(), err)
	}
	other, err := repo.Open(ctx, o)
	if err != nil {
		t.Fatalf("a second open beside the journal writer: %v", err)
	}
	if head, err := other.Head(ctx, alice, vcs.MainBranch); err != nil || head.Hash != before.Hash {
		t.Fatalf("another open sees head %s (%v), want the published %s until the journal is published: the commit did not journal",
			head.Hash.Short(), err, before.Hash.Short())
	}
	_ = other.Close()
	if _, err := repo.GC(ctx, alice, oj, time.Hour); err != nil { // the host's own options, journal and all
		t.Fatalf("GC beside a repository holding the journal: %v", err)
	}
	if err := rj.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	after, err := repo.Open(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	defer after.Close()
	if head, err := after.Head(ctx, alice, vcs.MainBranch); err != nil || head.Hash != c.Hash {
		t.Fatalf("after Close another open sees head %s (%v), want the journaled commit %s", head.Hash.Short(), err, c.Hash.Short())
	}
}
