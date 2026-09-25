// Package repo is the entry point: a repository on a BlobStore, created or
// opened with a master key, a model registry and an authorizer. Init writes
// the sealed config object (docs/DESIGN.md §8) that says how everything else
// is written; Open reads it before anything else.
package repo

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/auth"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/boundary"
	"github.com/SmithOperatingSolutions/snapshot-core/core/cdc"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/packstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/gc"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/pack"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
	"github.com/SmithOperatingSolutions/snapshot-core/core/stream"
	"github.com/SmithOperatingSolutions/snapshot-core/core/vcs"
	"github.com/SmithOperatingSolutions/snapshot-core/core/wire"
)

// Errors.
var (
	ErrExists   = errors.New("repo: the store already holds a repository")
	ErrNoRepo   = errors.New("repo: the store holds no repository")
	ErrWrongKey = errors.New("repo: this key did not create the repository")
	ErrConfig   = errors.New("repo: the config does not authenticate or decode")
)

// The config object, config/<repo id>: "SCRC" · version u16 · repo id [16] ·
// key id [32] · salt [32] · seal(Config, ctx = header, "SCRP" · version u16 ·
// cdc min u32 · cdc max u32 · cdc mask u64 · node min u32 · node target u32 ·
// node max u32 · inline limit u32 · pack size u32).
const (
	configPrefix = "config/"
	configMagic  = "SCRC"
	plainMagic   = "SCRP"
	configV1     = 1
	headerLen    = 4 + 2 + 16 + 32 + 32
	maxConfig    = 4 << 10
	maxInline    = 512 << 10
)

// Geometry is how a repository writes: fixed at Init, read back by Open.
type Geometry struct {
	CDC         cdc.Geometry
	Nodes       boundary.Geometry
	InlineLimit int
	PackSize    int
}

// DefaultGeometry is the default repo geometry.
func DefaultGeometry() Geometry {
	return Geometry{CDC: cdc.DefaultGeometry(), Nodes: boundary.DefaultGeometry(), InlineLimit: 256 << 10, PackSize: packstore.DefaultPackSize}
}

func (g Geometry) validate() error {
	if err := g.CDC.Validate(); err != nil {
		return err
	}
	if err := g.Nodes.Validate(); err != nil {
		return err
	}
	if g.InlineLimit < 0 || g.InlineLimit > maxInline {
		return fmt.Errorf("repo: inline limit %d outside 0..%d", g.InlineLimit, maxInline)
	}
	if g.PackSize < pack.HeaderSize+pack.TrailerSize || g.PackSize > pack.MaxPackSize {
		return fmt.Errorf("repo: pack size %d outside %d..%d", g.PackSize, pack.HeaderSize+pack.TrailerSize, pack.MaxPackSize)
	}
	return nil
}

// Options configures Init and Open.
type Options struct {
	Blobs      blob.BlobStore
	Keys       *seal.Keyring
	Registry   *model.Registry
	Authorizer auth.Authorizer  // nil denies everything
	Clock      func() time.Time // nil: time.Now
	Geometry   Geometry         // Init only; the zero Geometry is DefaultGeometry
	// Journal commits into the backend's journal where it keeps one
	// (blob/local, blob/multivol, blob/mem), publishing in the background
	// at most JournalInterval later (0: packstore.DefaultJournalInterval)
	// and at Close (packstore.Options.Journal, #34).
	Journal         packstore.JournalMode
	JournalInterval time.Duration
}

func (o Options) check() error {
	if o.Blobs == nil || o.Keys == nil || o.Registry == nil {
		return errors.New("repo: a store, a key and a model registry are required (there is no plaintext mode)")
	}
	return nil
}

// Config is what the config object records.
type Config struct {
	RepoID   seal.RepoID
	KeyID    seal.KeyID
	Geometry Geometry
}

func (c Config) seal(kr *seal.Keyring) ([]byte, error) {
	salt, err := seal.NewSalt()
	if err != nil {
		return nil, err
	}
	key, err := kr.Key(seal.Config, c.RepoID, salt)
	if err != nil {
		return nil, err
	}
	defer key.Destroy()
	var h, p wire.Writer
	h.Raw([]byte(configMagic))
	h.U16(configV1)
	h.Raw(c.RepoID[:])
	h.Raw(c.KeyID[:])
	h.Raw(salt[:])
	g := c.Geometry
	p.Raw([]byte(plainMagic))
	p.U16(configV1)
	p.U32(uint32(g.CDC.Min))
	p.U32(uint32(g.CDC.Max))
	p.U64(g.CDC.Mask)
	p.U32(uint32(g.Nodes.Min))
	p.U32(uint32(g.Nodes.Target))
	p.U32(uint32(g.Nodes.Max))
	p.U32(uint32(g.InlineLimit))
	p.U32(uint32(g.PackSize))
	sealed, err := key.Seal(h.Bytes(), p.Bytes())
	if err != nil {
		return nil, err
	}
	return append(h.Bytes(), sealed...), nil
}

// openConfig reads the header, refuses another key before decrypting, then
// opens and checks the geometry.
func openConfig(b []byte, kr *seal.Keyring) (Config, error) {
	if len(b) < headerLen {
		return Config{}, fmt.Errorf("%w: %d bytes", ErrConfig, len(b))
	}
	var c Config
	var salt seal.Salt
	h := wire.NewReader(b[:headerLen])
	magic, v := h.Fixed(4), h.U16()
	copy(c.RepoID[:], h.Fixed(len(c.RepoID)))
	copy(c.KeyID[:], h.Fixed(len(c.KeyID)))
	copy(salt[:], h.Fixed(len(salt)))
	if h.Done() != nil || string(magic) != configMagic || v != configV1 {
		return Config{}, fmt.Errorf("%w: header", ErrConfig)
	}
	if c.KeyID != kr.ID() {
		return Config{}, fmt.Errorf("%w: it was created with key %x", ErrWrongKey, c.KeyID[:6])
	}
	key, err := kr.Key(seal.Config, c.RepoID, salt)
	if err != nil {
		return Config{}, err
	}
	defer key.Destroy()
	plain, err := key.Open(b[:headerLen], b[headerLen:])
	if err != nil {
		return Config{}, fmt.Errorf("%w: it does not authenticate", ErrConfig)
	}
	p := wire.NewReader(plain)
	pm, pv := p.Fixed(4), p.U16()
	g := &c.Geometry
	g.CDC.Min, g.CDC.Max, g.CDC.Mask = int(p.U32()), int(p.U32()), p.U64()
	g.Nodes.Min, g.Nodes.Target, g.Nodes.Max = int(p.U32()), int(p.U32()), int(p.U32())
	g.InlineLimit, g.PackSize = int(p.U32()), int(p.U32())
	if p.Done() != nil || string(pm) != plainMagic || pv != configV1 {
		return Config{}, fmt.Errorf("%w: plaintext", ErrConfig)
	}
	if err := g.validate(); err != nil {
		return Config{}, fmt.Errorf("%w: %w", ErrConfig, err)
	}
	return c, nil
}

// Repo is an open repository: the version graph over the chunk layer, which
// runs on blob.NoDelete(o.Blobs): only GC, handed the raw store, deletes.
type Repo struct {
	*vcs.Repo
	Config Config
	chunks *packstore.Store
}

func (o Options) vcs(g Geometry) vcs.Options {
	return vcs.Options{Config: g.Prolly(), Registry: o.Registry, Authorizer: o.Authorizer, Clock: o.Clock}
}

// Init creates a repository in o.Blobs. Writing the config (put-if-absent)
// claims the store; the chunk layer and the version graph follow. An Init
// that stopped after writing the config is finished by the next Init with
// the same key, on the geometry that config records; a store claimed by
// another key, or holding a finished repository, is ErrExists.
func Init(ctx context.Context, p auth.Principal, o Options) (*Repo, error) {
	if err := o.check(); err != nil {
		return nil, err
	}
	g := o.Geometry
	if g == (Geometry{}) {
		g = DefaultGeometry()
	}
	if err := g.validate(); err != nil {
		return nil, err
	}
	if err := auth.Check(ctx, o.Authorizer, p, auth.Admin, "repo"); err != nil {
		return nil, err
	}
	ours, others, err := configs(ctx, o)
	if err != nil {
		return nil, err
	}
	if len(ours) == 0 && others != nil { // claimed by another key, or damaged
		return nil, fmt.Errorf("%w: %w", ErrExists, others)
	}
	var c Config
	if len(ours) > 0 {
		c = ours[0] // an Init that stopped: finish it on the config it left
	} else {
		c = Config{KeyID: o.Keys.ID(), Geometry: g}
		if _, err := rand.Read(c.RepoID[:]); err != nil {
			return nil, err
		}
		b, err := c.seal(o.Keys)
		if err != nil {
			return nil, err
		}
		if err := o.Blobs.Put(ctx, configName(c.RepoID), bytes.NewReader(b), int64(len(b))); err != nil {
			return nil, err
		}
	}
	chunks, err := packstore.Open(ctx, packOptions(o, c))
	if errors.Is(err, packstore.ErrManifest) {
		return nil, fmt.Errorf("%w: another Init landed first", ErrExists) // a root not this config's
	}
	if err != nil {
		return nil, err
	}
	// The claim: the first swap of the root, atomic wherever the root lives.
	v, err := vcs.Init(ctx, chunks, p, o.vcs(c.Geometry))
	if err != nil {
		_ = chunks.Close()
		if errors.Is(err, vcs.ErrExists) {
			return nil, ErrExists
		}
		return nil, err
	}
	return &Repo{Repo: v, Config: c, chunks: chunks}, nil
}

// configName is where an Init writes its config: a name of its own, so no
// two Inits ever write one name.
func configName(id seal.RepoID) string { return configPrefix + hex.EncodeToString(id[:]) }

func packOptions(o Options, c Config) packstore.Options {
	return packstore.Options{Blobs: blob.NoDelete(o.Blobs), Keys: o.Keys, Repo: c.RepoID, PackSize: c.Geometry.PackSize,
		Journal: o.Journal, JournalInterval: o.JournalInterval}
}

// configs reads the store's configs in name order: those that open under
// o.Keys, and the first error among the rest (a damaged one, ErrConfig, or
// another key's, ErrWrongKey).
func configs(ctx context.Context, o Options) (ours []Config, others error, err error) {
	after := ""
	for {
		page, err := o.Blobs.List(ctx, configPrefix, after, blob.MaxListPage)
		if err != nil {
			return nil, nil, err
		}
		for _, info := range page {
			c, err := readConfig(ctx, o, info.Name)
			switch {
			case err == nil:
				ours = append(ours, c)
			case errors.Is(err, ErrConfig) || errors.Is(err, ErrWrongKey):
				if others == nil {
					others = err
				}
			default:
				return nil, nil, err
			}
		}
		if len(page) < blob.MaxListPage {
			return ours, others, nil
		}
		after = page[len(page)-1].Name
	}
}

// current is the repository's config: the one of this key whose repo id
// authenticates the root's manifest (with no root yet, the first).
func current(ctx context.Context, o Options) (Config, error) {
	ours, others, err := configs(ctx, o)
	if err != nil {
		return Config{}, err
	}
	if len(ours) == 0 {
		if others != nil {
			return Config{}, others
		}
		return Config{}, ErrNoRepo
	}
	for _, c := range ours {
		// A probe: it needs the manifest to authenticate, not the journal
		// (a writer beside it may hold that, GC's probe included).
		po := packOptions(o, c)
		po.Journal = packstore.JournalOff
		chunks, err := packstore.Open(ctx, po)
		if errors.Is(err, packstore.ErrManifest) {
			continue // the root is not this config's: another Init's, or a race it lost
		}
		if err != nil {
			return Config{}, err
		}
		_ = chunks.Close()
		return c, nil
	}
	return Config{}, fmt.Errorf("%w: no config of this key authenticates the root", ErrWrongKey)
}

// readConfig reads and opens one config object.
func readConfig(ctx context.Context, o Options, name string) (Config, error) {
	rc, err := o.Blobs.Get(ctx, name, 0, -1)
	if err != nil {
		return Config{}, err
	}
	b, err := io.ReadAll(io.LimitReader(rc, maxConfig+1))
	_ = rc.Close()
	if err != nil {
		return Config{}, err
	}
	if len(b) > maxConfig {
		return Config{}, fmt.Errorf("%w: over %d bytes", ErrConfig, maxConfig)
	}
	return openConfig(b, o.Keys)
}

// Open opens the repository in o.Blobs. A store whose Init stopped before
// it finished holds no repository yet (ErrNoRepo): Init finishes it.
func Open(ctx context.Context, o Options) (*Repo, error) {
	if err := o.check(); err != nil {
		return nil, err
	}
	c, err := current(ctx, o)
	if err != nil {
		return nil, err
	}
	chunks, err := packstore.Open(ctx, packOptions(o, c))
	if err != nil {
		return nil, err
	}
	v, err := vcs.Open(ctx, chunks, o.vcs(c.Geometry))
	if err != nil {
		_ = chunks.Close()
		if errors.Is(err, vcs.ErrNoRepo) {
			return nil, fmt.Errorf("%w: an Init stopped before it finished; Init again to finish it", ErrNoRepo)
		}
		return nil, err
	}
	return &Repo{Repo: v, Config: c, chunks: chunks}, nil
}

// Close releases the repository.
func (r *Repo) Close() error { return r.chunks.Close() }

// Chunks is the repository's chunk store, for writing and reading objects:
// reads and writes, but no root swap, so every ref change goes through the
// version graph. What is written becomes durable when the version graph
// next changes a ref (a commit, a working-set update, a branch); a
// repository closed before then drops it.
//
// It is also a chunk.Preparer and a chunk.Flusher (#40), the write path
// the version graph's own store has: stream.Write (and so model/blob's
// Write) finds them by type assertion and hashes and compresses a long
// stream on every core, and flushes when the stream ends; a host that
// stores chunks itself asserts them the same way.
func (r *Repo) Chunks() chunk.ReadWriter { return readWriter{r.chunks} }

// readWriter narrows a chunk store to reading, writing, preparing and
// flushing: every method but the root's.
type readWriter struct{ s *packstore.Store }

var (
	_ chunk.Preparer = readWriter{}
	_ chunk.Flusher  = readWriter{}
)

func (w readWriter) Get(ctx context.Context, h hash.Hash) ([]byte, error) { return w.s.Get(ctx, h) }
func (w readWriter) Has(ctx context.Context, hs []hash.Hash) (map[hash.Hash]bool, error) {
	return w.s.Has(ctx, hs)
}
func (w readWriter) Put(ctx context.Context, data []byte) (hash.Hash, error) {
	return w.s.Put(ctx, data)
}
func (w readWriter) Prepare(data []byte) (chunk.Prepared, error) { return w.s.Prepare(data) }
func (w readWriter) PutPrepared(ctx context.Context, p chunk.Prepared) (hash.Hash, error) {
	return w.s.PutPrepared(ctx, p)
}
func (w readWriter) Flush(ctx context.Context) error { return w.s.Flush(ctx) }

// Prolly is the map geometry objects are written with.
func (g Geometry) Prolly() prolly.Config {
	return prolly.Config{Nodes: g.Nodes, InlineLimit: g.InlineLimit, Stream: g.Stream()}
}

// Stream is the stream geometry objects are written with.
func (g Geometry) Stream() stream.Config { return stream.Config{CDC: g.CDC, Nodes: g.Nodes} }

// GC collects the repository in o.Blobs (docs/DESIGN.md §9). It is the one
// operation that deletes, so o.Blobs must be the raw store (the GC role),
// not the NoDelete one a repository runs on; it needs admin, and a model
// registry whose every model walks.
func GC(ctx context.Context, p auth.Principal, o Options, grace time.Duration) (gc.Report, error) {
	if err := o.check(); err != nil {
		return gc.Report{}, err
	}
	if err := auth.Check(ctx, o.Authorizer, p, auth.Admin, "repo"); err != nil {
		return gc.Report{}, err
	}
	c, err := current(ctx, o)
	if err != nil {
		return gc.Report{}, err
	}
	return gc.Run(ctx, gc.Options{Blobs: o.Blobs, Keys: o.Keys, Repo: c.RepoID, Config: c.Geometry.Prolly(),
		Registry: o.Registry, Grace: grace, Clock: o.Clock})
}
