// Package repo is the entry point: a repository on a BlobStore, created or
// opened with a master key, a model registry and an authorizer. Init writes
// the sealed config object (docs/DESIGN.md §8) that says how everything else
// is written; Open reads it before anything else.
package repo

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/auth"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/boundary"
	"github.com/SmithOperatingSolutions/snapshot-core/core/cdc"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/packstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/internal/wire"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/pack"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
	"github.com/SmithOperatingSolutions/snapshot-core/core/stream"
	"github.com/SmithOperatingSolutions/snapshot-core/core/vcs"
)

// Errors.
var (
	ErrExists   = errors.New("repo: the store already holds a repository")
	ErrNoRepo   = errors.New("repo: the store holds no repository")
	ErrWrongKey = errors.New("repo: this key did not create the repository")
	ErrConfig   = errors.New("repo: the config does not authenticate or decode")
)

// The config object: "SCRC" · version u16 · repo id [16] · key id [32] ·
// salt [32] · seal(Config, ctx = header, "SCRP" · version u16 · cdc min u32 ·
// cdc max u32 · cdc mask u64 · node min u32 · node target u32 · node max u32 ·
// inline limit u32 · pack size u32).
const (
	configName  = "config"
	configMagic = "SCRC"
	plainMagic  = "SCRP"
	configV1    = 1
	headerLen   = 4 + 2 + 16 + 32 + 32
	maxConfig   = 4 << 10
	maxInline   = 512 << 10
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

func (g Geometry) prolly() prolly.Config {
	return prolly.Config{Nodes: g.Nodes, InlineLimit: g.InlineLimit, Stream: stream.Config{CDC: g.CDC, Nodes: g.Nodes}}
}

// Options configures Init and Open.
type Options struct {
	Blobs      blob.BlobStore
	Keys       *seal.Keyring
	Registry   *model.Registry
	Authorizer auth.Authorizer  // nil denies everything
	Clock      func() time.Time // nil: time.Now
	Geometry   Geometry         // Init only; the zero Geometry is DefaultGeometry
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

// Repo is an open repository: the version graph over the chunk layer.
type Repo struct {
	*vcs.Repo
	Config Config
	chunks *packstore.Store
}

func (o Options) vcs(g Geometry) vcs.Options {
	return vcs.Options{Config: g.prolly(), Registry: o.Registry, Authorizer: o.Authorizer, Clock: o.Clock}
}

// Init creates a repository in o.Blobs. Writing the config (put-if-absent)
// claims the store; the chunk layer and the version graph follow.
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
	c := Config{KeyID: o.Keys.ID(), Geometry: g}
	if _, err := rand.Read(c.RepoID[:]); err != nil {
		return nil, err
	}
	b, err := c.seal(o.Keys)
	if err != nil {
		return nil, err
	}
	if err := o.Blobs.Put(ctx, configName, bytes.NewReader(b), int64(len(b))); err != nil {
		if errors.Is(err, blob.ErrExists) {
			return nil, ErrExists
		}
		return nil, err
	}
	chunks, err := packstore.Open(ctx, packstore.Options{Blobs: o.Blobs, Keys: o.Keys, Repo: c.RepoID, PackSize: g.PackSize})
	if err != nil {
		return nil, err
	}
	v, err := vcs.Init(ctx, chunks, p, o.vcs(g))
	if err != nil {
		_ = chunks.Close()
		return nil, err
	}
	return &Repo{Repo: v, Config: c, chunks: chunks}, nil
}

// Open opens the repository in o.Blobs.
func Open(ctx context.Context, o Options) (*Repo, error) {
	if err := o.check(); err != nil {
		return nil, err
	}
	rc, err := o.Blobs.Get(ctx, configName, 0, -1)
	if errors.Is(err, blob.ErrNotFound) {
		return nil, ErrNoRepo
	}
	if err != nil {
		return nil, err
	}
	b, err := io.ReadAll(io.LimitReader(rc, maxConfig+1))
	_ = rc.Close()
	if err != nil {
		return nil, err
	}
	if len(b) > maxConfig {
		return nil, fmt.Errorf("%w: over %d bytes", ErrConfig, maxConfig)
	}
	c, err := openConfig(b, o.Keys)
	if err != nil {
		return nil, err
	}
	chunks, err := packstore.Open(ctx, packstore.Options{Blobs: o.Blobs, Keys: o.Keys, Repo: c.RepoID, PackSize: c.Geometry.PackSize})
	if err != nil {
		return nil, err
	}
	v, err := vcs.Open(ctx, chunks, o.vcs(c.Geometry))
	if err != nil {
		_ = chunks.Close()
		return nil, err
	}
	return &Repo{Repo: v, Config: c, chunks: chunks}, nil
}

// Close releases the repository.
func (r *Repo) Close() error { return r.chunks.Close() }
