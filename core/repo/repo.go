// Package repo is the entry point: a repository on a BlobStore, created or
// opened with a master key, a model registry and an authorizer. Init writes
// the sealed config object (docs/DESIGN.md §8) that says how everything else
// is written; Open reads it before anything else.
package repo

import (
	"context"
	"errors"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/auth"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/boundary"
	"github.com/SmithOperatingSolutions/snapshot-core/core/cdc"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
	"github.com/SmithOperatingSolutions/snapshot-core/core/vcs"
)

// Errors.
var (
	ErrExists   = errors.New("repo: the store already holds a repository")
	ErrNoRepo   = errors.New("repo: the store holds no repository")
	ErrWrongKey = errors.New("repo: this key did not create the repository")
	ErrConfig   = errors.New("repo: the config does not authenticate or decode")
)

// Geometry is how a repository writes: fixed at Init, read back by Open.
type Geometry struct {
	CDC         cdc.Geometry
	Nodes       boundary.Geometry
	InlineLimit int
	PackSize    int
}

// DefaultGeometry is the default repo geometry.
func DefaultGeometry() Geometry { return Geometry{} }

// Options configures Init and Open.
type Options struct {
	Blobs      blob.BlobStore
	Keys       *seal.Keyring
	Registry   *model.Registry
	Authorizer auth.Authorizer  // nil denies everything
	Clock      func() time.Time // nil: time.Now
	Geometry   Geometry         // Init only; the zero Geometry is DefaultGeometry
}

// Config is what the config object records.
type Config struct {
	RepoID   seal.RepoID
	KeyID    seal.KeyID
	Geometry Geometry
}

// Repo is an open repository: the version graph over the chunk layer.
type Repo struct {
	*vcs.Repo
	Config Config
}

// Init creates a repository in o.Blobs.
func Init(ctx context.Context, p auth.Principal, o Options) (*Repo, error) {
	return nil, errors.New("repo: not yet")
}

// Open opens the repository in o.Blobs.
func Open(ctx context.Context, o Options) (*Repo, error) { return nil, errors.New("repo: not yet") }

// Close releases the repository.
func (r *Repo) Close() error { return nil }
