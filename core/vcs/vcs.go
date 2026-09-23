// Package vcs is the version graph (docs/DESIGN.md §8; Engine Spec L2):
// commits naming namespaces, branches and tags naming commits, and a working
// set per branch of uncommitted changes. All of it hangs from the chunk
// store's root, the refs map, and every change to it is one
// CompareAndSetRoot: a writer that loses re-reads and re-applies, and never
// overwrites blindly. Every call takes a Principal and asks the Authorizer.
package vcs

import (
	"context"
	"errors"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/auth"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/merge"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/object"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
)

// Limits and names.
const (
	MainBranch    = "main"
	MaxLog        = 10_000
	MaxMessageLen = 64 << 10
)

// Errors.
var (
	ErrExists              = errors.New("vcs: the store already holds a repository")
	ErrNoRepo              = errors.New("vcs: the store holds no repository")
	ErrConflict            = errors.New("vcs: the working set changed since it was read")
	ErrBranchExists        = errors.New("vcs: branch exists")
	ErrBranchNotFound      = errors.New("vcs: no such branch")
	ErrBranchInUse         = errors.New("vcs: branch is checked out")
	ErrTagExists           = errors.New("vcs: tag exists")
	ErrInvalidName         = errors.New("vcs: invalid branch or tag name")
	ErrInvalidLimit        = errors.New("vcs: log limit must be 1 to 10,000")
	ErrUnresolvedConflicts = errors.New("vcs: unresolved merge conflicts")
)

// Options configures a repository.
type Options struct {
	Config       prolly.Config
	Registry     *model.Registry
	Authorizer   auth.Authorizer  // nil denies everything
	Clock        func() time.Time // nil: time.Now
	MaxConflicts int              // 0: merge.DefaultMaxConflicts
}

// Commit is one commit. Hash is its chunk's hash (not encoded).
type Commit struct {
	Hash      hash.Hash
	Parents   []hash.Hash // 0–2; the first is the branch merged into
	Namespace hash.Hash
	Height    uint64
	Time      time.Time
	Author    string
	Message   string
}

// Encode is the DESIGN §8 commit chunk.
func (c Commit) Encode() []byte { return nil }

// DecodeCommit parses a commit chunk (chunk.ErrCorrupt when malformed).
func DecodeCommit(b []byte) (Commit, error) { return Commit{}, nil }

// Tag is one tag.
type Tag struct {
	Hash    hash.Hash
	Target  hash.Hash
	Time    time.Time
	Tagger  string
	Message string
}

// MergeState is a merge in progress: which commit is being merged, from
// which base, and the root of its conflicts map.
type MergeState struct {
	Base, Theirs hash.Hash
	Conflicts    hash.Hash
}

// WorkingSet is a branch's uncommitted state. Hash is its chunk's hash.
type WorkingSet struct {
	Hash            hash.Hash
	Working, Staged hash.Hash
	Merge           *MergeState
}

// Repo is a repository over a chunk store.
type Repo struct{}

// Init creates a repository with branch main at an empty initial commit.
func Init(ctx context.Context, s chunk.Store, p auth.Principal, o Options) (*Repo, error) {
	return &Repo{}, nil
}

// Open opens the repository in s.
func Open(ctx context.Context, s chunk.Store, o Options) (*Repo, error) { return &Repo{}, nil }

// Namespace opens a namespace of this repository.
func (r *Repo) Namespace(ctx context.Context, root hash.Hash) (*object.Namespace, error) {
	return nil, errors.New("vcs: not yet")
}

// Head returns the commit a branch names.
func (r *Repo) Head(ctx context.Context, p auth.Principal, branch string) (Commit, error) {
	return Commit{}, nil
}

// Branches lists the branches.
func (r *Repo) Branches(ctx context.Context, p auth.Principal) ([]string, error) { return nil, nil }

// ReadCommit returns a commit.
func (r *Repo) ReadCommit(ctx context.Context, p auth.Principal, h hash.Hash) (Commit, error) {
	return Commit{}, nil
}

// WorkingSet returns a branch's working set.
func (r *Repo) WorkingSet(ctx context.Context, p auth.Principal, branch string) (WorkingSet, error) {
	return WorkingSet{}, nil
}

// UpdateWorkingSet replaces a branch's working set with next if it is still
// prev (ErrConflict otherwise), and returns next as stored.
func (r *Repo) UpdateWorkingSet(ctx context.Context, p auth.Principal, branch string, prev, next WorkingSet) (WorkingSet, error) {
	return WorkingSet{}, nil
}

// CommitWorkingSet commits what is staged on a branch, onto its head as it
// is when the commit lands; a merge in progress adds its second parent.
func (r *Repo) CommitWorkingSet(ctx context.Context, p auth.Principal, branch, message string) (Commit, error) {
	return Commit{}, nil
}

// CreateBranch names a new branch at a commit.
func (r *Repo) CreateBranch(ctx context.Context, p auth.Principal, name string, at hash.Hash) error {
	return nil
}

// DeleteBranch removes a branch no session has checked out.
func (r *Repo) DeleteBranch(ctx context.Context, p auth.Principal, name string) error { return nil }

// Session is a branch checked out by one session, in this process.
type Session struct{}

// Checkout marks a branch as checked out until the session closes.
func (r *Repo) Checkout(ctx context.Context, p auth.Principal, branch string) (*Session, error) {
	return &Session{}, nil
}

// Close ends a session.
func (s *Session) Close() {}

// CreateTag names a commit.
func (r *Repo) CreateTag(ctx context.Context, p auth.Principal, name string, target hash.Hash, message string) (Tag, error) {
	return Tag{}, nil
}

// Log returns up to limit commits reachable from from, highest first.
func (r *Repo) Log(ctx context.Context, p auth.Principal, from hash.Hash, limit int) ([]Commit, error) {
	return nil, nil
}

// MergeBase returns the best common ancestor of two commits.
func (r *Repo) MergeBase(ctx context.Context, p auth.Principal, a, b hash.Hash) (hash.Hash, error) {
	return hash.Hash{}, nil
}

// Merge merges a commit into a branch's working set, recording conflicts
// there; a failed merge leaves the working set as it was.
func (r *Repo) Merge(ctx context.Context, p auth.Principal, branch string, theirs hash.Hash) (merge.Result, error) {
	return merge.Result{}, nil
}

// Conflicts lists a branch's unresolved merge conflicts.
func (r *Repo) Conflicts(ctx context.Context, p auth.Principal, branch string) ([]merge.Conflict, error) {
	return nil, nil
}

// ResolveConflict settles a conflicting path (nil deletes it) in the working
// and staged namespaces, and drops its conflict.
func (r *Repo) ResolveConflict(ctx context.Context, p auth.Principal, branch, path string, to *object.Ref) error {
	return nil
}
