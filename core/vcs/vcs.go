// Package vcs is the version graph (docs/DESIGN.md §8; Engine Spec L2):
// commits naming namespaces, branches and tags naming commits, and a working
// set per branch of uncommitted changes. All of it hangs from the chunk
// store's root, the refs map, and every change to it is one
// CompareAndSetRoot: a writer that loses re-reads and re-applies, and never
// overwrites blindly. Every call takes a Principal and asks the Authorizer,
// except Namespace, which opens what a hash names: a host holding the hash
// holds the chunk store it came from, so a check there would guard nothing.
package vcs

import (
	"bytes"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
	"unicode/utf8"

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
	maxAttempts   = 1000 // root swaps lost to other writers before giving up
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
	ErrTagNotFound         = errors.New("vcs: no such tag")
	ErrInvalidName         = errors.New("vcs: invalid branch or tag name")
	ErrInvalidLimit        = errors.New("vcs: log limit must be 1 to 10,000")
	ErrUnresolvedConflicts = errors.New("vcs: unresolved merge conflicts")
	ErrMergeState          = errors.New("vcs: only merging, resolving, committing and abandoning change a merge in progress")
	ErrNoMerge             = errors.New("vcs: no merge in progress")
	// ErrSessionLost is the chunk store's: GC deleted writes the repository
	// had not published, and the host must reopen it and write again.
	ErrSessionLost = chunk.ErrSessionLost
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
	// PreWorking and PreStaged are the namespaces the merge started from,
	// which AbortMerge puts back.
	PreWorking, PreStaged hash.Hash
}

// WorkingSet is a branch's uncommitted state. Hash is its chunk's hash.
type WorkingSet struct {
	Hash            hash.Hash
	Working, Staged hash.Hash
	Merge           *MergeState
}

// Repo is a repository over a chunk store.
type Repo struct {
	s chunk.Store
	o Options

	mu         sync.Mutex
	checkedOut map[string]int
}

func newRepo(s chunk.Store, o Options) (*Repo, error) {
	if o.Registry == nil {
		return nil, errors.New("vcs: a repository needs a model registry")
	}
	if o.Clock == nil {
		o.Clock = time.Now
	}
	return &Repo{s: s, o: o, checkedOut: map[string]int{}}, nil
}

func headKey(b string) []byte { return []byte("heads/" + b) }
func workKey(b string) []byte { return []byte("work/" + b) }
func tagKey(t string) []byte  { return []byte("tags/" + t) }

// Init creates a repository with branch main at an empty initial commit.
func Init(ctx context.Context, s chunk.Store, p auth.Principal, o Options) (*Repo, error) {
	r, err := newRepo(s, o)
	if err != nil {
		return nil, err
	}
	if err := auth.Check(ctx, o.Authorizer, p, auth.Admin, "repo"); err != nil {
		return nil, err
	}
	if root, err := s.Root(ctx); err != nil {
		return nil, err
	} else if !root.IsZero() {
		return nil, ErrExists
	}
	ns, err := object.New(ctx, s, o.Config, o.Registry)
	if err != nil {
		return nil, err
	}
	c := Commit{Namespace: ns.Root(), Time: r.o.Clock().UTC(), Author: p.ID, Message: "Initialize the repository"}
	commit, err := s.Put(ctx, c.Encode())
	if err != nil {
		return nil, err
	}
	ws, err := s.Put(ctx, WorkingSet{Working: ns.Root(), Staged: ns.Root()}.encode())
	if err != nil {
		return nil, err
	}
	refs, err := prolly.Empty(ctx, s, o.Config)
	if err != nil {
		return nil, err
	}
	e := refs.Editor()
	if err := e.Put(headKey(MainBranch), commit[:]); err != nil {
		return nil, err
	}
	if err := e.Put(workKey(MainBranch), ws[:]); err != nil {
		return nil, err
	}
	if refs, err = e.Flush(ctx); err != nil {
		return nil, err
	}
	if err := s.CompareAndSetRoot(ctx, hash.Hash{}, refs.Root()); err != nil {
		if errors.Is(err, chunk.ErrRootConflict) {
			return nil, ErrExists
		}
		return nil, err
	}
	return r, nil
}

// Open opens the repository in s.
func Open(ctx context.Context, s chunk.Store, o Options) (*Repo, error) {
	r, err := newRepo(s, o)
	if err != nil {
		return nil, err
	}
	if _, err := r.refs(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

// refs opens the refs map at the store's current root.
func (r *Repo) refs(ctx context.Context) (*prolly.Map, error) {
	root, err := r.s.Root(ctx)
	if err != nil {
		return nil, err
	}
	if root.IsZero() {
		return nil, ErrNoRepo
	}
	return prolly.Open(ctx, r.s, r.o.Config, root)
}

func ref(ctx context.Context, m *prolly.Map, key []byte) (hash.Hash, bool, error) {
	v, ok, err := m.Get(ctx, key)
	if err != nil || !ok {
		return hash.Hash{}, false, err
	}
	if len(v) != hash.Size {
		return hash.Hash{}, false, corrupt("ref %s holds %d bytes", key, len(v))
	}
	return hash.Hash(v), true, nil
}

// update applies fn to the refs map and swaps the root, re-reading and
// re-applying whenever another writer swapped first. fn's own errors end it.
func (r *Repo) update(ctx context.Context, fn func(m *prolly.Map, e *prolly.Editor) error) error {
	for attempt := 0; attempt < maxAttempts; attempt++ {
		m, err := r.refs(ctx)
		if err != nil {
			return err
		}
		e := m.Editor()
		if err := fn(m, e); err != nil {
			return err
		}
		next, err := e.Flush(ctx)
		if err != nil {
			return err
		}
		err = r.s.CompareAndSetRoot(ctx, m.Root(), next.Root())
		if !errors.Is(err, chunk.ErrRootConflict) {
			return err
		}
	}
	return fmt.Errorf("vcs: the refs changed under %d attempts in a row", maxAttempts)
}

// Namespace opens a namespace of this repository.
func (r *Repo) Namespace(ctx context.Context, root hash.Hash) (*object.Namespace, error) {
	return object.Open(ctx, r.s, r.o.Config, r.o.Registry, root)
}

func (r *Repo) check(ctx context.Context, p auth.Principal, a auth.Action, resource string) error {
	return auth.Check(ctx, r.o.Authorizer, p, a, resource)
}

func (r *Repo) branchCheck(ctx context.Context, p auth.Principal, a auth.Action, branch string) error {
	if err := r.check(ctx, p, a, "branch:"+branch); err != nil {
		return err
	}
	return validName(branch)
}

// pathResource names one path of a branch to the Authorizer; a branch name
// cannot hold a colon, so the two never run together.
func pathResource(branch, path string) string { return "path:" + branch + ":" + path }

// checkPaths asks for write on every path that differs between two
// namespaces of a branch (the Storage Core Spec: "per branch and per path
// prefix").
func (r *Repo) checkPaths(ctx context.Context, p auth.Principal, branch string, from, to hash.Hash) error {
	if from == to {
		return nil
	}
	f, err := r.Namespace(ctx, from)
	if err != nil {
		return err
	}
	t, err := r.Namespace(ctx, to)
	if err != nil {
		return err
	}
	d, err := object.Diff(ctx, f, t)
	if err != nil {
		return err
	}
	for {
		c, ok, err := d.Next()
		if err != nil || !ok {
			return err
		}
		if err := r.check(ctx, p, auth.Write, pathResource(branch, c.Path)); err != nil {
			return err
		}
	}
}

func (r *Repo) readCommit(ctx context.Context, h hash.Hash) (Commit, error) {
	b, err := r.s.Get(ctx, h)
	if err != nil {
		return Commit{}, err
	}
	c, err := DecodeCommit(b)
	if err != nil {
		return Commit{}, err
	}
	c.Hash = h
	return c, nil
}

func (r *Repo) readWorkingSet(ctx context.Context, h hash.Hash) (WorkingSet, error) {
	b, err := r.s.Get(ctx, h)
	if err != nil {
		return WorkingSet{}, err
	}
	ws, err := decodeWorkingSet(b)
	if err != nil {
		return WorkingSet{}, err
	}
	ws.Hash = h
	return ws, nil
}

// branchRefs reads a branch's head and working set hashes from m.
func branchRefs(ctx context.Context, m *prolly.Map, branch string) (head, work hash.Hash, err error) {
	head, ok, err := ref(ctx, m, headKey(branch))
	if err != nil {
		return head, work, err
	}
	if !ok {
		return head, work, fmt.Errorf("%w: %s", ErrBranchNotFound, branch)
	}
	work, ok, err = ref(ctx, m, workKey(branch))
	if err == nil && !ok {
		err = corrupt("branch %s has no working set", branch)
	}
	return head, work, err
}

// Head returns the commit a branch names.
func (r *Repo) Head(ctx context.Context, p auth.Principal, branch string) (Commit, error) {
	if err := r.branchCheck(ctx, p, auth.Read, branch); err != nil {
		return Commit{}, err
	}
	m, err := r.refs(ctx)
	if err != nil {
		return Commit{}, err
	}
	head, _, err := branchRefs(ctx, m, branch)
	if err != nil {
		return Commit{}, err
	}
	return r.readCommit(ctx, head)
}

// Branches lists the branches.
func (r *Repo) Branches(ctx context.Context, p auth.Principal) ([]string, error) {
	if err := r.check(ctx, p, auth.Read, "repo"); err != nil {
		return nil, err
	}
	return r.names(ctx, "heads/")
}

// names lists the refs under prefix ("heads/" or "tags/") in order, without
// the prefix.
func (r *Repo) names(ctx context.Context, prefix string) ([]string, error) {
	m, err := r.refs(ctx)
	if err != nil {
		return nil, err
	}
	end := prefix[:len(prefix)-1] + "0" // '0' follows '/'
	it, err := m.IterRange(ctx, []byte(prefix), []byte(end))
	if err != nil {
		return nil, err
	}
	var out []string
	for {
		k, _, ok, err := it.Next()
		if err != nil || !ok {
			return out, err
		}
		out = append(out, string(bytes.TrimPrefix(k, []byte(prefix))))
	}
}

// ReadCommit returns a commit.
func (r *Repo) ReadCommit(ctx context.Context, p auth.Principal, h hash.Hash) (Commit, error) {
	if err := r.check(ctx, p, auth.Read, "repo"); err != nil {
		return Commit{}, err
	}
	return r.readCommit(ctx, h)
}

// WorkingSet returns a branch's working set.
func (r *Repo) WorkingSet(ctx context.Context, p auth.Principal, branch string) (WorkingSet, error) {
	if err := r.branchCheck(ctx, p, auth.Read, branch); err != nil {
		return WorkingSet{}, err
	}
	m, err := r.refs(ctx)
	if err != nil {
		return WorkingSet{}, err
	}
	_, work, err := branchRefs(ctx, m, branch)
	if err != nil {
		return WorkingSet{}, err
	}
	return r.readWorkingSet(ctx, work)
}

// UpdateWorkingSet replaces a branch's working set with next if it is still
// prev (ErrConflict otherwise), and returns next as stored. It changes the
// namespaces only: next carries the merge in progress as stored
// (ErrMergeState otherwise), which only merging, resolving, committing and
// abandoning change.
func (r *Repo) UpdateWorkingSet(ctx context.Context, p auth.Principal, branch string, prev, next WorkingSet) (WorkingSet, error) {
	if err := r.branchCheck(ctx, p, auth.Write, branch); err != nil {
		return WorkingSet{}, err
	}
	// The paths changed are checked against the working set stored under
	// prev.Hash, never prev's own fields; the swap below lands only if that
	// is still the branch's working set.
	stored, err := r.readWorkingSet(ctx, prev.Hash)
	if err != nil {
		return WorkingSet{}, err
	}
	for _, ns := range [][2]hash.Hash{{stored.Working, next.Working}, {stored.Staged, next.Staged}} {
		if err := r.checkPaths(ctx, p, branch, ns[0], ns[1]); err != nil {
			return WorkingSet{}, err
		}
	}
	return r.setWorkingSet(ctx, branch, prev, next, true)
}

func sameMerge(a, b *MergeState) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}

// setWorkingSet is UpdateWorkingSet, and with keepMerge false it may
// change the merge state too.
func (r *Repo) setWorkingSet(ctx context.Context, branch string, prev, next WorkingSet, keepMerge bool) (WorkingSet, error) {
	h, err := r.s.Put(ctx, next.encode())
	if err != nil {
		return WorkingSet{}, err
	}
	err = r.update(ctx, func(m *prolly.Map, e *prolly.Editor) error {
		_, work, err := branchRefs(ctx, m, branch)
		if err != nil {
			return err
		}
		if work != prev.Hash {
			return fmt.Errorf("%w: branch %s", ErrConflict, branch)
		}
		if keepMerge {
			stored, err := r.readWorkingSet(ctx, work)
			if err != nil {
				return err
			}
			if !sameMerge(stored.Merge, next.Merge) {
				return fmt.Errorf("%w: branch %s", ErrMergeState, branch)
			}
		}
		return e.Put(workKey(branch), h[:])
	})
	if err != nil {
		return WorkingSet{}, err
	}
	next.Hash = h
	return next, nil
}

func (r *Repo) conflictCount(ctx context.Context, ws WorkingSet) (uint64, error) {
	if ws.Merge == nil {
		return 0, nil
	}
	m, err := prolly.Open(ctx, r.s, r.o.Config, ws.Merge.Conflicts)
	if err != nil {
		return 0, err
	}
	return m.Count(), nil
}

// CommitWorkingSet commits what is staged on a branch, onto its head as it
// is when the commit lands; a merge in progress adds its second parent.
func (r *Repo) CommitWorkingSet(ctx context.Context, p auth.Principal, branch, message string) (Commit, error) {
	if err := r.branchCheck(ctx, p, auth.Commit, branch); err != nil {
		return Commit{}, err
	}
	if len(message) > MaxMessageLen || !utf8.ValidString(message) {
		return Commit{}, fmt.Errorf("vcs: a commit message must be UTF-8 of at most %d bytes", MaxMessageLen)
	}
	var out Commit
	err := r.update(ctx, func(m *prolly.Map, e *prolly.Editor) error {
		headHash, work, err := branchRefs(ctx, m, branch)
		if err != nil {
			return err
		}
		ws, err := r.readWorkingSet(ctx, work)
		if err != nil {
			return err
		}
		if n, err := r.conflictCount(ctx, ws); err != nil {
			return err
		} else if n > 0 {
			return fmt.Errorf("%w: %d on %s", ErrUnresolvedConflicts, n, branch)
		}
		head, err := r.readCommit(ctx, headHash)
		if err != nil {
			return err
		}
		if err := r.checkPaths(ctx, p, branch, head.Namespace, ws.Staged); err != nil {
			return err
		}
		c := Commit{Parents: []hash.Hash{head.Hash}, Namespace: ws.Staged, Height: head.Height + 1,
			Time: r.o.Clock().UTC(), Author: p.ID, Message: message}
		if ws.Merge != nil {
			theirs, err := r.readCommit(ctx, ws.Merge.Theirs)
			if err != nil {
				return err
			}
			c.Parents = append(c.Parents, theirs.Hash)
			c.Height = max(head.Height, theirs.Height) + 1
		}
		if c.Hash, err = r.s.Put(ctx, c.Encode()); err != nil {
			return err
		}
		nextWS, err := r.s.Put(ctx, WorkingSet{Working: ws.Working, Staged: ws.Staged}.encode())
		if err != nil {
			return err
		}
		out = c
		if err := e.Put(headKey(branch), c.Hash[:]); err != nil {
			return err
		}
		return e.Put(workKey(branch), nextWS[:])
	})
	return out, err
}

// Commit replaces a branch's working and staged namespaces with namespace
// and commits it onto the branch's head, in one publish: the working set
// must still be prev (ErrConflict otherwise) and carry the merge in
// progress as stored, whose conflicts must be resolved; a merge adds its
// second parent. It is UpdateWorkingSet then CommitWorkingSet, at one
// swap's cost (#10).
func (r *Repo) Commit(ctx context.Context, p auth.Principal, branch string, prev WorkingSet, namespace hash.Hash, message string) (Commit, error) {
	if err := r.branchCheck(ctx, p, auth.Write, branch); err != nil {
		return Commit{}, err
	}
	if err := r.check(ctx, p, auth.Commit, "branch:"+branch); err != nil { // it writes the working set and commits it
		return Commit{}, err
	}
	if len(message) > MaxMessageLen || !utf8.ValidString(message) {
		return Commit{}, fmt.Errorf("vcs: a commit message must be UTF-8 of at most %d bytes", MaxMessageLen)
	}
	var out Commit
	err := r.update(ctx, func(m *prolly.Map, e *prolly.Editor) error {
		headHash, work, err := branchRefs(ctx, m, branch)
		if err != nil {
			return err
		}
		if work != prev.Hash {
			return fmt.Errorf("%w: branch %s", ErrConflict, branch)
		}
		stored, err := r.readWorkingSet(ctx, work)
		if err != nil {
			return err
		}
		if !sameMerge(stored.Merge, prev.Merge) {
			return fmt.Errorf("%w: branch %s", ErrMergeState, branch)
		}
		if n, err := r.conflictCount(ctx, stored); err != nil {
			return err
		} else if n > 0 {
			return fmt.Errorf("%w: %d on %s", ErrUnresolvedConflicts, n, branch)
		}
		head, err := r.readCommit(ctx, headHash)
		if err != nil {
			return err
		}
		// The paths the working set changes, and the paths the commit changes.
		for _, from := range []hash.Hash{stored.Working, stored.Staged, head.Namespace} {
			if err := r.checkPaths(ctx, p, branch, from, namespace); err != nil {
				return err
			}
		}
		c := Commit{Parents: []hash.Hash{head.Hash}, Namespace: namespace, Height: head.Height + 1,
			Time: r.o.Clock().UTC(), Author: p.ID, Message: message}
		if stored.Merge != nil {
			theirs, err := r.readCommit(ctx, stored.Merge.Theirs)
			if err != nil {
				return err
			}
			c.Parents = append(c.Parents, theirs.Hash)
			c.Height = max(head.Height, theirs.Height) + 1
		}
		if c.Hash, err = r.s.Put(ctx, c.Encode()); err != nil {
			return err
		}
		nextWS, err := r.s.Put(ctx, WorkingSet{Working: namespace, Staged: namespace}.encode())
		if err != nil {
			return err
		}
		out = c
		if err := e.Put(headKey(branch), c.Hash[:]); err != nil {
			return err
		}
		return e.Put(workKey(branch), nextWS[:])
	})
	return out, err
}

// CreateBranch names a new branch at a commit.
func (r *Repo) CreateBranch(ctx context.Context, p auth.Principal, name string, at hash.Hash) error {
	if err := r.branchCheck(ctx, p, auth.Manage, name); err != nil {
		return err
	}
	c, err := r.readCommit(ctx, at)
	if err != nil {
		return err
	}
	ws, err := r.s.Put(ctx, WorkingSet{Working: c.Namespace, Staged: c.Namespace}.encode())
	if err != nil {
		return err
	}
	return r.update(ctx, func(m *prolly.Map, e *prolly.Editor) error {
		if _, ok, err := ref(ctx, m, headKey(name)); err != nil {
			return err
		} else if ok {
			return fmt.Errorf("%w: %s", ErrBranchExists, name)
		}
		if err := e.Put(headKey(name), at[:]); err != nil {
			return err
		}
		return e.Put(workKey(name), ws[:])
	})
}

// DeleteBranch removes a branch no session has checked out.
func (r *Repo) DeleteBranch(ctx context.Context, p auth.Principal, name string) error {
	if err := r.branchCheck(ctx, p, auth.Manage, name); err != nil {
		return err
	}
	r.mu.Lock()
	inUse := r.checkedOut[name] > 0
	r.mu.Unlock()
	if inUse {
		return fmt.Errorf("%w: %s", ErrBranchInUse, name)
	}
	return r.update(ctx, func(m *prolly.Map, e *prolly.Editor) error {
		if _, _, err := branchRefs(ctx, m, name); err != nil {
			return err
		}
		if err := e.Delete(headKey(name)); err != nil {
			return err
		}
		return e.Delete(workKey(name))
	})
}

// Session is a branch checked out by one session, in this process.
type Session struct {
	r      *Repo
	branch string
	once   sync.Once
}

// Checkout marks a branch as checked out until the session closes.
func (r *Repo) Checkout(ctx context.Context, p auth.Principal, branch string) (*Session, error) {
	if _, err := r.Head(ctx, p, branch); err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.checkedOut[branch]++
	r.mu.Unlock()
	return &Session{r: r, branch: branch}, nil
}

// Close ends a session.
func (s *Session) Close() {
	s.once.Do(func() {
		s.r.mu.Lock()
		defer s.r.mu.Unlock()
		if s.r.checkedOut[s.branch]--; s.r.checkedOut[s.branch] <= 0 {
			delete(s.r.checkedOut, s.branch)
		}
	})
}

// CreateTag names a commit.
func (r *Repo) CreateTag(ctx context.Context, p auth.Principal, name string, target hash.Hash, message string) (Tag, error) {
	if err := r.check(ctx, p, auth.Manage, "tag:"+name); err != nil {
		return Tag{}, err
	}
	if err := validName(name); err != nil {
		return Tag{}, err
	}
	if len(message) > MaxMessageLen || !utf8.ValidString(message) {
		return Tag{}, fmt.Errorf("vcs: a tag message must be UTF-8 of at most %d bytes", MaxMessageLen)
	}
	if _, err := r.readCommit(ctx, target); err != nil {
		return Tag{}, err
	}
	t := Tag{Target: target, Time: r.o.Clock().UTC(), Tagger: p.ID, Message: message}
	h, err := r.s.Put(ctx, t.encode())
	if err != nil {
		return Tag{}, err
	}
	t.Hash = h
	err = r.update(ctx, func(m *prolly.Map, e *prolly.Editor) error {
		if _, ok, err := ref(ctx, m, tagKey(name)); err != nil {
			return err
		} else if ok {
			return fmt.Errorf("%w: %s", ErrTagExists, name)
		}
		return e.Put(tagKey(name), h[:])
	})
	return t, err
}

// Tags lists the tags' names in order.
func (r *Repo) Tags(ctx context.Context, p auth.Principal) ([]string, error) {
	if err := r.check(ctx, p, auth.Read, "repo"); err != nil {
		return nil, err
	}
	return r.names(ctx, "tags/")
}

// Tag returns the tag a name holds (ErrTagNotFound when there is none).
func (r *Repo) Tag(ctx context.Context, p auth.Principal, name string) (Tag, error) {
	if err := r.check(ctx, p, auth.Read, "tag:"+name); err != nil {
		return Tag{}, err
	}
	if err := validName(name); err != nil {
		return Tag{}, err
	}
	m, err := r.refs(ctx)
	if err != nil {
		return Tag{}, err
	}
	h, ok, err := ref(ctx, m, tagKey(name))
	if err != nil {
		return Tag{}, err
	}
	if !ok {
		return Tag{}, fmt.Errorf("%w: %s", ErrTagNotFound, name)
	}
	return r.readTag(ctx, h)
}

func (r *Repo) readTag(ctx context.Context, h hash.Hash) (Tag, error) {
	b, err := r.s.Get(ctx, h)
	if err != nil {
		return Tag{}, err
	}
	t, err := decodeTag(b)
	if err != nil {
		return Tag{}, err
	}
	t.Hash = h
	return t, nil
}

// DeleteTag removes a tag, freeing its name. The commit it named stays while
// anything else reaches it; what only the tag reached, GC collects.
func (r *Repo) DeleteTag(ctx context.Context, p auth.Principal, name string) error {
	if err := r.check(ctx, p, auth.Manage, "tag:"+name); err != nil {
		return err
	}
	if err := validName(name); err != nil {
		return err
	}
	return r.update(ctx, func(m *prolly.Map, e *prolly.Editor) error {
		if _, ok, err := ref(ctx, m, tagKey(name)); err != nil {
			return err
		} else if !ok {
			return fmt.Errorf("%w: %s", ErrTagNotFound, name)
		}
		return e.Delete(tagKey(name))
	})
}

// byHeight is a max-heap of commits by height, lower hash first on a tie.
type byHeight []Commit

func (h byHeight) Len() int { return len(h) }
func (h byHeight) Less(i, j int) bool {
	if h[i].Height != h[j].Height {
		return h[i].Height > h[j].Height
	}
	return h[i].Hash.Compare(h[j].Hash) < 0
}
func (h byHeight) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *byHeight) Push(x any)   { *h = append(*h, x.(Commit)) }
func (h *byHeight) Pop() any {
	old := *h
	c := old[len(old)-1]
	*h = old[:len(old)-1]
	return c
}

// Log returns up to limit commits reachable from from, highest first.
func (r *Repo) Log(ctx context.Context, p auth.Principal, from hash.Hash, limit int) ([]Commit, error) {
	if err := r.check(ctx, p, auth.Read, "repo"); err != nil {
		return nil, err
	}
	if limit < 1 || limit > MaxLog {
		return nil, fmt.Errorf("%w: %d", ErrInvalidLimit, limit)
	}
	start, err := r.readCommit(ctx, from)
	if err != nil {
		return nil, err
	}
	q, seen := &byHeight{start}, map[hash.Hash]bool{from: true}
	var out []Commit
	for q.Len() > 0 && len(out) < limit {
		c := heap.Pop(q).(Commit)
		out = append(out, c)
		for _, ph := range c.Parents {
			if seen[ph] {
				continue
			}
			seen[ph] = true
			pc, err := r.readCommit(ctx, ph)
			if err != nil {
				return nil, err
			}
			heap.Push(q, pc)
		}
	}
	return out, nil
}

// MergeBase returns the best common ancestor of two commits.
func (r *Repo) MergeBase(ctx context.Context, p auth.Principal, a, b hash.Hash) (hash.Hash, error) {
	if err := r.check(ctx, p, auth.Read, "repo"); err != nil {
		return hash.Hash{}, err
	}
	return r.mergeBase(ctx, a, b)
}

// mergeBase walks both histories highest first. A commit's descendants are
// all higher than it, so when it is popped every path to it from a and b has
// been seen: the first commit reachable from both is the highest common
// ancestor (the lower hash on a tie), a best merge base. A commit is queued
// only while it has no flags, and queuing gives it some, so none is popped
// twice.
func (r *Repo) mergeBase(ctx context.Context, a, b hash.Hash) (hash.Hash, error) {
	const fromA, fromB = 1, 2
	flags := map[hash.Hash]int{}
	q := &byHeight{}
	for _, s := range []struct {
		h hash.Hash
		f int
	}{{a, fromA}, {b, fromB}} {
		if flags[s.h] == 0 {
			c, err := r.readCommit(ctx, s.h)
			if err != nil {
				return hash.Hash{}, err
			}
			heap.Push(q, c)
		}
		flags[s.h] |= s.f
	}
	for q.Len() > 0 {
		c := heap.Pop(q).(Commit)
		f := flags[c.Hash]
		if f == fromA|fromB {
			return c.Hash, nil
		}
		for _, ph := range c.Parents {
			if flags[ph] == 0 {
				pc, err := r.readCommit(ctx, ph)
				if err != nil {
					return hash.Hash{}, err
				}
				heap.Push(q, pc)
			}
			flags[ph] |= f
		}
	}
	return hash.Hash{}, fmt.Errorf("vcs: %s and %s share no history", a.Short(), b.Short())
}

// Merge merges a commit into a branch's working set, recording conflicts
// there; a failed merge leaves the working set as it was, and so does
// merging a commit the branch already holds (its head or an ancestor).
func (r *Repo) Merge(ctx context.Context, p auth.Principal, branch string, theirs hash.Hash) (merge.Result, error) {
	if err := r.branchCheck(ctx, p, auth.Merge, branch); err != nil {
		return merge.Result{}, err
	}
	m, err := r.refs(ctx)
	if err != nil {
		return merge.Result{}, err
	}
	headHash, work, err := branchRefs(ctx, m, branch)
	if err != nil {
		return merge.Result{}, err
	}
	ws, err := r.readWorkingSet(ctx, work)
	if err != nil {
		return merge.Result{}, err
	}
	if ws.Merge != nil {
		return merge.Result{}, fmt.Errorf("vcs: a merge is already in progress on %s", branch)
	}
	baseHash, err := r.mergeBase(ctx, headHash, theirs)
	if err != nil {
		return merge.Result{}, err
	}
	if baseHash == theirs { // the branch holds theirs already: nothing to merge
		ours, err := r.Namespace(ctx, ws.Working)
		return merge.Result{Merged: ours}, err
	}
	var ns [3]*object.Namespace
	for i, h := range []hash.Hash{baseHash, theirs} {
		c, err := r.readCommit(ctx, h)
		if err != nil {
			return merge.Result{}, err
		}
		if ns[i*2], err = r.Namespace(ctx, c.Namespace); err != nil {
			return merge.Result{}, err
		}
	}
	if ns[1], err = r.Namespace(ctx, ws.Working); err != nil {
		return merge.Result{}, err
	}
	res, err := merge.Merge(ctx, r.o.Registry, ns[0], ns[1], ns[2], r.s, merge.Options{MaxConflicts: r.o.MaxConflicts})
	if err != nil {
		return merge.Result{}, err
	}
	if err := r.checkPaths(ctx, p, branch, ws.Working, res.Merged.Root()); err != nil {
		return merge.Result{}, err
	}
	conflicts, err := prolly.Empty(ctx, r.s, r.o.Config)
	if err != nil {
		return merge.Result{}, err
	}
	ce := conflicts.Editor()
	for _, c := range res.Conflicts {
		if err := ce.Put([]byte(c.Path), encodeConflict(c)); err != nil {
			return merge.Result{}, err
		}
	}
	if conflicts, err = ce.Flush(ctx); err != nil {
		return merge.Result{}, err
	}
	next := WorkingSet{Working: res.Merged.Root(), Staged: res.Merged.Root(),
		Merge: &MergeState{Base: baseHash, Theirs: theirs, Conflicts: conflicts.Root(), PreWorking: ws.Working, PreStaged: ws.Staged}}
	if _, err := r.setWorkingSet(ctx, branch, ws, next, false); err != nil {
		return merge.Result{}, err
	}
	return res, nil
}

// AbortMerge abandons a branch's merge in progress: it drops the merge
// state and puts back the working and staged namespaces the merge started
// from. What the merge brought in, its resolutions and every edit made
// since it began are discarded; edits made before it began are not. It
// needs merge on the branch and write on every path it changes, and with no
// merge in progress it is ErrNoMerge.
func (r *Repo) AbortMerge(ctx context.Context, p auth.Principal, branch string) error {
	if err := r.branchCheck(ctx, p, auth.Merge, branch); err != nil {
		return err
	}
	return r.update(ctx, func(m *prolly.Map, e *prolly.Editor) error {
		_, work, err := branchRefs(ctx, m, branch)
		if err != nil {
			return err
		}
		ws, err := r.readWorkingSet(ctx, work)
		if err != nil {
			return err
		}
		if ws.Merge == nil {
			return fmt.Errorf("%w: %s", ErrNoMerge, branch)
		}
		back := WorkingSet{Working: ws.Merge.PreWorking, Staged: ws.Merge.PreStaged}
		for _, ns := range [][2]hash.Hash{{ws.Working, back.Working}, {ws.Staged, back.Staged}} {
			if err := r.checkPaths(ctx, p, branch, ns[0], ns[1]); err != nil {
				return err
			}
		}
		h, err := r.s.Put(ctx, back.encode())
		if err != nil {
			return err
		}
		return e.Put(workKey(branch), h[:])
	})
}

// Conflicts lists a branch's unresolved merge conflicts.
func (r *Repo) Conflicts(ctx context.Context, p auth.Principal, branch string) ([]merge.Conflict, error) {
	ws, err := r.WorkingSet(ctx, p, branch)
	if err != nil || ws.Merge == nil {
		return nil, err
	}
	m, err := prolly.Open(ctx, r.s, r.o.Config, ws.Merge.Conflicts)
	if err != nil {
		return nil, err
	}
	it, err := m.IterRange(ctx, nil, nil)
	if err != nil {
		return nil, err
	}
	var out []merge.Conflict
	for {
		k, v, ok, err := it.Next()
		if err != nil || !ok {
			return out, err
		}
		c, err := decodeConflict(string(k), v)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
}

// ResolveConflict settles a conflicting path (nil deletes it) in the working
// and staged namespaces, and drops its conflict.
func (r *Repo) ResolveConflict(ctx context.Context, p auth.Principal, branch, path string, to *object.Ref) error {
	if err := r.branchCheck(ctx, p, auth.Merge, branch); err != nil {
		return err
	}
	if err := r.check(ctx, p, auth.Write, pathResource(branch, path)); err != nil {
		return err
	}
	for attempt := 0; attempt < maxAttempts; attempt++ {
		ws, err := r.WorkingSet(ctx, p, branch)
		if err != nil {
			return err
		}
		if ws.Merge == nil {
			return fmt.Errorf("vcs: no merge is in progress on %s", branch)
		}
		cm, err := prolly.Open(ctx, r.s, r.o.Config, ws.Merge.Conflicts)
		if err != nil {
			return err
		}
		if _, ok, err := cm.Get(ctx, []byte(path)); err != nil {
			return err
		} else if !ok {
			return fmt.Errorf("vcs: no conflict at %s on %s", path, branch)
		}
		next := ws
		merging := *ws.Merge // resolving keeps where the merge started
		next.Merge = &merging
		for _, dst := range []*hash.Hash{&next.Working, &next.Staged} {
			n, err := r.Namespace(ctx, *dst)
			if err != nil {
				return err
			}
			e := n.Editor()
			if to == nil {
				err = e.Delete(path)
			} else {
				err = e.Put(path, *to)
			}
			if err != nil {
				return err
			}
			if n, err = e.Flush(ctx); err != nil {
				return err
			}
			*dst = n.Root()
		}
		ce := cm.Editor()
		if err := ce.Delete([]byte(path)); err != nil {
			return err
		}
		if cm, err = ce.Flush(ctx); err != nil {
			return err
		}
		next.Merge.Conflicts = cm.Root()
		if _, err = r.setWorkingSet(ctx, branch, ws, next, false); !errors.Is(err, ErrConflict) {
			return err
		}
	}
	return fmt.Errorf("vcs: the working set of %s kept changing", branch)
}
