package vcs_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/auth"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/memstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/merge"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/object"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
	"github.com/SmithOperatingSolutions/snapshot-core/core/vcs"
)

var (
	ctx   = context.Background()
	alice = auth.Principal{ID: "user:alice"}
	bob   = auth.Principal{ID: "user:bob"}
)

// lines (model 7) merges sets of lines; strict (8) never combines; failing (9) errors.
type lines struct{}

func (lines) ID() model.ID                                             { return 7 }
func (lines) FormatVersion() uint16                                    { return 1 }
func (lines) Validate(context.Context, model.Root, chunk.Reader) error { return nil }
func (lines) Diff(context.Context, model.Root, model.Root, chunk.Reader) (model.DiffIter, error) {
	return nil, errors.New("unused")
}

// Walk implements model.Walker: an object of lines is one chunk.
func (lines) Walk(_ context.Context, root model.Root, _ chunk.Reader, visit func(hash.Hash, bool) (bool, error)) error {
	_, err := visit(root.Hash, true)
	return err
}

func (lines) Merge(ctx context.Context, b, o, t model.Root, rw chunk.ReadWriter) (model.MergeResult, error) {
	var sets [3]map[string]bool
	for i, r := range []model.Root{b, o, t} {
		raw, err := rw.Get(ctx, r.Hash)
		if err != nil {
			return model.MergeResult{}, err
		}
		sets[i] = map[string]bool{}
		for _, l := range strings.Split(string(raw), "\n") {
			if l != "" {
				sets[i][l] = true
			}
		}
	}
	out := map[string]bool{}
	for l := range sets[0] {
		if sets[1][l] && sets[2][l] {
			out[l] = true
		}
	}
	for _, s := range sets[1:] {
		for l := range s {
			if !sets[0][l] {
				out[l] = true
			}
		}
	}
	return model.MergeResult{Root: store(ctx, rw, out)}, nil
}

func store(ctx context.Context, w chunk.Writer, s map[string]bool) model.Root {
	ls := make([]string, 0, len(s))
	for l := range s {
		ls = append(ls, l)
	}
	sort.Strings(ls)
	b := []byte(strings.Join(ls, "\n"))
	h, err := w.Put(ctx, b)
	if err != nil {
		panic(err)
	}
	return model.Root{Hash: h, Size: uint64(len(b)), Format: 1}
}

type strict struct{ lines }

func (strict) ID() model.ID { return 8 }
func (strict) Merge(context.Context, model.Root, model.Root, model.Root, chunk.ReadWriter) (model.MergeResult, error) {
	return model.MergeResult{Conflicts: []model.Conflict{{Location: []byte("all"), Reason: "strict"}}}, nil
}

var errModelFailed = errors.New("the model failed")

type failing struct{ lines }

func (failing) ID() model.ID { return 9 }
func (failing) Merge(context.Context, model.Root, model.Root, model.Root, chunk.ReadWriter) (model.MergeResult, error) {
	return model.MergeResult{}, errModelFailed
}

// clock ticks one second per reading, from a fixed start.
type clock struct{ n atomic.Int64 }

func (c *clock) now() time.Time {
	return time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC).Add(time.Duration(c.n.Add(1)) * time.Second)
}

type fixture struct {
	t   *testing.T
	s   chunk.Store
	r   *vcs.Repo
	o   vcs.Options
	clk *clock
}

func options(t *testing.T, az auth.Authorizer, clk *clock) vcs.Options {
	t.Helper()
	reg, err := model.NewRegistry(lines{}, strict{}, failing{}, fakeModel(1), fakeModel(3), fakeModel(4))
	if err != nil {
		t.Fatal(err)
	}
	return vcs.Options{Config: prolly.DefaultConfig(), Registry: reg, Authorizer: az, Clock: clk.now}
}

// fakeModel is a model with only an identity, for objects nothing merges.
type fakeModel model.ID

func (f fakeModel) ID() model.ID                                           { return model.ID(f) }
func (fakeModel) FormatVersion() uint16                                    { return 1 }
func (fakeModel) Validate(context.Context, model.Root, chunk.Reader) error { return nil }
func (fakeModel) Diff(context.Context, model.Root, model.Root, chunk.Reader) (model.DiffIter, error) {
	return nil, errors.New("unused")
}
func (fakeModel) Merge(context.Context, model.Root, model.Root, model.Root, chunk.ReadWriter) (model.MergeResult, error) {
	return model.MergeResult{}, errors.New("unused")
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	return newFixtureOn(t, memstore.New())
}

// newFixtureOn makes a repository on s, with an authorizer that allows all.
func newFixtureOn(t *testing.T, s chunk.Store) *fixture {
	t.Helper()
	clk := &clock{}
	f := &fixture{t: t, s: s, clk: clk}
	f.o = options(t, auth.AllowAll{}, clk)
	r, err := vcs.Init(ctx, f.s, alice, f.o)
	if err != nil {
		t.Fatal(err)
	}
	f.r = r
	return f
}

func (f *fixture) obj(m model.ID, ls ...string) object.Ref {
	s := map[string]bool{}
	for _, l := range ls {
		s[l] = true
	}
	return object.Ref{Model: m, Root: store(ctx, f.s, s)}
}

// edit applies puts (a nil ref deletes) to a branch's working namespace and
// stages it, retrying if another writer got in first.
func (f *fixture) edit(p auth.Principal, branch string, puts map[string]*object.Ref) {
	f.t.Helper()
	for attempt := 0; attempt < 100; attempt++ {
		ws, err := f.r.WorkingSet(ctx, p, branch)
		if err != nil {
			f.t.Fatal(err)
		}
		n, err := f.r.Namespace(ctx, ws.Working)
		if err != nil {
			f.t.Fatal(err)
		}
		e := n.Editor()
		for path, ref := range puts {
			if ref == nil {
				err = e.Delete(path)
			} else {
				err = e.Put(path, *ref)
			}
			if err != nil {
				f.t.Fatal(err)
			}
		}
		if n, err = e.Flush(ctx); err != nil {
			f.t.Fatal(err)
		}
		next := ws
		next.Working, next.Staged = n.Root(), n.Root()
		if _, err = f.r.UpdateWorkingSet(ctx, p, branch, ws, next); err == nil {
			return
		}
		if !errors.Is(err, vcs.ErrConflict) {
			f.t.Fatal(err)
		}
	}
	f.t.Fatal("could not update the working set in 100 attempts")
}

func (f *fixture) put(branch, path string, ref object.Ref) {
	f.edit(alice, branch, map[string]*object.Ref{path: &ref})
}

func (f *fixture) commit(branch, msg string) vcs.Commit {
	f.t.Helper()
	c, err := f.r.CommitWorkingSet(ctx, alice, branch, msg)
	if err != nil {
		f.t.Fatalf("committing %s: %v", branch, err)
	}
	return c
}

func (f *fixture) head(branch string) vcs.Commit {
	f.t.Helper()
	c, err := f.r.Head(ctx, alice, branch)
	if err != nil {
		f.t.Fatal(err)
	}
	return c
}

func (f *fixture) get(root hash.Hash, path string) (object.Ref, bool) {
	f.t.Helper()
	n, err := f.r.Namespace(ctx, root)
	if err != nil {
		f.t.Fatal(err)
	}
	r, _, ok, err := n.Get(ctx, path)
	if err != nil {
		f.t.Fatal(err)
	}
	return r, ok
}

// Engine Spec L2: "A new repo has one branch main pointing at an empty-root
// initial commit".
func TestANewRepoHasMainAtAnEmptyInitialCommit(t *testing.T) {
	f := newFixture(t)
	c := f.head(vcs.MainBranch)
	empty, err := object.New(ctx, memstore.New(), prolly.DefaultConfig(), f.o.Registry)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Parents) != 0 || c.Height != 0 || c.Namespace != empty.Root() || c.Author != alice.ID || c.Hash.IsZero() {
		t.Fatalf("the initial commit is %+v; want no parents, height 0, the empty namespace %s, by %s", c, empty.Root(), alice.ID)
	}
	if bs, err := f.r.Branches(ctx, alice); err != nil || len(bs) != 1 || bs[0] != vcs.MainBranch {
		t.Fatalf("Branches = %v, %v; want [main]", bs, err)
	}
	ws, err := f.r.WorkingSet(ctx, alice, vcs.MainBranch)
	if err != nil || ws.Working != empty.Root() || ws.Staged != empty.Root() || ws.Merge != nil || ws.Hash.IsZero() {
		t.Fatalf("main's working set is %+v (%v); want both namespaces empty and no merge", ws, err)
	}
	if _, err := vcs.Init(ctx, f.s, alice, f.o); !errors.Is(err, vcs.ErrExists) {
		t.Fatalf("a second Init = %v, want ErrExists", err)
	}
	re, err := vcs.Open(ctx, f.s, f.o)
	if err != nil {
		t.Fatal(err)
	}
	if again, err := re.Head(ctx, alice, vcs.MainBranch); err != nil || again.Hash != c.Hash {
		t.Fatalf("reopened, main is %v (%v), want %v", again.Hash, err, c.Hash)
	}
	if _, err := vcs.Open(ctx, memstore.New(), f.o); !errors.Is(err, vcs.ErrNoRepo) {
		t.Fatalf("Open of an empty store = %v, want ErrNoRepo", err)
	}
}

// Engine Spec L2: "Committing a working set produces a commit whose parent
// is the old head".
func TestCommittingParentsTheOldHead(t *testing.T) {
	f := newFixture(t)
	old := f.head(vcs.MainBranch)
	f.put(vcs.MainBranch, "files/a", f.obj(7, "a"))
	ws, _ := f.r.WorkingSet(ctx, alice, vcs.MainBranch)
	c := f.commit(vcs.MainBranch, "add a")
	if len(c.Parents) != 1 || c.Parents[0] != old.Hash || c.Height != 1 || c.Namespace != ws.Staged || c.Message != "add a" {
		t.Fatalf("the commit is %+v; want parent %v, height 1, namespace %v", c, old.Hash, ws.Staged)
	}
	if got := f.head(vcs.MainBranch); got.Hash != c.Hash {
		t.Fatalf("main is %v, want the new commit %v", got.Hash, c.Hash)
	}
	if back, err := f.r.ReadCommit(ctx, alice, c.Hash); err != nil || back.Hash != c.Hash || back.Message != "add a" || !back.Time.Equal(c.Time) {
		t.Fatalf("ReadCommit = %+v, %v", back, err)
	}
}

// Engine Spec L2: "Two sessions commit to the same branch concurrently: both
// commits land, in some order, and neither is lost".
func TestConcurrentCommitsBothLand(t *testing.T) {
	f := newFixture(t)
	var wg sync.WaitGroup
	for i, p := range []auth.Principal{alice, bob} {
		wg.Add(1)
		go func(i int, p auth.Principal) {
			defer wg.Done()
			ref := f.obj(7, p.ID)
			f.edit(p, vcs.MainBranch, map[string]*object.Ref{fmt.Sprintf("by/%d", i): &ref})
			if _, err := f.r.CommitWorkingSet(ctx, p, vcs.MainBranch, "commit by "+p.ID); err != nil {
				t.Errorf("%s's commit: %v", p.ID, err)
			}
		}(i, p)
	}
	wg.Wait()
	log, err := f.r.Log(ctx, alice, f.head(vcs.MainBranch).Hash, 10)
	if err != nil {
		t.Fatal(err)
	}
	var msgs []string
	for _, c := range log {
		msgs = append(msgs, c.Message)
	}
	joined := strings.Join(msgs, "|")
	if len(log) != 3 || !strings.Contains(joined, "commit by user:alice") || !strings.Contains(joined, "commit by user:bob") {
		t.Fatalf("after two concurrent commits the log is %v, want both commits and the initial one", msgs)
	}
}

// Engine Spec L2: "UpdateWorkingSet with a stale prev returns ErrConflict".
func TestAStaleWorkingSetUpdateIsAConflict(t *testing.T) {
	f := newFixture(t)
	stale, _ := f.r.WorkingSet(ctx, alice, vcs.MainBranch)
	f.put(vcs.MainBranch, "x", f.obj(7, "x"))
	current, _ := f.r.WorkingSet(ctx, alice, vcs.MainBranch)
	next := current
	next.Staged = stale.Staged
	if _, err := f.r.UpdateWorkingSet(ctx, alice, vcs.MainBranch, stale, next); !errors.Is(err, vcs.ErrConflict) {
		t.Fatalf("an update from a stale working set = %v, want ErrConflict", err)
	}
	if now, _ := f.r.WorkingSet(ctx, alice, vcs.MainBranch); now.Hash != current.Hash {
		t.Fatal("a refused update changed the working set")
	}
	if _, err := f.r.UpdateWorkingSet(ctx, alice, vcs.MainBranch, current, next); err != nil {
		t.Fatalf("positive control: an update from the current working set: %v", err)
	}
}

// writeCommit stores a commit by hand, for shaping a history exactly.
func writeCommit(t *testing.T, s chunk.Store, parents []hash.Hash, height uint64, msg string) hash.Hash {
	t.Helper()
	c := vcs.Commit{Parents: parents, Namespace: hash.Sum([]byte("ns")), Height: height, Time: time.Unix(0, int64(height)).UTC(), Author: "user:test", Message: msg}
	h, err := s.Put(ctx, c.Encode())
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// Engine Spec L2: "MergeBase is correct on linear history, a simple fork,
// and a criss-cross merge (table-driven)".
func TestMergeBase(t *testing.T) {
	f := newFixture(t)
	root := writeCommit(t, f.s, nil, 0, "root")
	a1 := writeCommit(t, f.s, []hash.Hash{root}, 1, "a1")
	a2 := writeCommit(t, f.s, []hash.Hash{a1}, 2, "a2")
	b1 := writeCommit(t, f.s, []hash.Hash{root}, 1, "b1")
	b2 := writeCommit(t, f.s, []hash.Hash{b1}, 2, "b2")
	// criss-cross: m1 merges b2 into a2, m2 merges a2 into b2.
	m1 := writeCommit(t, f.s, []hash.Hash{a2, b2}, 3, "m1")
	m2 := writeCommit(t, f.s, []hash.Hash{b2, a2}, 3, "m2")
	for _, c := range []struct {
		name string
		a, b hash.Hash
		want []hash.Hash // any of these
	}{
		{"linear: an ancestor", a2, a1, []hash.Hash{a1}},
		{"linear: itself", a2, a2, []hash.Hash{a2}},
		{"a simple fork", a2, b2, []hash.Hash{root}},
		{"criss-cross", m1, m2, []hash.Hash{a2, b2}},
	} {
		got, err := f.r.MergeBase(ctx, alice, c.a, c.b)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		again, _ := f.r.MergeBase(ctx, alice, c.b, c.a)
		ok := false
		for _, w := range c.want {
			ok = ok || got == w
		}
		if !ok || again != got {
			t.Errorf("%s: MergeBase = %v (reversed %v), want one of %v, the same both ways", c.name, got, again, c.want)
		}
	}
}

// Engine Spec L2: "Invalid branch names (../x, a..b, 129 chars, empty,
// x.lock) are rejected".
func TestInvalidBranchNamesAreRejected(t *testing.T) {
	f := newFixture(t)
	at := f.head(vcs.MainBranch).Hash
	for _, name := range []string{"dev", "feature/x", "v1.2", "a_b-c", strings.Repeat("a", 128)} {
		if err := f.r.CreateBranch(ctx, alice, name, at); err != nil {
			t.Errorf("positive control: CreateBranch(%q) = %v", name, err)
		}
	}
	for _, name := range []string{"../x", "a..b", strings.Repeat("a", 129), "", "x.lock", "x/", "a//b", "-x", ".x", "a b", "a\x00b"} {
		if err := f.r.CreateBranch(ctx, alice, name, at); !errors.Is(err, vcs.ErrInvalidName) {
			t.Errorf("CreateBranch(%q) = %v, want ErrInvalidName", name, err)
		}
	}
	if err := f.r.CreateBranch(ctx, alice, "dev", at); !errors.Is(err, vcs.ErrBranchExists) {
		t.Errorf("creating an existing branch = %v, want ErrBranchExists", err)
	}
}

// Engine Spec L2: "Deleting the checked-out branch of an active session
// returns ErrBranchInUse".
func TestDeletingACheckedOutBranchIsRefused(t *testing.T) {
	f := newFixture(t)
	if err := f.r.CreateBranch(ctx, alice, "dev", f.head(vcs.MainBranch).Hash); err != nil {
		t.Fatal(err)
	}
	sess, err := f.r.Checkout(ctx, bob, "dev")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.r.DeleteBranch(ctx, alice, "dev"); !errors.Is(err, vcs.ErrBranchInUse) {
		t.Fatalf("deleting a checked-out branch = %v, want ErrBranchInUse", err)
	}
	sess.Close()
	if err := f.r.DeleteBranch(ctx, alice, "dev"); err != nil {
		t.Fatalf("deleting it once the session closed: %v", err)
	}
	if _, err := f.r.Head(ctx, alice, "dev"); !errors.Is(err, vcs.ErrBranchNotFound) {
		t.Fatalf("a deleted branch's head = %v, want ErrBranchNotFound", err)
	}
	if _, err := f.r.WorkingSet(ctx, alice, "dev"); !errors.Is(err, vcs.ErrBranchNotFound) {
		t.Fatalf("a deleted branch's working set = %v, want ErrBranchNotFound", err)
	}
}

// Engine Spec L2: "Log limit is required and capped at 10,000".
func TestLogIsBoundedAndHighestFirst(t *testing.T) {
	f := newFixture(t)
	for i := 0; i < 4; i++ {
		f.put(vcs.MainBranch, fmt.Sprintf("f%d", i), f.obj(7, fmt.Sprint(i)))
		f.commit(vcs.MainBranch, fmt.Sprintf("c%d", i))
	}
	head := f.head(vcs.MainBranch).Hash
	log, err := f.r.Log(ctx, alice, head, 3)
	if err != nil || len(log) != 3 || log[0].Message != "c3" || log[1].Message != "c2" || log[2].Message != "c1" {
		t.Fatalf("Log(3) = %d commits (%v)", len(log), err)
	}
	if all, err := f.r.Log(ctx, alice, head, vcs.MaxLog); err != nil || len(all) != 5 {
		t.Fatalf("Log(10,000) = %d commits, %v; want all 5", len(all), err)
	}
	for _, limit := range []int{0, -1, vcs.MaxLog + 1} {
		if _, err := f.r.Log(ctx, alice, head, limit); !errors.Is(err, vcs.ErrInvalidLimit) {
			t.Errorf("Log(limit %d) = %v, want ErrInvalidLimit", limit, err)
		}
	}
}

// recording answers yes and remembers what it was asked.
type recording struct {
	mu    sync.Mutex
	calls []string
}

func (r *recording) Authorize(_ context.Context, p auth.Principal, a auth.Action, res string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, fmt.Sprintf("%s %d %s", p.ID, a, res))
	return nil
}

// readOnly allows reading and nothing else.
type readOnly struct{}

func (readOnly) Authorize(_ context.Context, _ auth.Principal, a auth.Action, _ string) error {
	if a != auth.Read {
		return errors.New("read only")
	}
	return nil
}

// Every call asks the authorizer about exactly what it does; none gets
// through a repository whose authorizer denies (nil denies too), and none
// that changes anything gets through on permission to read.
func TestEveryCallIsAuthorized(t *testing.T) {
	f := newFixture(t)
	f.put(vcs.MainBranch, "doc", f.obj(8, "base"))
	f.commit(vcs.MainBranch, "base")
	f.branchFrom("dev")
	f.put("dev", "doc", f.obj(8, "dev"))
	theirs := f.commit("dev", "dev work").Hash
	f.put(vcs.MainBranch, "doc", f.obj(8, "main"))
	head := f.commit(vcs.MainBranch, "main work").Hash
	ws, err := f.r.WorkingSet(ctx, alice, vcs.MainBranch)
	if err != nil {
		t.Fatal(err)
	}
	resolved := f.obj(8, "resolved")
	rec := &recording{}
	o := f.o
	o.Authorizer = rec
	r, err := vcs.Open(ctx, f.s, o)
	if err != nil {
		t.Fatal(err)
	}
	type call struct {
		want string
		do   func(r *vcs.Repo) error
	}
	calls := []call{
		{"user:bob 1 branch:main", func(r *vcs.Repo) error { _, err := r.Head(ctx, bob, "main"); return err }},
		{"user:bob 1 branch:main", func(r *vcs.Repo) error { _, err := r.WorkingSet(ctx, bob, "main"); return err }},
		{"user:bob 1 repo", func(r *vcs.Repo) error { _, err := r.Branches(ctx, bob); return err }},
		{"user:bob 1 repo", func(r *vcs.Repo) error { _, err := r.ReadCommit(ctx, bob, head); return err }},
		{"user:bob 1 repo", func(r *vcs.Repo) error { _, err := r.Log(ctx, bob, head, 1); return err }},
		{"user:bob 1 repo", func(r *vcs.Repo) error { _, err := r.MergeBase(ctx, bob, head, theirs); return err }},
		{"user:bob 2 branch:main", func(r *vcs.Repo) error { _, err := r.UpdateWorkingSet(ctx, bob, "main", ws, ws); return err }},
		{"user:bob 2 branch:main", func(r *vcs.Repo) error { _, err := r.CommitWorkingSet(ctx, bob, "main", "m"); return err }},
		{"user:bob 3 branch:feature", func(r *vcs.Repo) error { return r.CreateBranch(ctx, bob, "feature", head) }},
		{"user:bob 1 branch:feature", func(r *vcs.Repo) error {
			sess, err := r.Checkout(ctx, bob, "feature")
			if err == nil {
				sess.Close()
			}
			return err
		}},
		{"user:bob 3 branch:feature", func(r *vcs.Repo) error { return r.DeleteBranch(ctx, bob, "feature") }},
		{"user:bob 3 tag:v1", func(r *vcs.Repo) error { _, err := r.CreateTag(ctx, bob, "v1", head, "t"); return err }},
		{"user:bob 1 tag:v1", func(r *vcs.Repo) error { _, err := r.Tag(ctx, bob, "v1"); return err }},
		{"user:bob 1 repo", func(r *vcs.Repo) error { _, err := r.Tags(ctx, bob); return err }},
		{"user:bob 3 tag:v1", func(r *vcs.Repo) error { return r.DeleteTag(ctx, bob, "v1") }},
		{"user:bob 2 branch:main", func(r *vcs.Repo) error { _, err := r.Merge(ctx, bob, "main", theirs); return err }},
		{"user:bob 1 branch:main", func(r *vcs.Repo) error { _, err := r.Conflicts(ctx, bob, "main"); return err }},
		{"user:bob 2 branch:main", func(r *vcs.Repo) error { return r.ResolveConflict(ctx, bob, "main", "doc", &resolved) }},
	}
	for _, c := range calls {
		rec.calls = nil
		if err := c.do(r); err != nil {
			t.Fatalf("%s: %v", c.want, err)
		}
		if len(rec.calls) == 0 || rec.calls[0] != c.want {
			t.Errorf("the authorizer was asked %v, want first %q", rec.calls, c.want)
		}
	}
	for name, az := range map[string]auth.Authorizer{"DenyAll": auth.DenyAll{}, "nil": nil} {
		o := f.o
		o.Authorizer = az
		denied, err := vcs.Open(ctx, f.s, o)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range calls {
			if err := c.do(denied); !errors.Is(err, auth.ErrDenied) {
				t.Errorf("%s: %q = %v, want ErrDenied", name, c.want, err)
			}
		}
	}
	o.Authorizer = readOnly{}
	reader, err := vcs.Open(ctx, f.s, o)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range calls {
		reads := strings.Fields(c.want)[1] == "1"
		if err := c.do(reader); errors.Is(err, auth.ErrDenied) == reads {
			t.Errorf("read only: %q = %v; want ErrDenied exactly when it needs more than read", c.want, err)
		}
	}
}

// Engine Spec L2: "Commit metadata author comes from the authenticated
// Principal, not from client input".
func TestTheAuthorIsThePrincipal(t *testing.T) {
	f := newFixture(t)
	f.put(vcs.MainBranch, "x", f.obj(7, "x"))
	c, err := f.r.CommitWorkingSet(ctx, bob, vcs.MainBranch, "by bob")
	if err != nil || c.Author != bob.ID {
		t.Fatalf("a commit by bob has author %q (%v)", c.Author, err)
	}
	if _, err := f.r.CommitWorkingSet(ctx, auth.Principal{ID: ""}, vcs.MainBranch, "anonymous"); !errors.Is(err, auth.ErrInvalidPrincipal) {
		t.Fatalf("a commit by an invalid principal = %v, want ErrInvalidPrincipal", err)
	}
}

// branchFrom makes a branch at main's head.
func (f *fixture) branchFrom(name string) {
	f.t.Helper()
	if err := f.r.CreateBranch(ctx, alice, name, f.head(vcs.MainBranch).Hash); err != nil {
		f.t.Fatal(err)
	}
}

// Engine Spec L3: conflicts are stored in the working set, and "a commit is
// refused while unresolved conflicts exist on that branch"; resolved, the
// merge commits with both parents.
func TestConflictsBlockTheCommitUntilResolved(t *testing.T) {
	f := newFixture(t)
	f.put(vcs.MainBranch, "doc", f.obj(8, "base"))
	f.put(vcs.MainBranch, "notes", f.obj(7, "a"))
	f.commit(vcs.MainBranch, "base")
	f.branchFrom("dev")
	f.put("dev", "doc", f.obj(8, "dev"))
	f.put("dev", "notes", f.obj(7, "a", "dev"))
	theirs := f.commit("dev", "dev work")
	f.put(vcs.MainBranch, "doc", f.obj(8, "main"))
	f.put(vcs.MainBranch, "notes", f.obj(7, "a", "main"))
	ours := f.commit(vcs.MainBranch, "main work")

	r, err := f.r.Merge(ctx, alice, vcs.MainBranch, theirs.Hash)
	if err != nil || len(r.Conflicts) != 1 || r.Conflicts[0].Path != "doc" {
		t.Fatalf("the merge found %+v (%v), want one conflict at doc", r.Conflicts, err)
	}
	stored, err := f.r.Conflicts(ctx, alice, vcs.MainBranch)
	if err != nil || len(stored) != 1 || stored[0].Path != "doc" || stored[0].Kind != merge.BothChanged {
		t.Fatalf("the working set holds conflicts %+v (%v)", stored, err)
	}
	ws, _ := f.r.WorkingSet(ctx, alice, vcs.MainBranch)
	if got, _ := f.get(ws.Working, "notes"); got != f.obj(7, "a", "dev", "main") {
		t.Fatal("the merge did not combine what it could while a conflict stood")
	}
	if _, err := f.r.CommitWorkingSet(ctx, alice, vcs.MainBranch, "too soon"); !errors.Is(err, vcs.ErrUnresolvedConflicts) {
		t.Fatalf("committing with a conflict standing = %v, want ErrUnresolvedConflicts", err)
	}
	resolved := f.obj(8, "resolved")
	if err := f.r.ResolveConflict(ctx, alice, vcs.MainBranch, "doc", &resolved); err != nil {
		t.Fatal(err)
	}
	c := f.commit(vcs.MainBranch, "merge dev")
	if len(c.Parents) != 2 || c.Parents[0] != ours.Hash || c.Parents[1] != theirs.Hash || c.Height != ours.Height+1 {
		t.Fatalf("the merge commit is %+v; want parents [ours theirs] and height %d", c, ours.Height+1)
	}
	if got, _ := f.get(c.Namespace, "doc"); got != resolved {
		t.Fatal("the merge commit does not hold the resolution")
	}
	if ws, _ := f.r.WorkingSet(ctx, alice, vcs.MainBranch); ws.Merge != nil {
		t.Fatal("the merge state outlived its commit")
	}
}

// Engine Spec L3: "A commit is refused while unresolved conflicts exist on
// that branch". So UpdateWorkingSet cannot change a merge in progress: not
// drop it, not empty its conflicts, not name a commit that was never merged
// as the second parent, and not by misstating the merge state in prev.
// Editing files during a merge keeps the merge state, and goes through.
func TestUpdateWorkingSetKeepsTheMergeState(t *testing.T) {
	f := newFixture(t)
	main := vcs.MainBranch
	f.put(main, "doc", f.obj(8, "base"))
	base := f.commit(main, "base")
	f.branchFrom("dev")
	f.put("dev", "doc", f.obj(8, "dev"))
	theirs := f.commit("dev", "dev work")
	f.put(main, "doc", f.obj(8, "main"))
	f.commit(main, "main work")
	if r, err := f.r.Merge(ctx, alice, main, theirs.Hash); err != nil || len(r.Conflicts) != 1 {
		t.Fatalf("fixture: the merge found %d conflicts (%v), want 1", len(r.Conflicts), err)
	}
	ws, err := f.r.WorkingSet(ctx, alice, main)
	if err != nil || ws.Merge == nil {
		t.Fatalf("fixture: the working set is %+v (%v), want a merge in progress", ws, err)
	}
	none, err := prolly.Empty(ctx, f.s, f.o.Config)
	if err != nil {
		t.Fatal(err)
	}
	misstated := ws
	misstated.Merge = nil
	for _, c := range []struct {
		name  string
		prev  vcs.WorkingSet
		merge *vcs.MergeState
	}{
		{"dropped", ws, nil},
		{"with its conflicts emptied", ws, &vcs.MergeState{Base: ws.Merge.Base, Theirs: ws.Merge.Theirs, Conflicts: none.Root()}},
		{"naming another commit as theirs", ws, &vcs.MergeState{Base: ws.Merge.Base, Theirs: base.Hash, Conflicts: ws.Merge.Conflicts}},
		{"dropped, from a prev that says there is none", misstated, nil},
	} {
		next := ws
		next.Merge = c.merge
		if _, err := f.r.UpdateWorkingSet(ctx, alice, main, c.prev, next); !errors.Is(err, vcs.ErrMergeState) {
			t.Errorf("an update with the merge %s = %v, want ErrMergeState", c.name, err)
		}
		if now, _ := f.r.WorkingSet(ctx, alice, main); now.Hash != ws.Hash {
			t.Fatalf("a refused update (the merge %s) changed the working set", c.name)
		}
	}
	if _, err := f.r.CommitWorkingSet(ctx, alice, main, "unresolved"); !errors.Is(err, vcs.ErrUnresolvedConflicts) {
		t.Fatalf("committing after the refused updates = %v, want ErrUnresolvedConflicts", err)
	}
	f.put(main, "notes", f.obj(7, "during the merge"))
	if now, _ := f.r.WorkingSet(ctx, alice, main); now.Merge == nil || *now.Merge != *ws.Merge {
		t.Fatalf("editing a file during the merge left the merge state %+v, want %+v", now.Merge, ws.Merge)
	}
}

// Merging a commit the branch already holds, its head or an ancestor,
// changes nothing: no merge starts, and the next commit is an ordinary one.
// Merging the head used to start a merge whose commit named the head as
// both parents, which no reader accepts, leaving the branch unreadable.
func TestMergingWhatIsAlreadyMergedChangesNothing(t *testing.T) {
	f := newFixture(t)
	main := vcs.MainBranch
	f.put(main, "a", f.obj(7, "a"))
	first := f.commit(main, "first")
	f.put(main, "b", f.obj(7, "b"))
	head := f.commit(main, "second")
	f.put(main, "uncommitted", f.obj(7, "u"))
	before, err := f.r.WorkingSet(ctx, alice, main)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name   string
		theirs hash.Hash
	}{{"its head", head.Hash}, {"an ancestor", first.Hash}} {
		r, err := f.r.Merge(ctx, alice, main, c.theirs)
		if err != nil || len(r.Conflicts) != 0 || r.Merged == nil || r.Merged.Root() != before.Working {
			t.Fatalf("merging %s = %+v, %v; want nothing merged", c.name, r, err)
		}
		if now, _ := f.r.WorkingSet(ctx, alice, main); now.Hash != before.Hash {
			t.Fatalf("merging %s changed the working set (a merge in progress: %v)", c.name, now.Merge != nil)
		}
	}
	c := f.commit(main, "after")
	if len(c.Parents) != 1 || c.Parents[0] != head.Hash {
		t.Fatalf("the commit after merging what was merged has parents %v, want [%s]", c.Parents, head.Hash)
	}
	if got := f.head(main); got.Hash != c.Hash {
		t.Fatalf("main is %s, want %s", got.Hash, c.Hash)
	}
}

// Engine Spec L3: "A CellMerger error mid-merge leaves the working set hash
// unchanged"; so does passing the conflict limit.
func TestAFailedMergeLeavesTheWorkingSetUnchanged(t *testing.T) {
	for name, model := range map[string]model.ID{"a model error": 9, "too many conflicts": 8} {
		f := newFixture(t)
		o := f.o
		o.MaxConflicts = 1
		r, err := vcs.Open(ctx, f.s, o)
		if err != nil {
			t.Fatal(err)
		}
		f.r = r
		f.put(vcs.MainBranch, "a", f.obj(model, "base"))
		f.put(vcs.MainBranch, "b", f.obj(model, "base"))
		f.commit(vcs.MainBranch, "base")
		f.branchFrom("dev")
		f.edit(alice, "dev", map[string]*object.Ref{"a": ptr(f.obj(model, "dev")), "b": ptr(f.obj(model, "dev"))})
		theirs := f.commit("dev", "dev")
		f.edit(alice, vcs.MainBranch, map[string]*object.Ref{"a": ptr(f.obj(model, "main")), "b": ptr(f.obj(model, "main"))})
		f.commit(vcs.MainBranch, "main")
		before, _ := f.r.WorkingSet(ctx, alice, vcs.MainBranch)
		if _, err := f.r.Merge(ctx, alice, vcs.MainBranch, theirs.Hash); err == nil {
			t.Fatalf("%s: the merge succeeded", name)
		}
		if after, _ := f.r.WorkingSet(ctx, alice, vcs.MainBranch); after.Hash != before.Hash {
			t.Fatalf("%s: a failed merge changed the working set", name)
		}
	}
}

func ptr(r object.Ref) *object.Ref { return &r }

// Storage Core Spec: "One commit holding a table, a blob and a JSON document
// round-trips all three" (models 3, 1 and 4, as fakes).
func TestACommitHoldingThreeModelsRoundTrips(t *testing.T) {
	f := newFixture(t)
	objs := map[string]object.Ref{"db/users": f.obj(3, "table"), "files/logo.png": f.obj(1, "blob"), "docs/config.json": f.obj(4, "json")}
	edits := map[string]*object.Ref{}
	for p, r := range objs {
		edits[p] = ptr(r)
	}
	f.edit(alice, vcs.MainBranch, edits)
	c := f.commit(vcs.MainBranch, "three models")
	re, err := vcs.Open(ctx, f.s, f.o)
	if err != nil {
		t.Fatal(err)
	}
	head, err := re.Head(ctx, alice, vcs.MainBranch)
	if err != nil || head.Hash != c.Hash {
		t.Fatalf("reopened, main is %v (%v)", head.Hash, err)
	}
	n, err := re.Namespace(ctx, head.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	for p, want := range objs {
		got, m, ok, err := n.Get(ctx, p)
		if err != nil || !ok || got != want || m.ID() != want.Model {
			t.Fatalf("%s in the reopened commit: %+v (model %v), %v, %v", p, got, m, ok, err)
		}
	}
}

// Tags name commits, once.
func TestTagsNameCommits(t *testing.T) {
	f := newFixture(t)
	head := f.head(vcs.MainBranch)
	tag, err := f.r.CreateTag(ctx, alice, "v1.0", head.Hash, "first release")
	if err != nil || tag.Target != head.Hash || tag.Tagger != alice.ID || tag.Hash.IsZero() {
		t.Fatalf("CreateTag = %+v, %v", tag, err)
	}
	if _, err := f.r.CreateTag(ctx, alice, "v1.0", head.Hash, "again"); !errors.Is(err, vcs.ErrTagExists) {
		t.Fatalf("a second tag of one name = %v, want ErrTagExists", err)
	}
	if _, err := f.r.CreateTag(ctx, alice, "bad..name", head.Hash, "x"); !errors.Is(err, vcs.ErrInvalidName) {
		t.Fatalf("an invalid tag name = %v, want ErrInvalidName", err)
	}
}

// Tags are listed in order and read back by name as they were created
// (issue #2).
func TestTagsAreListedAndReadBack(t *testing.T) {
	f := newFixture(t)
	if tags, err := f.r.Tags(ctx, alice); err != nil || len(tags) != 0 {
		t.Fatalf("a new repository lists tags %q (%v), want none", tags, err)
	}
	head := f.head(vcs.MainBranch).Hash
	created := map[string]vcs.Tag{}
	for _, c := range []struct {
		name, message string
		by            auth.Principal
	}{{"v1.0", "first release", alice}, {"v0.9", "a preview", bob}, {"nightly/2026-09-23", "", alice}} {
		tag, err := f.r.CreateTag(ctx, c.by, c.name, head, c.message)
		if err != nil {
			t.Fatal(err)
		}
		created[c.name] = tag
	}
	tags, err := f.r.Tags(ctx, alice)
	if want := "nightly/2026-09-23 v0.9 v1.0"; err != nil || strings.Join(tags, " ") != want {
		t.Fatalf("Tags = %q (%v), want %q", tags, err, want)
	}
	for name, want := range created {
		got, err := f.r.Tag(ctx, bob, name)
		if err != nil {
			t.Fatalf("Tag(%q) = %v", name, err)
		}
		if got.Hash != want.Hash || got.Target != want.Target || !got.Time.Equal(want.Time) || got.Tagger != want.Tagger || got.Message != want.Message {
			t.Errorf("Tag(%q) reads back %+v, created as %+v", name, got, want)
		}
	}
}

// A tag the refs do not hold is not found, to read or to delete; a deleted
// tag is gone, and its name takes a new tag (issue #2).
func TestAMissingOrDeletedTagIsNotFound(t *testing.T) {
	f := newFixture(t)
	if _, err := f.r.Tag(ctx, alice, "v9"); !errors.Is(err, vcs.ErrTagNotFound) {
		t.Fatalf("reading a tag never created = %v, want ErrTagNotFound", err)
	}
	if err := f.r.DeleteTag(ctx, alice, "v9"); !errors.Is(err, vcs.ErrTagNotFound) {
		t.Fatalf("deleting a tag never created = %v, want ErrTagNotFound", err)
	}
	if _, err := f.r.Tag(ctx, alice, "bad..name"); !errors.Is(err, vcs.ErrInvalidName) {
		t.Fatalf("reading an invalid tag name = %v, want ErrInvalidName", err)
	}
	if err := f.r.DeleteTag(ctx, alice, "bad..name"); !errors.Is(err, vcs.ErrInvalidName) {
		t.Fatalf("deleting an invalid tag name = %v, want ErrInvalidName", err)
	}
	head := f.head(vcs.MainBranch).Hash
	first, err := f.r.CreateTag(ctx, alice, "v1", head, "first")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.r.DeleteTag(ctx, alice, "v1"); err != nil {
		t.Fatalf("DeleteTag = %v", err)
	}
	if _, err := f.r.Tag(ctx, alice, "v1"); !errors.Is(err, vcs.ErrTagNotFound) {
		t.Fatalf("a deleted tag reads as %v, want ErrTagNotFound", err)
	}
	if tags, err := f.r.Tags(ctx, alice); err != nil || len(tags) != 0 {
		t.Fatalf("after its one tag was deleted the repository lists %q (%v)", tags, err)
	}
	f.put(vcs.MainBranch, "doc", f.obj(8, "later"))
	later := f.commit(vcs.MainBranch, "later").Hash
	again, err := f.r.CreateTag(ctx, alice, "v1", later, "second")
	if err != nil {
		t.Fatalf("a deleted tag's name does not take a new tag: %v", err)
	}
	if got, err := f.r.Tag(ctx, alice, "v1"); err != nil || got.Target != later || got.Hash == first.Hash || got.Hash != again.Hash {
		t.Fatalf("the name's new tag reads as %+v (%v), want the one naming the later commit", got, err)
	}
}
