package vcs_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/auth"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/memstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/object"
	"github.com/SmithOperatingSolutions/snapshot-core/core/prolly"
	"github.com/SmithOperatingSolutions/snapshot-core/core/vcs"
)

// asked records every path a principal is asked write on, and refuses
// bob writing under one prefix.
type asked struct {
	mu     sync.Mutex
	paths  []string
	prefix string
}

func (a *asked) Authorize(_ context.Context, p auth.Principal, act auth.Action, res string) error {
	if act != auth.Write || !strings.HasPrefix(res, "path:") {
		return nil
	}
	a.mu.Lock()
	a.paths = append(a.paths, strings.TrimPrefix(res, "path:"+vcs.MainBranch+":"))
	a.mu.Unlock()
	if a.prefix != "" && p.ID == bob.ID && strings.HasPrefix(res, a.prefix) {
		return errors.New("not under " + a.prefix)
	}
	return nil
}

func (a *asked) take() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := a.paths
	a.paths = nil
	slices.Sort(out)
	return slices.Compact(out)
}

// flushed edits the namespace at root: puts (a nil ref deletes), flushed
// once per batch.
func (f *fixture) flushed(root hash.Hash, batches ...map[string]*object.Ref) *object.Namespace {
	f.t.Helper()
	n, err := f.r.Namespace(ctx, root)
	if err != nil {
		f.t.Fatal(err)
	}
	e := n.Editor()
	for _, puts := range batches {
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
	}
	return n
}

func (f *fixture) diffPaths(from, to hash.Hash) []string {
	f.t.Helper()
	a, err := f.r.Namespace(ctx, from)
	if err != nil {
		f.t.Fatal(err)
	}
	b, err := f.r.Namespace(ctx, to)
	if err != nil {
		f.t.Fatal(err)
	}
	d, err := object.Diff(ctx, a, b)
	if err != nil {
		f.t.Fatal(err)
	}
	var out []string
	for {
		c, ok, err := d.Next()
		if err != nil {
			f.t.Fatal(err)
		}
		if !ok {
			return out
		}
		out = append(out, c.Path)
	}
}

// #43: CommitNamespace commits a namespace an editor flushed, and when the
// flush edited the namespace the branch stores, it asks for write on the
// paths the flush says it changed instead of diffing. It asks exactly what
// Commit asks, the paths a diff finds (the authority): a changed value, a
// delete and an add; not a put of the value already there or a delete of a
// path the namespace lacks.
func TestCommitNamespaceAsksWhatTheDiffFinds(t *testing.T) {
	f := newFixture(t)
	az := &asked{}
	o := f.o
	o.Authorizer = az
	r, err := vcs.Open(ctx, f.s, o)
	if err != nil {
		t.Fatal(err)
	}
	f.r = r
	main := vcs.MainBranch
	for _, p := range []string{"a", "b", "c"} {
		f.put(main, p, f.obj(8, "base "+p))
	}
	f.commit(main, "base")
	ws := f.ws(main)
	same, changed, added := f.obj(8, "base b"), f.obj(8, "new a"), f.obj(8, "d")
	n := f.flushed(ws.Working, map[string]*object.Ref{"a": &changed, "b": &same, "c": nil, "zz": nil, "d": &added})
	want := f.diffPaths(ws.Working, n.Root())
	if !slices.Equal(want, []string{"a", "c", "d"}) {
		t.Fatalf("fixture: the diff finds %v, want [a c d]", want)
	}
	az.take()
	if _, err := f.r.CommitNamespace(ctx, alice, main, ws, n, "edit"); err != nil {
		t.Fatalf("CommitNamespace: %v", err)
	}
	if got := az.take(); !slices.Equal(got, want) {
		t.Fatalf("committing an edit of a, b (same value), c (deleted), zz (absent) and d asked write on %v, want %v, what a diff finds", got, want)
	}
	if head := f.head(main); head.Namespace != n.Root() {
		t.Fatalf("the head names namespace %s, want the one committed, %s", head.Namespace.Short(), n.Root().Short())
	}
}

// What the flush says it changed stands for a diff only against the
// namespace it edited. Bob, kept out of secret/, is refused a
// CommitNamespace that changes secret/plan when the flush edited the stored
// namespace; when it edited another (the head, while alice's uncommitted
// edit to secret/plan stands in the working set), which bob's commit would
// drop; and when an earlier flush of the same editor made the change and
// the last flush did not. Each is ErrDenied and leaves the head, and the
// same shapes under public/ go through.
func TestCommitNamespaceAsksForEveryChangeTheFlushDidNotSee(t *testing.T) {
	cases := []struct {
		name string
		run  func(f *fixture, path string) error
	}{
		{"the flush's own change", func(f *fixture, path string) error {
			ws := f.ws(vcs.MainBranch)
			ref := f.obj(8, "bob")
			return f.commitNS(bob, ws, f.flushed(ws.Working, map[string]*object.Ref{path: &ref}))
		}},
		{"a change in the working set the flush did not start from", func(f *fixture, path string) error {
			f.putWorking(vcs.MainBranch, path, f.obj(8, "alice's edit"))
			ws := f.ws(vcs.MainBranch)
			ref := f.obj(8, "bob")
			return f.commitNS(bob, ws, f.flushed(f.head(vcs.MainBranch).Namespace, map[string]*object.Ref{"other": &ref}))
		}},
		{"an earlier flush's change", func(f *fixture, path string) error {
			ws := f.ws(vcs.MainBranch)
			first, second := f.obj(8, "bob"), f.obj(8, "bob again")
			return f.commitNS(bob, ws, f.flushed(ws.Working, map[string]*object.Ref{path: &first}, map[string]*object.Ref{"other": &second}))
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for _, path := range []string{"public/notes", "secret/plan"} {
				f := newFixture(t)
				o := f.o
				o.Authorizer = &asked{prefix: "path:main:secret/"}
				r, err := vcs.Open(ctx, f.s, o)
				if err != nil {
					t.Fatal(err)
				}
				f.r = r
				f.put(vcs.MainBranch, "public/notes", f.obj(8, "base"))
				f.put(vcs.MainBranch, "secret/plan", f.obj(8, "base"))
				f.commit(vcs.MainBranch, "base")
				head := f.head(vcs.MainBranch)
				err = c.run(f, path)
				if path == "public/notes" {
					if err != nil {
						t.Fatalf("positive control: %s at %s: %v", c.name, path, err)
					}
					continue
				}
				if !errors.Is(err, auth.ErrDenied) {
					t.Fatalf("bob committing %s at %s = %v, want ErrDenied", c.name, path, err)
				}
				if f.head(vcs.MainBranch).Hash != head.Hash {
					t.Fatal("a refused commit moved the head")
				}
			}
		})
	}
}

func (f *fixture) commitNS(p auth.Principal, prev vcs.WorkingSet, n *object.Namespace) error {
	_, err := f.r.CommitNamespace(ctx, p, vcs.MainBranch, prev, n, "c")
	return err
}

// reads records the hashes read.
type reads struct {
	chunk.Store
	mu  sync.Mutex
	got []hash.Hash
}

func (r *reads) Get(ctx context.Context, h hash.Hash) ([]byte, error) {
	r.mu.Lock()
	r.got = append(r.got, h)
	r.mu.Unlock()
	return r.Store.Get(ctx, h)
}

func (r *reads) take() []hash.Hash {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.got
	r.got = nil
	return out
}

// The point of CommitNamespace (#43): a host committing one edit of a
// namespace the branch stores pays for no diff. It reads no node of either
// namespace, where Commit of the same edit (the positive control, which
// shows the fixture can see such a read) reads nodes of both.
func TestCommitNamespaceReadsNoNamespaceNode(t *testing.T) {
	rs := &reads{Store: memstore.New()}
	f := newFixtureOn(t, rs)
	main := vcs.MainBranch
	puts := map[string]*object.Ref{}
	for i := range 2000 {
		ref := f.obj(8, fmt.Sprint(i))
		puts[fmt.Sprintf("dir/%05d", i)] = &ref
	}
	f.edit(alice, main, puts)
	f.commit(main, "base")
	nodes := func(root hash.Hash) map[hash.Hash]bool {
		out := map[hash.Hash]bool{}
		err := prolly.Walk(ctx, f.s, f.o.Config, root, func(h hash.Hash, _ bool) (bool, error) {
			out[h] = true
			return true, nil
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	for _, c := range []struct {
		name   string
		commit func(ws vcs.WorkingSet, n *object.Namespace) error
		reads  bool
	}{
		{"Commit", func(ws vcs.WorkingSet, n *object.Namespace) error {
			_, err := f.r.Commit(ctx, alice, main, ws, n.Root(), "c")
			return err
		}, true},
		{"CommitNamespace", func(ws vcs.WorkingSet, n *object.Namespace) error {
			_, err := f.r.CommitNamespace(ctx, alice, main, ws, n, "c")
			return err
		}, false},
	} {
		ws := f.ws(main)
		ref := f.obj(8, c.name)
		n := f.flushed(ws.Working, map[string]*object.Ref{"dir/01000": &ref})
		if n.Count() < 2000 {
			t.Fatalf("fixture: the namespace holds %d paths", n.Count())
		}
		from, to := nodes(ws.Working), nodes(n.Root())
		rs.take()
		if err := c.commit(ws, n); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		var hit int
		for _, h := range rs.take() {
			if from[h] || to[h] {
				hit++
			}
		}
		switch {
		case c.reads && hit == 0:
			t.Fatalf("positive control: %s read no namespace node; the fixture cannot see a diff", c.name)
		case !c.reads && hit > 0:
			t.Fatalf("committing one edit of the stored namespace through %s read %d namespace nodes: it diffed what the flush already knew", c.name, hit)
		}
	}
}
