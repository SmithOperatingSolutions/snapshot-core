package vcs_test

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/auth"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/memstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/object"
	"github.com/SmithOperatingSolutions/snapshot-core/core/vcs"
)

// stage makes n the working and staged namespace of main, as alice.
func (f *fixture) stage(n *object.Namespace) {
	f.t.Helper()
	ws := f.ws(vcs.MainBranch)
	next := ws
	next.Working, next.Staged = n.Root(), n.Root()
	if _, err := f.r.UpdateWorkingSet(ctx, alice, vcs.MainBranch, ws, next); err != nil {
		f.t.Fatal(err)
	}
}

// fenced reopens f's repository under an asked authorizer that keeps bob
// out of main's secret/, and seeds public/notes and secret/plan.
func (f *fixture) fencedBase() *asked {
	f.t.Helper()
	az := &asked{prefix: "path:main:secret/"}
	o := f.o
	o.Authorizer = az
	r, err := vcs.Open(ctx, f.s, o)
	if err != nil {
		f.t.Fatal(err)
	}
	f.r = r
	f.put(vcs.MainBranch, "public/notes", f.obj(8, "base"))
	f.put(vcs.MainBranch, "secret/plan", f.obj(8, "base"))
	f.put(vcs.MainBranch, "other", f.obj(8, "base"))
	f.commit(vcs.MainBranch, "base")
	return az
}

// #43: UpdateWorkingSetFlushed and CommitWorkingSetFlushed ask for write
// on exactly the paths a diff finds (the authority): a changed value, a
// delete and an add, and not a put of the stored value or a delete of an
// absent path, by the flush's record instead of diffing.
func TestTheFlushedWorkingSetCallsAskWhatTheDiffFinds(t *testing.T) {
	for _, call := range []string{"UpdateWorkingSetFlushed", "CommitWorkingSetFlushed"} {
		t.Run(call, func(t *testing.T) {
			f := newFixture(t)
			az := &asked{}
			o := f.o
			o.Authorizer = az
			r, err := vcs.Open(ctx, f.s, o)
			if err != nil {
				t.Fatal(err)
			}
			f.r = r
			for _, p := range []string{"a", "b", "c"} {
				f.put(vcs.MainBranch, p, f.obj(8, "base "+p))
			}
			f.commit(vcs.MainBranch, "base")
			ws := f.ws(vcs.MainBranch)
			same, changed, added := f.obj(8, "base b"), f.obj(8, "new a"), f.obj(8, "d")
			n := f.flushed(ws.Working, map[string]*object.Ref{"a": &changed, "b": &same, "c": nil, "zz": nil, "d": &added})
			want := f.diffPaths(ws.Working, n.Root())
			if !slices.Equal(want, []string{"a", "c", "d"}) {
				t.Fatalf("fixture: the diff finds %v, want [a c d]", want)
			}
			if call == "UpdateWorkingSetFlushed" {
				az.take()
				next := ws
				next.Working, next.Staged = n.Root(), n.Root()
				if _, err := f.r.UpdateWorkingSetFlushed(ctx, alice, vcs.MainBranch, ws, next, n); err != nil {
					t.Fatalf("%s: %v", call, err)
				}
			} else {
				f.stage(n)
				az.take()
				if _, err := f.r.CommitWorkingSetFlushed(ctx, alice, vcs.MainBranch, "c", n); err != nil {
					t.Fatalf("%s: %v", call, err)
				}
			}
			if got := az.take(); !slices.Equal(got, want) {
				t.Fatalf("%s of an edit of a, b (same value), c (deleted), zz (absent) and d asked write on %v, want %v, what a diff finds", call, got, want)
			}
		})
	}
}

// A flush's record stands for a diff only from the namespace it edited to
// the namespace it made. Bob, kept out of secret/, is refused (ErrDenied,
// nothing changed) where the record does not cover a change to
// secret/plan: his own change, asked by the record; a change the flush he
// hands over did not start from; and a record of a namespace other than
// the one the call stores. The same shapes under public/ go through.
func TestTheFlushedWorkingSetCallsAskForEveryChangeTheFlushDidNotSee(t *testing.T) {
	type scene func(f *fixture, path string) error
	update := func(f *fixture, target hash.Hash, record *object.Namespace) error {
		ws := f.ws(vcs.MainBranch)
		next := ws
		next.Working, next.Staged = target, target
		_, err := f.r.UpdateWorkingSetFlushed(ctx, bob, vcs.MainBranch, ws, next, record)
		return err
	}
	commitWS := func(f *fixture, record *object.Namespace) error {
		_, err := f.r.CommitWorkingSetFlushed(ctx, bob, vcs.MainBranch, "c", record)
		return err
	}
	ref := func(f *fixture, s string) *object.Ref { r := f.obj(8, s); return &r }
	cases := []struct {
		name string
		run  scene
	}{
		{"update: the flush's own change", func(f *fixture, path string) error {
			n := f.flushed(f.ws(vcs.MainBranch).Working, map[string]*object.Ref{path: ref(f, "bob")})
			return update(f, n.Root(), n)
		}},
		{"update: a working change the flush did not start from", func(f *fixture, path string) error {
			f.putWorking(vcs.MainBranch, path, f.obj(8, "alice's edit"))
			n := f.flushed(f.head(vcs.MainBranch).Namespace, map[string]*object.Ref{"other": ref(f, "bob")})
			return update(f, n.Root(), n)
		}},
		{"update: a record of another namespace", func(f *fixture, path string) error {
			from := f.ws(vcs.MainBranch).Working
			record := f.flushed(from, map[string]*object.Ref{"other": ref(f, "bob")})
			target := f.flushed(from, map[string]*object.Ref{path: ref(f, "bob")})
			return update(f, target.Root(), record)
		}},
		{"commit: the flush's own change", func(f *fixture, path string) error {
			n := f.flushed(f.ws(vcs.MainBranch).Working, map[string]*object.Ref{path: ref(f, "alice")})
			f.stage(n)
			return commitWS(f, n)
		}},
		{"commit: a staged change the flush did not start from", func(f *fixture, path string) error {
			first := f.flushed(f.ws(vcs.MainBranch).Working, map[string]*object.Ref{path: ref(f, "alice")})
			f.stage(first)
			second := f.flushed(first.Root(), map[string]*object.Ref{"other": ref(f, "alice")})
			f.stage(second)
			return commitWS(f, second)
		}},
		{"commit: a record of another namespace", func(f *fixture, path string) error {
			from := f.ws(vcs.MainBranch).Working
			f.stage(f.flushed(from, map[string]*object.Ref{path: ref(f, "alice")}))
			return commitWS(f, f.flushed(from, map[string]*object.Ref{"other": ref(f, "alice")}))
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for _, path := range []string{"public/notes", "secret/plan"} {
				f := newFixture(t)
				f.fencedBase()
				err := c.run(f, path)
				if path == "public/notes" {
					if err != nil {
						t.Fatalf("positive control: %s at %s: %v", c.name, path, err)
					}
					continue
				}
				if !errors.Is(err, auth.ErrDenied) {
					t.Fatalf("bob, %s at %s = %v, want ErrDenied", c.name, path, err)
				}
			}
		})
	}
}

// The point of the two calls (#43): handed the flush's record, a host
// pays for no diff. Neither reads a node of either namespace, where
// UpdateWorkingSet and CommitWorkingSet of the same edit (the positive
// controls, which show the fixture can see such a read) read nodes.
func TestTheFlushedWorkingSetCallsReadNoNamespaceNode(t *testing.T) {
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
	nodes := func(roots ...hash.Hash) map[hash.Hash]bool {
		out := map[hash.Hash]bool{}
		for _, root := range roots {
			for h := range walkNodes(t, f, root) {
				out[h] = true
			}
		}
		return out
	}
	for i, c := range []struct {
		name  string
		call  func(ws vcs.WorkingSet, n *object.Namespace) error
		reads bool
	}{
		{"UpdateWorkingSet", func(ws vcs.WorkingSet, n *object.Namespace) error {
			next := ws
			next.Working, next.Staged = n.Root(), n.Root()
			_, err := f.r.UpdateWorkingSet(ctx, alice, main, ws, next)
			return err
		}, true},
		{"UpdateWorkingSetFlushed", func(ws vcs.WorkingSet, n *object.Namespace) error {
			next := ws
			next.Working, next.Staged = n.Root(), n.Root()
			_, err := f.r.UpdateWorkingSetFlushed(ctx, alice, main, ws, next, n)
			return err
		}, false},
		{"CommitWorkingSet", func(_ vcs.WorkingSet, _ *object.Namespace) error {
			_, err := f.r.CommitWorkingSet(ctx, alice, main, "c")
			return err
		}, true},
		{"CommitWorkingSetFlushed", func(_ vcs.WorkingSet, n *object.Namespace) error {
			_, err := f.r.CommitWorkingSetFlushed(ctx, alice, main, "c", n)
			return err
		}, false},
	} {
		ws := f.ws(main)
		ref := f.obj(8, c.name)
		n := f.flushed(ws.Working, map[string]*object.Ref{fmt.Sprintf("dir/%05d", 500*i+1): &ref})
		if c.name == "CommitWorkingSet" || c.name == "CommitWorkingSetFlushed" {
			f.stage(n)
		}
		seen := nodes(ws.Working, n.Root())
		rs.take()
		if err := c.call(ws, n); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		var hit int
		for _, h := range rs.take() {
			if seen[h] {
				hit++
			}
		}
		switch {
		case c.reads && hit == 0:
			t.Fatalf("positive control: %s read no namespace node; the fixture cannot see a diff", c.name)
		case !c.reads && hit > 0:
			t.Fatalf("%s of one edit, handed the flush's record, read %d namespace nodes: it diffed what the flush already knew", c.name, hit)
		}
	}
}
