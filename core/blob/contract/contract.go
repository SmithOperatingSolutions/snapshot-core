// Package contract is the suite every blob.BlobStore must pass, unchanged
// (Storage Core Spec: "a shared blob/contract suite runs against every
// backend"; Engine Spec boundary rule 4). A backend that fails any of it is
// not safe to put a repository on.
package contract

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
)

// Factory returns a fresh, empty store. Cleanup is the factory's business
// (t.Cleanup).
type Factory func(t *testing.T) blob.BlobStore

// Options tunes the suite for a backend.
type Options struct {
	// Swappers is how many goroutines race each root swap (default 100, the
	// Engine Spec's number; S3 uses the Storage Core Spec's 50).
	Swappers int
	// Rounds is how many racing rounds to run (default 5).
	Rounds int
}

// Run runs the whole contract.
func Run(t *testing.T, newStore Factory, opts Options) {
	if opts.Swappers == 0 {
		opts.Swappers = 100
	}
	if opts.Rounds == 0 {
		opts.Rounds = 5
	}
	t.Run("PutGetRoundTrip", func(t *testing.T) { putGetRoundTrip(t, newStore(t)) })
	t.Run("MissingIsNotFound", func(t *testing.T) { missingIsNotFound(t, newStore(t)) })
	t.Run("RangeGet", func(t *testing.T) { rangeGet(t, newStore(t)) })
	t.Run("PutExistingRefused", func(t *testing.T) { putExistingRefused(t, newStore(t)) })
	t.Run("PutSizeMismatchLeavesNothing", func(t *testing.T) { putSizeMismatch(t, newStore(t)) })
	t.Run("PutIsAtomic", func(t *testing.T) { putIsAtomic(t, newStore(t)) })
	t.Run("InvalidNamesRefused", func(t *testing.T) { invalidNames(t, newStore(t)) })
	t.Run("Stat", func(t *testing.T) { stat(t, newStore(t)) })
	t.Run("ListPaging", func(t *testing.T) { listPaging(t, newStore(t)) })
	t.Run("ListLimits", func(t *testing.T) { listLimits(t, newStore(t)) })
	t.Run("Delete", func(t *testing.T) { deleteObject(t, newStore(t)) })
	t.Run("RootLifecycle", func(t *testing.T) { rootLifecycle(t, newStore(t)) })
	t.Run("StaleSwapRefused", func(t *testing.T) { staleSwapRefused(t, newStore(t)) })
	t.Run("VersionsNeverRepeat", func(t *testing.T) { versionsNeverRepeat(t, newStore(t)) })
	t.Run("RootValueLimits", func(t *testing.T) { rootValueLimits(t, newStore(t)) })
	t.Run("ConcurrentSwappersOneWinnerPerRound", func(t *testing.T) {
		concurrentSwappers(t, newStore(t), opts.Swappers, opts.Rounds)
	})
	t.Run("ConcurrentPutsOneWinner", func(t *testing.T) { concurrentPuts(t, newStore(t)) })
	t.Run("RootReadsDuringSwapsAreWhole", func(t *testing.T) { rootReadsDuringSwapsAreWhole(t, newStore(t)) })
}

var ctx = context.Background()

func payload(seed string, n int) []byte {
	out := make([]byte, 0, n+32)
	for i := 0; len(out) < n; i++ {
		h := sha256.Sum256([]byte(fmt.Sprintf("%s/%d", seed, i)))
		out = append(out, h[:]...)
	}
	return out[:n]
}

func put(t *testing.T, s blob.BlobStore, name string, b []byte) {
	t.Helper()
	if err := s.Put(ctx, name, bytes.NewReader(b), int64(len(b))); err != nil {
		t.Fatalf("Put(%q, %d bytes): %v", name, len(b), err)
	}
}

func get(t *testing.T, s blob.BlobStore, name string, off, n int64) ([]byte, error) {
	t.Helper()
	rc, err := s.Get(ctx, name, off, n)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

func mustGet(t *testing.T, s blob.BlobStore, name string) []byte {
	t.Helper()
	b, err := get(t, s, name, 0, -1)
	if err != nil {
		t.Fatalf("Get(%q): %v", name, err)
	}
	return b
}

func putGetRoundTrip(t *testing.T, s blob.BlobStore) {
	for _, n := range []int{0, 1, 1<<20 + 17} {
		name := fmt.Sprintf("objects/rt-%d", n)
		want := payload(name, n)
		put(t, s, name, want)
		if got := mustGet(t, s, name); !bytes.Equal(got, want) {
			t.Fatalf("%s: read back %d bytes that differ from the %d written: a stored pack would not "+
				"read back as written", name, len(got), len(want))
		}
	}
}

func missingIsNotFound(t *testing.T, s blob.BlobStore) {
	put(t, s, "present", []byte("x"))
	if _, err := get(t, s, "present", 0, -1); err != nil {
		t.Fatalf("positive control: %v", err)
	}
	if _, err := get(t, s, "absent", 0, -1); !errors.Is(err, blob.ErrNotFound) {
		t.Errorf("Get(absent) = %v, want ErrNotFound", err)
	}
	if _, err := s.Stat(ctx, "absent"); !errors.Is(err, blob.ErrNotFound) {
		t.Errorf("Stat(absent) = %v, want ErrNotFound", err)
	}
}

func rangeGet(t *testing.T, s blob.BlobStore) {
	data := payload("range", 1000)
	put(t, s, "r", data)
	for _, c := range []struct {
		off, n int64
		want   []byte
	}{
		{0, -1, data},
		{0, 10, data[:10]},
		{990, 10, data[990:]},
		{995, 10, data[995:]}, // cut at the end
		{500, -1, data[500:]},
		{1000, 0, nil},
		{1000, 5, nil}, // off == size yields nothing
		{1000, -1, nil},
		{7, 0, nil},
	} {
		got, err := get(t, s, "r", c.off, c.n)
		if err != nil {
			t.Errorf("Get(off=%d, n=%d): %v", c.off, c.n, err)
			continue
		}
		if !bytes.Equal(got, c.want) {
			t.Errorf("Get(off=%d, n=%d) = %d bytes, want %d: a frame read by range would be wrong",
				c.off, c.n, len(got), len(c.want))
		}
	}
	for _, c := range []struct{ off, n int64 }{{1001, 1}, {-1, 5}, {0, -2}} {
		if _, err := get(t, s, "r", c.off, c.n); !errors.Is(err, blob.ErrInvalidRange) {
			t.Errorf("Get(off=%d, n=%d) = %v, want ErrInvalidRange", c.off, c.n, err)
		}
	}
}

func putExistingRefused(t *testing.T, s blob.BlobStore) {
	put(t, s, "packs/one", []byte("original"))
	err := s.Put(ctx, "packs/one", bytes.NewReader([]byte("replacement")), 11)
	if !errors.Is(err, blob.ErrExists) {
		t.Fatalf("a second Put of the same name = %v, want ErrExists: immutable objects were overwritten", err)
	}
	if got := mustGet(t, s, "packs/one"); string(got) != "original" {
		t.Fatalf("after a refused Put the object reads %q, want %q", got, "original")
	}
}

// shortReader yields fewer bytes than declared, then EOF.
func putSizeMismatch(t *testing.T, s blob.BlobStore) {
	for name, c := range map[string]struct {
		body []byte
		size int64
	}{
		"short": {[]byte("abc"), 10},
		"long":  {[]byte("abcdefghijkl"), 10},
	} {
		obj := "mismatch-" + name
		if err := s.Put(ctx, obj, bytes.NewReader(c.body), c.size); !errors.Is(err, blob.ErrSizeMismatch) {
			t.Errorf("%s: Put of %d bytes declared as %d = %v, want ErrSizeMismatch", name, len(c.body), c.size, err)
		}
		if _, err := s.Stat(ctx, obj); !errors.Is(err, blob.ErrNotFound) {
			t.Errorf("%s: after a refused Put the name is visible (Stat err=%v): a torn object would block "+
				"its own retry and fail every read", name, err)
		}
		good := payload(obj, 10)
		if err := s.Put(ctx, obj, bytes.NewReader(good), 10); err != nil {
			t.Errorf("%s: a correct Put after a refused one failed: %v", name, err)
		}
	}
	if err := s.Put(ctx, "neg", bytes.NewReader(nil), -1); err == nil {
		t.Error("Put with a negative size was accepted")
	}
}

// gatedReader yields its first half, then blocks until released.
type gatedReader struct {
	data    []byte
	half    int
	off     int
	reached chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *gatedReader) Read(p []byte) (int, error) {
	if g.off >= len(g.data) {
		return 0, io.EOF
	}
	if g.off >= g.half {
		g.once.Do(func() { close(g.reached) })
		<-g.release
	}
	end := len(g.data)
	if g.off < g.half {
		end = g.half
	}
	n := copy(p, g.data[g.off:end])
	g.off += n
	return n, nil
}

// An object must never be observable half-written. The Put is held open
// halfway (the test establishes the state; it does not race for it).
func putIsAtomic(t *testing.T, s blob.BlobStore) {
	data := payload("atomic", 256<<10)
	g := &gatedReader{data: data, half: len(data) / 2, reached: make(chan struct{}), release: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- s.Put(ctx, "packs/atomic", g, int64(len(data))) }()
	select {
	case <-g.reached:
	case err := <-done:
		t.Fatalf("Put returned before its reader was drained: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("Put never read the first half of its body")
	}
	if _, err := s.Stat(ctx, "packs/atomic"); !errors.Is(err, blob.ErrNotFound) {
		t.Errorf("mid-Put, Stat = %v, want ErrNotFound: a reader could see a partial pack", err)
	}
	if b, err := get(t, s, "packs/atomic", 0, -1); !errors.Is(err, blob.ErrNotFound) {
		t.Errorf("mid-Put, Get returned %d bytes (err=%v), want ErrNotFound", len(b), err)
	}
	infos, err := s.List(ctx, "packs/", "", blob.MaxListPage)
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 0 {
		t.Errorf("mid-Put, List shows %v", infos)
	}
	close(g.release)
	if err := <-done; err != nil {
		t.Fatalf("Put: %v", err)
	}
	if got := mustGet(t, s, "packs/atomic"); !bytes.Equal(got, data) {
		t.Fatal("after the Put completed the object differs from what was written")
	}
}

var badNames = []string{
	"", "/", "/abs", "trail/", "a//b", ".", "..", "a/../b", "a/./b", ".hidden", "a/.hidden",
	"UPPER", "sp ace", "nul\x00", "tab\t", "unié", "back\\slash", "colon:",
	strings.Repeat("a", blob.MaxSegmentLen+1),
	strings.Repeat("a/", blob.MaxDepth) + "a",
	strings.Repeat(strings.Repeat("a", 100)+"/", 5) + strings.Repeat("a", 100), // > MaxNameLen
}

func invalidNames(t *testing.T, s blob.BlobStore) {
	put(t, s, "valid/name-1.x_y", []byte("ok")) // positive control
	for _, n := range badNames {
		if err := s.Put(ctx, n, bytes.NewReader([]byte("x")), 1); !errors.Is(err, blob.ErrInvalidName) {
			t.Errorf("Put(%q) = %v, want ErrInvalidName", n, err)
		}
		if _, err := get(t, s, n, 0, -1); !errors.Is(err, blob.ErrInvalidName) {
			t.Errorf("Get(%q) = %v, want ErrInvalidName", n, err)
		}
		if _, err := s.Stat(ctx, n); !errors.Is(err, blob.ErrInvalidName) {
			t.Errorf("Stat(%q) = %v, want ErrInvalidName", n, err)
		}
		if err := s.Delete(ctx, n); !errors.Is(err, blob.ErrInvalidName) {
			t.Errorf("Delete(%q) = %v, want ErrInvalidName", n, err)
		}
	}
	for _, p := range []string{"/abs", "a//", "..", "UP"} {
		if _, err := s.List(ctx, p, "", 10); !errors.Is(err, blob.ErrInvalidName) {
			t.Errorf("List(prefix %q) = %v, want ErrInvalidName", p, err)
		}
	}
}

func stat(t *testing.T, s blob.BlobStore) {
	before := time.Now().Add(-time.Minute)
	put(t, s, "s/obj", payload("stat", 777))
	info, err := s.Stat(ctx, "s/obj")
	if err != nil {
		t.Fatal(err)
	}
	if info.Name != "s/obj" || info.Size != 777 {
		t.Errorf("Stat = %+v, want name s/obj, size 777", info)
	}
	if info.ModTime.Before(before) || info.ModTime.After(time.Now().Add(time.Minute)) {
		t.Errorf("Stat ModTime %v is not the time of the Put: GC grace windows are measured from it", info.ModTime)
	}
}

// listNames are chosen so that walking directories in their own sorted order
// gives the wrong answer: "a-b" < "a/b" < "a0" in byte order ('-' < '/' < '0').
var listNames = []string{
	"a-b", "a/b", "a/b/c", "a/c", "a0", "b", "packs/00", "packs/01", "packs/0f/x",
	"packs/10", "packs/ff", "index/1", "index/2", "z/z/z",
}

func listAll(t *testing.T, s blob.BlobStore, prefix string, limit int) []string {
	t.Helper()
	var names []string
	after := ""
	for pages := 0; ; pages++ {
		if pages > 100 {
			t.Fatal("List never returned a short page")
		}
		infos, err := s.List(ctx, prefix, after, limit)
		if err != nil {
			t.Fatalf("List(%q, %q, %d): %v", prefix, after, limit, err)
		}
		if len(infos) > limit {
			t.Fatalf("List returned %d entries for limit %d", len(infos), limit)
		}
		for _, in := range infos {
			names = append(names, in.Name)
		}
		if len(infos) < limit {
			return names
		}
		after = infos[len(infos)-1].Name
	}
}

func listPaging(t *testing.T, s blob.BlobStore) {
	for _, n := range listNames {
		put(t, s, n, []byte(n))
	}
	want := append([]string(nil), listNames...)
	sort.Strings(want)
	for _, limit := range []int{1, 2, 3, 7, blob.MaxListPage} {
		if got := listAll(t, s, "", limit); strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("paging with limit %d gave\n  %v\nwant (byte order)\n  %v", limit, got, want)
		}
	}
	for prefix, wantP := range map[string][]string{
		"packs/":   {"packs/00", "packs/01", "packs/0f/x", "packs/10", "packs/ff"},
		"packs/0":  {"packs/00", "packs/01", "packs/0f/x"},
		"a":        {"a-b", "a/b", "a/b/c", "a/c", "a0"},
		"a/":       {"a/b", "a/b/c", "a/c"},
		"a/b":      {"a/b", "a/b/c"},
		"nothing/": nil,
	} {
		got := listAll(t, s, prefix, 2)
		if strings.Join(got, ",") != strings.Join(wantP, ",") {
			t.Errorf("List(prefix %q) = %v, want %v", prefix, got, wantP)
		}
	}
	// after is exclusive, and need not name an existing object.
	infos, err := s.List(ctx, "", "a/b", 3)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, in := range infos {
		got = append(got, in.Name)
	}
	if strings.Join(got, ",") != "a/b/c,a/c,a0" {
		t.Errorf("List(after a/b) = %v, want [a/b/c a/c a0]", got)
	}
	infos, err = s.List(ctx, "packs/", "packs/05", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 3 || infos[0].Name != "packs/0f/x" {
		t.Errorf("List(packs/, after packs/05) = %v, want packs/0f/x, packs/10, packs/ff", infos)
	}
	for _, in := range infos {
		if in.Size != int64(len(in.Name)) {
			t.Errorf("List reports %s as %d bytes, want %d", in.Name, in.Size, len(in.Name))
		}
	}
}

func listLimits(t *testing.T, s blob.BlobStore) {
	put(t, s, "x", []byte("x"))
	if _, err := s.List(ctx, "", "", blob.MaxListPage); err != nil {
		t.Fatalf("positive control: List at the page cap: %v", err)
	}
	for _, l := range []int{0, -1, blob.MaxListPage + 1} {
		if _, err := s.List(ctx, "", "", l); !errors.Is(err, blob.ErrInvalidLimit) {
			t.Errorf("List(limit %d) = %v, want ErrInvalidLimit — a silently clamped page looks like the last page", l, err)
		}
	}
}

func deleteObject(t *testing.T, s blob.BlobStore) {
	put(t, s, "d/1", []byte("one"))
	put(t, s, "d/2", []byte("two"))
	if err := s.Delete(ctx, "d/1"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Stat(ctx, "d/1"); !errors.Is(err, blob.ErrNotFound) {
		t.Errorf("after Delete, Stat = %v", err)
	}
	if got := mustGet(t, s, "d/2"); string(got) != "two" {
		t.Error("Delete removed the wrong object")
	}
	if err := s.Delete(ctx, "d/1"); err != nil {
		t.Errorf("deleting an absent object = %v, want nil (GC resumes idempotently)", err)
	}
	put(t, s, "d/1", []byte("again")) // a deleted name can be stored again
	if got := mustGet(t, s, "d/1"); string(got) != "again" {
		t.Errorf("re-stored object reads %q", got)
	}
}

func rootLifecycle(t *testing.T, s blob.BlobStore) {
	r, err := s.Root(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if r.Version != blob.NoVersion || len(r.Value) != 0 {
		t.Fatalf("a fresh store's root = %+v, want empty with NoVersion", r)
	}
	v1, err := s.SwapRoot(ctx, blob.NoVersion, []byte("first"))
	if err != nil {
		t.Fatalf("creating the root: %v", err)
	}
	if v1 == blob.NoVersion {
		t.Fatal("SwapRoot returned NoVersion for a created root")
	}
	if _, err := s.SwapRoot(ctx, blob.NoVersion, []byte("again")); !errors.Is(err, blob.ErrRootConflict) {
		t.Fatalf("creating a root that exists = %v, want ErrRootConflict", err)
	}
	v2, err := s.SwapRoot(ctx, v1, []byte("second"))
	if err != nil {
		t.Fatalf("swapping with the current version: %v", err)
	}
	r, err = s.Root(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(r.Value) != "second" || r.Version != v2 {
		t.Fatalf("Root = %q@%q, want second@%q", r.Value, r.Version, v2)
	}
}

func staleSwapRefused(t *testing.T, s blob.BlobStore) {
	v1, err := s.SwapRoot(ctx, blob.NoVersion, []byte("one"))
	if err != nil {
		t.Fatal(err)
	}
	v2, err := s.SwapRoot(ctx, v1, []byte("two"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.SwapRoot(ctx, v1, []byte("stale write")); !errors.Is(err, blob.ErrRootConflict) {
		t.Fatalf("a swap with a stale version = %v, want ErrRootConflict: a concurrent writer's update would be lost", err)
	}
	r, err := s.Root(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(r.Value) != "two" || r.Version != v2 {
		t.Fatalf("after a refused swap the root is %q@%q, want two@%q", r.Value, r.Version, v2)
	}
}

// Swapping the same bytes back must not resurrect an old version: a writer
// holding it would otherwise overwrite two updates it never saw.
func versionsNeverRepeat(t *testing.T, s blob.BlobStore) {
	seen := map[blob.Version]bool{}
	v, err := s.SwapRoot(ctx, blob.NoVersion, []byte("A"))
	if err != nil {
		t.Fatal(err)
	}
	first := v
	seen[v] = true
	for i := 0; i < 6; i++ {
		val := []byte("A")
		if i%2 == 0 {
			val = []byte("B")
		}
		if v, err = s.SwapRoot(ctx, v, val); err != nil {
			t.Fatal(err)
		}
		if seen[v] {
			t.Fatalf("version %q handed out twice (the root went A, B, A, ...): ABA", v)
		}
		seen[v] = true
	}
	if _, err := s.SwapRoot(ctx, first, []byte("from the past")); !errors.Is(err, blob.ErrRootConflict) {
		t.Fatalf("a swap with the first version succeeded after the value returned to its first bytes (err=%v)", err)
	}
}

func rootValueLimits(t *testing.T, s blob.BlobStore) {
	if _, err := s.SwapRoot(ctx, blob.NoVersion, nil); !errors.Is(err, blob.ErrEmptyRoot) {
		t.Errorf("an empty root value = %v, want ErrEmptyRoot (empty means \"no root\")", err)
	}
	if _, err := s.SwapRoot(ctx, blob.NoVersion, make([]byte, blob.MaxRootSize+1)); !errors.Is(err, blob.ErrTooLarge) {
		t.Errorf("a root over MaxRootSize = %v, want ErrTooLarge", err)
	}
	big := payload("root", blob.MaxRootSize)
	v, err := s.SwapRoot(ctx, blob.NoVersion, big)
	if err != nil {
		t.Fatalf("positive control: a root of exactly MaxRootSize: %v", err)
	}
	r, err := s.Root(ctx)
	if err != nil || !bytes.Equal(r.Value, big) || r.Version != v {
		t.Fatalf("the maximum-size root did not read back (err=%v)", err)
	}
}

// Every racer reads the same version and swaps; exactly one may win per round
// and the root must hold the winner's value (Storage Core Spec: "exactly one
// wins per round, none lost").
func concurrentSwappers(t *testing.T, s blob.BlobStore, racers, rounds int) {
	v, err := s.SwapRoot(ctx, blob.NoVersion, []byte("round-0"))
	if err != nil {
		t.Fatal(err)
	}
	for round := 1; round <= rounds; round++ {
		var (
			wg      sync.WaitGroup
			mu      sync.Mutex
			winners []int
			newV    blob.Version
			others  []error
			start   = make(chan struct{})
		)
		for i := 0; i < racers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				nv, err := s.SwapRoot(ctx, v, []byte(fmt.Sprintf("round-%d-racer-%d", round, i)))
				mu.Lock()
				defer mu.Unlock()
				switch {
				case err == nil:
					winners = append(winners, i)
					newV = nv
				case !errors.Is(err, blob.ErrRootConflict):
					others = append(others, err)
				}
			}(i)
		}
		close(start)
		wg.Wait()
		if len(others) > 0 {
			t.Fatalf("round %d: losers failed with something other than ErrRootConflict: %v", round, others[0])
		}
		if len(winners) != 1 {
			t.Fatalf("round %d: %d of %d racers won the same swap (want exactly 1): concurrent commits would "+
				"overwrite each other", round, len(winners), racers)
		}
		r, err := s.Root(ctx)
		if err != nil {
			t.Fatal(err)
		}
		want := fmt.Sprintf("round-%d-racer-%d", round, winners[0])
		if string(r.Value) != want || r.Version != newV {
			t.Fatalf("round %d: root holds %q, but the winner wrote %q", round, r.Value, want)
		}
		v = newV
	}
}

func concurrentPuts(t *testing.T, s blob.BlobStore) {
	const racers = 20
	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		wins   []int
		others []error
		start  = make(chan struct{})
	)
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := payload(fmt.Sprintf("racer-%d", i), 64<<10)
			<-start
			err := s.Put(ctx, "packs/contended", bytes.NewReader(body), int64(len(body)))
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				wins = append(wins, i)
			case !errors.Is(err, blob.ErrExists):
				others = append(others, err)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	if len(others) > 0 {
		t.Fatalf("losing Puts failed with something other than ErrExists: %v", others[0])
	}
	if len(wins) != 1 {
		t.Fatalf("%d concurrent Puts of one name succeeded, want exactly 1", len(wins))
	}
	if got := mustGet(t, s, "packs/contended"); !bytes.Equal(got, payload(fmt.Sprintf("racer-%d", wins[0]), 64<<10)) {
		t.Fatal("the stored object is not the winner's bytes: two writers' bytes were mixed")
	}
}

// A root read while swaps are in progress returns a whole root that some swap
// wrote, never a torn one: Root takes no lock, so only an atomic replace keeps
// its readers safe. Unlike PutIsAtomic this has to race (a swap's value is a
// byte slice, with no reader to hold open halfway); large values and readers
// in tight loops make a torn window, if there is one, near-certain to be hit.
func rootReadsDuringSwapsAreWhole(t *testing.T, s blob.BlobStore) {
	const swaps, readers, size = 50, 4, 256 << 10
	values := make([][]byte, swaps)
	written := make(map[string]bool, swaps)
	for i := range values {
		values[i] = payload(fmt.Sprintf("whole-root-%d", i), size)
		written[string(values[i])] = true
	}
	v, err := s.SwapRoot(ctx, blob.NoVersion, values[0])
	if err != nil {
		t.Fatal(err)
	}
	var (
		stop  = make(chan struct{})
		wg    sync.WaitGroup
		reads atomic.Int64
		bad   = make(chan string, readers)
	)
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				root, err := s.Root(ctx)
				switch {
				case err != nil:
					bad <- fmt.Sprintf("Root during a swap failed: %v", err)
					return
				case !written[string(root.Value)]:
					bad <- fmt.Sprintf("Root during a swap returned %d bytes that no swap wrote", len(root.Value))
					return
				}
				reads.Add(1)
			}
		}()
	}
	// However fast the backend, the readers get in between every two swaps.
	deadline := time.Now().Add(30 * time.Second)
	for i := 1; i < swaps && err == nil; i++ {
		v, err = s.SwapRoot(ctx, v, values[i])
		for next := reads.Load() + 1; reads.Load() < next && len(bad) == 0 && time.Now().Before(deadline); {
			runtime.Gosched()
		}
	}
	close(stop)
	wg.Wait()
	close(bad)
	if err != nil {
		t.Fatalf("swapping: %v", err)
	}
	for msg := range bad {
		t.Fatal(msg)
	}
	if n := reads.Load(); n < swaps {
		t.Fatalf("only %d root reads overlapped %d swaps: the readers never raced the writer", n, swaps)
	}
}
