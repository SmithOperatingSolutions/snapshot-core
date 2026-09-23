package packstore_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/local"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/mem"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/multivol"
	s3blob "github.com/SmithOperatingSolutions/snapshot-core/core/blob/s3"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/s3/s3fake"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/contract"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/packstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
)

var (
	ctx  = context.Background()
	repo = seal.RepoID{0xc0, 0xde}
)

func keyring(t testing.TB) *seal.Keyring {
	t.Helper()
	kr, err := seal.KeyringFromBytes(bytes.Repeat([]byte{0x5c}, 32))
	if err != nil {
		t.Fatal(err)
	}
	return kr
}

func open(t testing.TB, bs blob.BlobStore, kr *seal.Keyring) *packstore.Store {
	t.Helper()
	s, err := packstore.Open(ctx, packstore.WithBackoff(packstore.Options{Blobs: bs, Keys: kr, Repo: repo}, time.Millisecond))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// tamper rewrites the byte in the middle of a chunk's frame in its pack
// object, through the raw backend (packs are immutable, so: delete and put).
func tamper(t *testing.T, bs blob.BlobStore, s *packstore.Store, h hash.Hash) {
	t.Helper()
	name, off, n, ok := s.Location(h)
	if !ok {
		t.Fatalf("chunk %s has no location", h.Short())
	}
	rc, err := bs.Get(ctx, name, 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	b[off+n/2] ^= 1
	if err := bs.Delete(ctx, name); err != nil {
		t.Fatal(err)
	}
	if err := bs.Put(ctx, name, bytes.NewReader(b), int64(len(b))); err != nil {
		t.Fatal(err)
	}
}

func subject(t *testing.T, bs blob.BlobStore) contract.Subject {
	kr := keyring(t)
	s := open(t, bs, kr)
	return contract.Subject{
		Store:  s,
		Tamper: func(t *testing.T, h hash.Hash) { tamper(t, bs, s, h) },
		Reopen: func(t *testing.T) chunk.Store { return open(t, bs, kr) },
	}
}

func localStore(t *testing.T) blob.BlobStore {
	t.Helper()
	s, err := local.Create(filepath.Join(t.TempDir(), "store"), local.Options{})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// The chunk contract, unchanged, over every backend.
func TestContractOverEveryBackend(t *testing.T) {
	t.Run("mem", func(t *testing.T) {
		contract.Run(t, func(t *testing.T) contract.Subject { return subject(t, mem.New()) }, contract.Options{})
	})
	t.Run("local", func(t *testing.T) {
		contract.Run(t, func(t *testing.T) contract.Subject { return subject(t, localStore(t)) }, contract.Options{})
	})
	t.Run("multivol", func(t *testing.T) {
		contract.Run(t, func(t *testing.T) contract.Subject {
			root := t.TempDir()
			mv, err := multivol.Create(filepath.Join(root, "a"), []string{filepath.Join(root, "b"), filepath.Join(root, "c")}, multivol.Options{})
			if err != nil {
				t.Fatal(err)
			}
			return subject(t, mv)
		}, contract.Options{})
	})
	t.Run("s3", func(t *testing.T) {
		srv := s3fake.New()
		t.Cleanup(srv.Close)
		srv.CreateBucket("b")
		c := s3blob.NewClient(srv.URL(), "us-east-1", "k", "s")
		n := 0
		contract.Run(t, func(t *testing.T) contract.Subject {
			n++
			st, err := s3blob.Open(ctx, s3blob.Options{Client: c, Bucket: "b", Prefix: fmt.Sprintf("r%d/", n), AllowHTTP: true})
			if err != nil {
				t.Fatal(err)
			}
			return subject(t, st)
		}, contract.Options{Racers: 50})
	})
}

// Security table: no chunk hash and no chunk plaintext on disk, anywhere in
// the store (packs, index objects, manifest).
func TestNoPlaintextOnDisk(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	bs, err := local.Create(dir, local.Options{})
	if err != nil {
		t.Fatal(err)
	}
	s := open(t, bs, keyring(t))
	var hs []hash.Hash
	var datas [][]byte
	for i := 0; i < 20; i++ {
		d := payload(fmt.Sprintf("secret-%d", i), 3000)
		h, err := s.Put(ctx, d)
		if err != nil {
			t.Fatal(err)
		}
		hs, datas = append(hs, h), append(datas, d)
	}
	if err := s.CompareAndSetRoot(ctx, hash.Hash{}, hs[0]); err != nil {
		t.Fatal(err)
	}
	var files [][]byte
	err = filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			b, rerr := os.ReadFile(p)
			if rerr != nil {
				return rerr
			}
			files = append(files, b)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) < 3 {
		t.Fatalf("found %d files; a pack, an index object and the root must be on disk", len(files))
	}
	for i, h := range hs {
		for _, f := range files {
			if bytes.Contains(f, h[:]) {
				t.Fatalf("chunk hash %s is on disk in the clear", h.Short())
			}
			if bytes.Contains(f, datas[i][:32]) {
				t.Fatalf("chunk %d's plaintext is on disk", i)
			}
		}
	}
}

func payload(seed string, n int) []byte {
	out := make([]byte, 0, n+32)
	for i := 0; len(out) < n; i++ {
		h := sha256.Sum256([]byte(fmt.Sprintf("%s/%d", seed, i)))
		out = append(out, h[:]...)
	}
	return out[:n]
}

// Two stores on one backend (two processes): what one publishes, the other
// reads without reopening, and a stale CAS from the other is refused.
func TestWritersOnOneBackendSeeEachOther(t *testing.T) {
	bs := mem.New()
	kr := keyring(t)
	a, b := open(t, bs, kr), open(t, bs, kr)
	d := payload("shared", 5000)
	h, err := a.Put(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.CompareAndSetRoot(ctx, hash.Hash{}, h); err != nil {
		t.Fatal(err)
	}
	if got, err := b.Get(ctx, h); err != nil || !bytes.Equal(got, d) {
		t.Fatalf("the second store cannot read what the first published: %v", err)
	}
	if r, err := b.Root(ctx); err != nil || r != h {
		t.Fatalf("the second store's root = %s (%v), want %s", r.Short(), err, h.Short())
	}
	h2, _ := b.Put(ctx, []byte("b's"))
	if err := b.CompareAndSetRoot(ctx, hash.Hash{}, h2); !errors.Is(err, chunk.ErrRootConflict) {
		t.Fatalf("a CAS from a stale expected root = %v, want ErrRootConflict", err)
	}
	if err := b.CompareAndSetRoot(ctx, h, h2); err != nil {
		t.Fatalf("a CAS from the current root: %v", err)
	}
	if r, _ := a.Root(ctx); r != h2 {
		t.Fatalf("the first store sees root %s, want %s", r.Short(), h2.Short())
	}
}

// conflictingBlobs makes the next n root swaps fail as if another writer had
// swapped first, without changing anything: the manifest-level conflict S3
// answers with 412.
type conflictingBlobs struct {
	blob.BlobStore
	left  atomic.Int32
	swaps atomic.Int32
}

func (c *conflictingBlobs) SwapRoot(ctx context.Context, expected blob.Version, next []byte) (blob.Version, error) {
	c.swaps.Add(1)
	if c.left.Add(-1) >= 0 {
		return blob.NoVersion, blob.ErrRootConflict
	}
	return c.BlobStore.SwapRoot(ctx, expected, next)
}

// Engine Spec: "forced 412 on the manifest follows the retry path; after 10
// conflicts returns ErrRootConflict and the manifest is unchanged".
func TestManifestConflictsRetryThenGiveUp(t *testing.T) {
	cb := &conflictingBlobs{BlobStore: mem.New()}
	s := open(t, cb, keyring(t))
	h, _ := s.Put(ctx, []byte("retried"))
	cb.left.Store(3)
	if err := s.CompareAndSetRoot(ctx, hash.Hash{}, h); err != nil {
		t.Fatalf("a CAS whose first 3 swaps met manifest conflicts failed: %v", err)
	}
	if n := cb.swaps.Load(); n != 4 {
		t.Fatalf("the CAS made %d swap attempts, want 4 (3 conflicts, then success)", n)
	}
	before, err := cb.Root(ctx)
	if err != nil {
		t.Fatal(err)
	}
	h2, _ := s.Put(ctx, []byte("never lands"))
	cb.left.Store(1000)
	cb.swaps.Store(0)
	if err := s.CompareAndSetRoot(ctx, h, h2); !errors.Is(err, chunk.ErrRootConflict) {
		t.Fatalf("after endless manifest conflicts CAS = %v, want ErrRootConflict", err)
	}
	if n := cb.swaps.Load(); n != packstore.MaxSwapAttempts {
		t.Fatalf("the CAS made %d swap attempts, want exactly %d", n, packstore.MaxSwapAttempts)
	}
	after, _ := cb.Root(ctx)
	if !bytes.Equal(after.Value, before.Value) || after.Version != before.Version {
		t.Fatal("a CAS that gave up changed the manifest")
	}
	if r, _ := s.Root(ctx); r != h {
		t.Fatalf("root after giving up = %s, want %s", r.Short(), h.Short())
	}
}

// Engine Spec: "a single-row commit stays within its request budget".
func TestSmallCommitRequestBudget(t *testing.T) {
	srv := s3fake.New()
	t.Cleanup(srv.Close)
	srv.CreateBucket("b")
	st, err := s3blob.Open(ctx, s3blob.Options{Client: s3blob.NewClient(srv.URL(), "us-east-1", "k", "s"), Bucket: "b", AllowHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	s := open(t, st, keyring(t))
	h0, _ := s.Put(ctx, []byte("warm up"))
	if err := s.CompareAndSetRoot(ctx, hash.Hash{}, h0); err != nil {
		t.Fatal(err)
	}
	srv.ResetRequests()
	h1, _ := s.Put(ctx, []byte("one small row"))
	if err := s.CompareAndSetRoot(ctx, h0, h1); err != nil {
		t.Fatal(err)
	}
	got := srv.Requests()
	total := 0
	for _, n := range got {
		total += n
	}
	// One pack, one index object, one root swap.
	if got["PUT"] != 3 || total > 4 {
		t.Fatalf("a one-chunk commit cost %v (%d requests); want 3 PUTs (pack, index, root) and at most 4 in all", got, total)
	}
}

func TestManyPacksBeforeOneCommit(t *testing.T) {
	bs := mem.New()
	kr := keyring(t)
	s, err := packstore.Open(ctx, packstore.WithBackoff(packstore.Options{Blobs: bs, Keys: kr, Repo: repo, PackSize: 64 << 10}, time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	var hs []hash.Hash
	for i := 0; i < 60; i++ {
		h, err := s.Put(ctx, payload(fmt.Sprintf("rot-%d", i), 8000))
		if err != nil {
			t.Fatal(err)
		}
		hs = append(hs, h)
		if got, err := s.Get(ctx, h); err != nil || len(got) != 8000 {
			t.Fatalf("chunk %d unreadable before its commit: %v", i, err)
		}
	}
	if err := s.CompareAndSetRoot(ctx, hash.Hash{}, hs[len(hs)-1]); err != nil {
		t.Fatal(err)
	}
	packs, err := bs.List(ctx, "packs/", "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(packs) < 5 {
		t.Fatalf("480 KB in 64 KB packs made %d packs; rotation did not happen", len(packs))
	}
	re := open(t, bs, kr)
	for i, h := range hs {
		if got, err := re.Get(ctx, h); err != nil || !bytes.Equal(got, payload(fmt.Sprintf("rot-%d", i), 8000)) {
			t.Fatalf("after reopen chunk %d reads wrong (%v)", i, err)
		}
	}
}

func TestWrongKeyOrRepoCannotOpen(t *testing.T) {
	bs := mem.New()
	s := open(t, bs, keyring(t))
	h, _ := s.Put(ctx, []byte("x"))
	if err := s.CompareAndSetRoot(ctx, hash.Hash{}, h); err != nil {
		t.Fatal(err)
	}
	other, _ := seal.NewKeyring()
	if _, err := packstore.Open(ctx, packstore.Options{Blobs: bs, Keys: other, Repo: repo}); !errors.Is(err, packstore.ErrManifest) {
		t.Errorf("Open with another master key = %v, want ErrManifest", err)
	}
	if _, err := packstore.Open(ctx, packstore.Options{Blobs: bs, Keys: keyring(t), Repo: seal.RepoID{1}}); !errors.Is(err, packstore.ErrManifest) {
		t.Errorf("Open with another repo id = %v, want ErrManifest", err)
	}
	if _, err := packstore.Open(ctx, packstore.Options{Blobs: bs, Keys: keyring(t), Repo: repo}); err != nil {
		t.Fatalf("positive control: %v", err)
	}
}

// A root may only name a chunk that is durable: the chunks it reaches are
// uploaded before the manifest names them, so a crash never publishes a root
// whose chunks are gone. Checked by opening a second store right after CAS.
func TestPublishedChunksAreDurableBeforeTheRoot(t *testing.T) {
	bs := mem.New()
	kr := keyring(t)
	s := open(t, bs, kr)
	h, _ := s.Put(ctx, payload("durable", 20000))
	if err := s.CompareAndSetRoot(ctx, hash.Hash{}, h); err != nil {
		t.Fatal(err)
	}
	fresh := open(t, bs, kr)
	if _, err := fresh.Get(ctx, h); err != nil {
		t.Fatalf("a fresh store cannot read the published root chunk: %v", err)
	}
}

// --- crash harness at the chunk layer (Engine Spec L0: kill mid-write, 1,000 times) ---

const envCrash = "SNAPSHOT_PACKSTORE_CRASH_DIR"

func TestMain(m *testing.M) {
	if dir := os.Getenv(envCrash); dir != "" {
		crashChild(dir)
	}
	os.Exit(m.Run())
}

func crashChunk(n int) []byte {
	return []byte(fmt.Sprintf("crash-chunk-%08d-%s", n, strings.Repeat("z", n%500)))
}

func crashChild(dir string) {
	bs, err := local.Open(dir, local.Options{})
	if err != nil {
		fmt.Printf("error open %v\n", err)
		os.Exit(3)
	}
	s, err := packstore.Open(ctx, packstore.Options{Blobs: bs, Keys: keyring(&testing.T{}), Repo: repo})
	if err != nil {
		fmt.Printf("error open %v\n", err)
		os.Exit(3)
	}
	cur, _ := s.Root(ctx)
	start, _ := strconv.Atoi(os.Getenv("SNAPSHOT_PACKSTORE_CRASH_START"))
	for n := start; ; n++ {
		h, err := s.Put(ctx, crashChunk(n))
		if err != nil {
			fmt.Printf("error put %v\n", err)
			os.Exit(3)
		}
		fmt.Printf("try %d\n", n)
		if err := s.CompareAndSetRoot(ctx, cur, h); err != nil {
			fmt.Printf("error cas %v\n", err)
			os.Exit(3)
		}
		fmt.Printf("done %d\n", n)
		cur = h
	}
}

func crashIterations() int {
	if n, err := strconv.Atoi(os.Getenv("SNAPSHOT_CRASH_ITERATIONS")); err == nil && n > 0 {
		return n
	}
	return 20
}

func TestCrashDuringCommitLeavesOldOrNew(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	if _, err := local.Create(dir, local.Options{}); err != nil {
		t.Fatal(err)
	}
	kr := keyring(t)
	next, last := 0, -1
	for i := 0; i < crashIterations(); i++ {
		cmd := exec.Command(os.Args[0], "-test.run=^$")
		cmd.Env = append(os.Environ(), envCrash+"="+dir, "SNAPSHOT_PACKSTORE_CRASH_START="+strconv.Itoa(next))
		out, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		lines := make(chan string, 1024)
		go func() {
			sc := bufio.NewScanner(out)
			for sc.Scan() {
				lines <- sc.Text()
			}
			close(lines)
		}()
		tried, done := next-1, next-1
		record := func(l string) {
			f := strings.Fields(l)
			if len(f) == 2 {
				n, _ := strconv.Atoi(f[1])
				switch f[0] {
				case "try":
					tried = n
				case "done":
					done = n
				case "error":
					t.Errorf("child: %s", l)
				}
			}
		}
		select {
		case l, ok := <-lines:
			if !ok {
				_ = cmd.Wait()
				t.Fatal("the crash child exited before its loop")
			}
			record(l)
		case <-time.After(30 * time.Second):
			_ = cmd.Process.Kill()
			t.Fatal("the crash child never started")
		}
		// A commit is several fsyncs (pack, shard directory, index object, root
		// swap); the window must span a few, landing both inside and between them.
		time.Sleep(time.Duration(rand.IntN(250000)) * time.Microsecond)
		_ = cmd.Process.Kill()
		for l := range lines {
			record(l)
		}
		_ = cmd.Wait()

		bs, err := local.Open(dir, local.Options{})
		if err != nil {
			t.Fatalf("iteration %d: the store does not reopen: %v", i, err)
		}
		s, err := packstore.Open(ctx, packstore.Options{Blobs: bs, Keys: kr, Repo: repo})
		if err != nil {
			t.Fatalf("iteration %d: the chunk store does not reopen after kill -9: %v", i, err)
		}
		r, err := s.Root(ctx)
		if err != nil {
			t.Fatal(err)
		}
		ok := map[hash.Hash]int{}
		if done >= next {
			ok[hash.Sum(crashChunk(done))] = done
		} else if last >= 0 {
			ok[hash.Sum(crashChunk(last))] = last
		}
		if tried > done {
			ok[hash.Sum(crashChunk(tried))] = tried
		}
		n, good := ok[r]
		neverCommitted := last < 0 && done < next && r.IsZero()
		if !good && !neverCommitted {
			t.Fatalf("iteration %d: after kill -9 the root is %s, neither the last committed nor the one in flight", i, r.Short())
		}
		if good {
			if got, err := s.Get(ctx, r); err != nil || !bytes.Equal(got, crashChunk(n)) {
				t.Fatalf("iteration %d: the root survived but its chunk did not (%v): a root named a chunk that was not durable", i, err)
			}
			last = n
		}
		_ = s.Close()
		next = max(tried, done) + 1
	}
	if last < 0 {
		t.Fatal("no commit completed; the harness exercised nothing")
	}
}
