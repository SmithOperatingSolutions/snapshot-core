//go:build slow

package e2e_test

import (
	"bytes"
	"context"
	"io"
	"math/rand/v2"
	"path/filepath"
	"testing"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/auth"
	blobstore "github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/local"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/object"
	"github.com/SmithOperatingSolutions/snapshot-core/core/repo"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
	"github.com/SmithOperatingSolutions/snapshot-core/core/vcs"
	"github.com/SmithOperatingSolutions/snapshot-core/model/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/model/tree"
)

// randomStream is n incompressible bytes, the same for the same seed.
type randomStream struct {
	left int64
	r    *rand.ChaCha8
}

func newRandom(seed byte, n int64) *randomStream {
	return &randomStream{left: n, r: rand.NewChaCha8([32]byte{seed})}
}

func (s *randomStream) Read(p []byte) (int, error) {
	if s.left <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > s.left {
		p = p[:s.left]
	}
	s.r.Read(p)
	s.left -= int64(len(p))
	return len(p), nil
}

// textStream is n bytes of repeating text: what zstd compresses well. It
// copies from a page of the pattern, so the source itself is never what
// bounds the write being measured.
type textStream struct{ left, at int64 }

const textPattern = "the quick brown fox jumps over the lazy dog 0123456789\n"

var textPage = bytes.Repeat([]byte(textPattern), 1024)

func (s *textStream) Read(p []byte) (int, error) {
	if s.left <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > s.left {
		p = p[:s.left]
	}
	n := 0
	for n < len(p) { // from the page, in the stream's phase

		off := int((s.at + int64(n)) % int64(len(textPattern)))
		n += copy(p[n:], textPage[off:])
	}
	s.at += int64(len(p))
	s.left -= int64(len(p))
	return len(p), nil
}

// perfRepo is a repository on a local disk, driven the way a host drives
// one: write a file, put it in the namespace, commit.
type perfRepo struct {
	t  *testing.T
	r  *repo.Repo
	o  repo.Options
	me auth.Principal
}

func newPerfRepo(t *testing.T) *perfRepo {
	t.Helper()
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "store")
	bs, err := local.Create(dir, local.Options{})
	if err != nil {
		t.Fatal(err)
	}
	keys, err := seal.NewKeyring()
	if err != nil {
		t.Fatal(err)
	}
	models, err := model.NewRegistry(blob.Model{}, tree.Model{Config: repo.DefaultGeometry().Prolly()})
	if err != nil {
		t.Fatal(err)
	}
	me := auth.Principal{ID: "user:me"}
	o := repo.Options{Blobs: bs, Keys: keys, Registry: models, Authorizer: auth.AllowAll{}}
	r, err := repo.Init(ctx, me, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return &perfRepo{t: t, r: r, o: o, me: me}
}

// put writes rd as path and commits; it returns how long the write and
// the commit took.
func (p *perfRepo) put(path string, rd io.Reader) (write, commit time.Duration) {
	p.t.Helper()
	ctx := context.Background()
	t0 := time.Now()
	root, err := blob.Write(ctx, p.r.Chunks(), rd, p.r.Config.Geometry.Stream())
	if err != nil {
		p.t.Fatal(err)
	}
	write = time.Since(t0)
	t1 := time.Now()
	ws, err := p.r.WorkingSet(ctx, p.me, vcs.MainBranch)
	if err != nil {
		p.t.Fatal(err)
	}
	n, err := p.r.Namespace(ctx, ws.Working)
	if err != nil {
		p.t.Fatal(err)
	}
	e := n.Editor()
	if err := e.Put(path, object.Ref{Model: blob.ID, Root: root}); err != nil {
		p.t.Fatal(err)
	}
	if n, err = e.Flush(ctx); err != nil {
		p.t.Fatal(err)
	}
	if _, err := p.r.Commit(ctx, p.me, vcs.MainBranch, ws, n.Root(), "put "+path); err != nil {
		p.t.Fatal(err)
	}
	return write, time.Since(t1)
}

// read reads path back whole and returns its bytes' hash-checked length.
func (p *perfRepo) read(path string) (int64, time.Duration) {
	p.t.Helper()
	ctx := context.Background()
	head, err := p.r.Head(ctx, p.me, vcs.MainBranch)
	if err != nil {
		p.t.Fatal(err)
	}
	n, err := p.r.Namespace(ctx, head.Namespace)
	if err != nil {
		p.t.Fatal(err)
	}
	ref, _, ok, err := n.Get(ctx, path)
	if err != nil || !ok {
		p.t.Fatalf("%s: %v %v", path, ok, err)
	}
	t0 := time.Now()
	rd, err := blob.Open(ctx, p.r.Chunks(), ref.Root)
	if err != nil {
		p.t.Fatal(err)
	}
	got, err := io.Copy(io.Discard, rd)
	if err != nil {
		p.t.Fatal(err)
	}
	return got, time.Since(t0)
}

func mbs(n int64, d time.Duration) float64 { return float64(n) / 1e6 / d.Seconds() }

// #10: what a repository on a local disk does with a gibibyte, the way a
// host drives it. The figures are reported for the weekly run to track;
// what is asserted is what holds at any speed: the files read back whole
// and equal, and re-snapshotting a file that changed by one byte writes
// no more pack bytes than its changed chunks, their index nodes and the
// commit need.
func TestSlowThroughputOnLocalDisk(t *testing.T) {
	const size = 1 << 30
	random := make([]byte, size) // generated once: the source must not be what is measured
	if _, err := io.ReadFull(newRandom(1, size), random); err != nil {
		t.Fatal(err)
	}
	p := newPerfRepo(t)
	w, c := p.put("random.bin", bytes.NewReader(random))
	t.Logf("write 1 GiB random:          %v (%.0f MB/s), commit %v", w.Round(time.Millisecond), mbs(size, w), c.Round(time.Millisecond))
	w, c = p.put("text.log", &textStream{left: size})
	t.Logf("write 1 GiB compressible:    %v (%.0f MB/s), commit %v", w.Round(time.Millisecond), mbs(size, w), c.Round(time.Millisecond))

	packBytesBefore := packBytes(t, p.o.Blobs)
	changed := io.MultiReader(bytes.NewReader(random[:size/2]), bytes.NewReader([]byte("X")), bytes.NewReader(random[size/2:]))
	w, c = p.put("random.bin", changed)
	t.Logf("re-snapshot random +1 byte:  %v (%.0f MB/s), commit %v", w.Round(time.Millisecond), mbs(size, w), c.Round(time.Millisecond))
	if added := packBytes(t, p.o.Blobs) - packBytesBefore; added > 8<<20 {
		t.Fatalf("re-snapshotting a gibibyte that changed by one byte wrote %d bytes of packs, want under 8 MiB (the changed chunks, the index nodes above them, the commit): every unchanged chunk must deduplicate", added)
	}

	for path, want := range map[string]int64{"random.bin": size + 1, "text.log": size} {
		got, d := p.read(path)
		t.Logf("read %-11s %v (%.0f MB/s)", path+":", d.Round(time.Millisecond), mbs(got, d))
		if got != want {
			t.Fatalf("%s reads back as %d bytes, want %d", path, got, want)
		}
	}
	// The random file with one byte inserted: the second half must equal the
	// original stream's second half byte for byte, not just in length.
	ctx := context.Background()
	head, _ := p.r.Head(ctx, p.me, vcs.MainBranch)
	n, _ := p.r.Namespace(ctx, head.Namespace)
	ref, _, _, _ := n.Get(ctx, "random.bin")
	rd, err := blob.Open(ctx, p.r.Chunks(), ref.Root)
	if err != nil {
		t.Fatal(err)
	}
	if !sameBytes(t, rd, io.MultiReader(bytes.NewReader(random[:size/2]), bytes.NewReader([]byte("X")), bytes.NewReader(random[size/2:]))) {
		t.Fatal("the re-snapshotted random file does not read back as the bytes written")
	}
	t0 := time.Now()
	re, err := repo.Open(ctx, p.o)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("open the repository:         %v", time.Since(t0).Round(time.Millisecond))
	_ = re.Close()
}

func sameBytes(t *testing.T, a, b io.Reader) bool {
	t.Helper()
	ba, bb := make([]byte, 1<<20), make([]byte, 1<<20)
	for {
		na, ea := io.ReadFull(a, ba)
		nb, eb := io.ReadFull(b, bb)
		if na != nb || !bytes.Equal(ba[:na], bb[:nb]) {
			return false
		}
		if ea != nil || eb != nil {
			return ea == eb || (na == 0 && nb == 0)
		}
	}
}

// packBytes is the size of every pack in the store.
func packBytes(t *testing.T, bs blobstore.BlobStore) int64 {
	t.Helper()
	var n int64
	after := ""
	for {
		page, err := bs.List(context.Background(), "packs/", after, blobstore.MaxListPage)
		if err != nil {
			t.Fatal(err)
		}
		for _, info := range page {
			n += info.Size
		}
		if len(page) < blobstore.MaxListPage {
			return n
		}
		after = page[len(page)-1].Name
	}
}
