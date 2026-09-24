package e2e_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	blobstore "github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/mem"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/packstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/repo"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
	"github.com/SmithOperatingSolutions/snapshot-core/model/blob"
)

// packPuts records when pack puts reach the backend.
type packPuts struct {
	blobstore.BlobStore
	mu    sync.Mutex
	names []string
}

func (p *packPuts) Put(ctx context.Context, name string, r io.Reader, size int64) error {
	if strings.HasPrefix(name, "packs/") {
		p.mu.Lock()
		p.names = append(p.names, name)
		p.mu.Unlock()
	}
	return p.BlobStore.Put(ctx, name, r, size)
}

func (p *packPuts) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.names)
}

// #10: when a stream's write ends and the pack holding its last chunks is
// worth an upload of its own, at least an eighth of a pack, it is finished
// and uploaded then, beside the host's next work, not at the publish: a
// 5 MiB file's pack reaches the backend before anything is committed. A
// small stream's chunks wait for the publish and share its pack with what
// comes next: two 100 KiB files and the publish make one pack, not three.
func TestAStreamsLastPackIsUploadedWhenTheStreamEnds(t *testing.T) {
	ctx := context.Background()
	pp := &packPuts{BlobStore: mem.New()}
	kr, err := seal.NewKeyring()
	if err != nil {
		t.Fatal(err)
	}
	s, err := packstore.Open(ctx, packstore.Options{Blobs: pp, Keys: kr, Repo: seal.RepoID{0x0f}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	cfg := repo.DefaultGeometry().Stream()
	big, err := blob.Write(ctx, s, bytes.NewReader(sampleBytes(5<<20)), cfg)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for pp.count() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("five seconds after a 5 MiB stream was written no pack has reached the backend: the last pack waits for the publish")
		}
		time.Sleep(time.Millisecond)
	}
	if err := s.CompareAndSetRoot(ctx, hash.Hash{}, big.Hash); err != nil {
		t.Fatal(err)
	}
	before := pp.count()
	for _, seed := range []string{"small one", "small two"} {
		if _, err := blob.Write(ctx, s, bytes.NewReader(sampleBytesSeeded(seed, 100<<10)), cfg); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.CompareAndSetRoot(ctx, big.Hash, big.Hash); err != nil {
		t.Fatal(err)
	}
	if got := pp.count() - before; got != 1 {
		t.Fatalf("two 100 KiB streams and their publish wrote %d packs, want one: a pack under an eighth of the pack size waits for the publish", got)
	}
}

// sampleBytesSeeded is sampleBytes with its own seed.
func sampleBytesSeeded(seed string, n int) []byte {
	out := make([]byte, 0, n+sha256.Size)
	var ctr [8]byte
	for i := uint64(0); len(out) < n; i++ {
		binary.BigEndian.PutUint64(ctr[:], i)
		h := sha256.Sum256(append([]byte(seed), ctr[:]...))
		out = append(out, h[:]...)
	}
	return out[:n]
}
