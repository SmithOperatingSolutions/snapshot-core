package e2e_test

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	blobstore "github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/mem"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/packstore"
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

// #10: when a stream's write ends, the pack holding its last chunks is
// finished and uploaded then, beside the host's next work, not at the
// publish: a small file's pack reaches the backend before anything is
// committed.
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
	if _, err := blob.Write(ctx, s, bytes.NewReader(sampleBytes(100<<10)), repo.DefaultGeometry().Stream()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for pp.count() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("five seconds after a 100 KiB stream was written no pack has reached the backend: the last pack waits for the publish")
		}
		time.Sleep(time.Millisecond)
	}
}
