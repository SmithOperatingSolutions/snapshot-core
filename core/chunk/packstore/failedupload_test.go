package packstore_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/mem"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/packstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
)

// failingPacks refuses to store packs while failing is set.
type failingPacks struct {
	blob.BlobStore
	failing atomic.Bool
}

func (f *failingPacks) Put(ctx context.Context, name string, r io.Reader, size int64) error {
	if f.failing.Load() && strings.HasPrefix(name, "packs/") {
		return errors.New("injected: backend unavailable")
	}
	return f.BlobStore.Put(ctx, name, r, size)
}

// A pack whose upload failed keeps its chunks readable, blocks publication,
// and is uploaded by the next CAS once the backend recovers.
func TestFailedUploadStaysReadableAndIsRetried(t *testing.T) {
	fp := &failingPacks{BlobStore: mem.New()}
	kr := keyring(t)
	s, err := packstore.Open(ctx, packstore.WithBackoff(packstore.Options{Blobs: fp, Keys: kr, Repo: repo, PackSize: 32 << 10, CacheBytes: -1}, time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	fp.failing.Store(true)
	var hs []hash.Hash
	for i := 0; i < 12; i++ { // several packs' worth: rotation uploads, and fails
		h, err := s.Put(ctx, payload(fmt.Sprintf("offline-%d", i), 8000))
		if err != nil {
			t.Fatalf("Put while uploads fail: %v", err)
		}
		hs = append(hs, h)
	}
	for i, h := range hs {
		if got, err := s.Get(ctx, h); err != nil || !bytes.Equal(got, payload(fmt.Sprintf("offline-%d", i), 8000)) {
			t.Fatalf("chunk %d, in a pack whose upload failed, is unreadable: %v", i, err)
		}
	}
	if err := s.CompareAndSetRoot(ctx, hash.Hash{}, hs[0]); err == nil {
		t.Fatal("a CAS published a root while its packs could not be stored")
	}
	if r, _ := s.Root(ctx); !r.IsZero() {
		t.Fatalf("a failed CAS set the root to %s", r.Short())
	}
	fp.failing.Store(false)
	if err := s.CompareAndSetRoot(ctx, hash.Hash{}, hs[0]); err != nil {
		t.Fatalf("CAS after the backend recovered: %v", err)
	}
	fresh := open(t, fp, kr)
	for i, h := range hs {
		if _, err := fresh.Get(ctx, h); err != nil {
			t.Fatalf("after recovery, chunk %d does not reach a fresh store: %v", i, err)
		}
	}
}
