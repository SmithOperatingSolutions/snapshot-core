package packstore_test

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/mem"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
)

// pairedPuts is a blob store whose pack puts wait until an index object's
// put has begun: a publish that writes the two one after the other never
// finishes; one that writes them together does.
type pairedPuts struct {
	blob.BlobStore
	once         sync.Once
	indexStarted chan struct{}
}

func newPaired(bs blob.BlobStore) *pairedPuts {
	return &pairedPuts{BlobStore: bs, indexStarted: make(chan struct{})}
}

func (p *pairedPuts) Put(ctx context.Context, name string, r io.Reader, size int64) error {
	switch {
	case strings.HasPrefix(name, "index/"):
		p.once.Do(func() { close(p.indexStarted) })
	case strings.HasPrefix(name, "packs/"):
		select {
		case <-p.indexStarted:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return p.BlobStore.Put(ctx, name, r, size)
}

// #10: a publish writes the pack it finished and the index object that
// lists it together, since each waits on the backend and neither on the
// other; the root is swapped once both have landed. A backend that lets
// the pack land only once the index object's write has begun sees the
// publish through.
func TestAPublishWritesItsPackAndIndexObjectTogether(t *testing.T) {
	pp := newPaired(mem.New())
	s := smallPacks(t, pp)
	chunks, hs := chunksOf("together", 4) // under one pack: finished at the publish
	if err := <-putAll(s, chunks); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.CompareAndSetRoot(ctx, hash.Hash{}, hs[0]); err != nil {
		t.Fatalf("a publish whose pack may land only once its index object's write has begun: %v; the two must be written together", err)
	}
	fresh := open(t, pp, keyring(t))
	for _, h := range hs {
		if _, err := fresh.Get(ctx, h); err != nil {
			t.Fatalf("after the publish a fresh store cannot read %s: %v", h.Short(), err)
		}
	}
}
