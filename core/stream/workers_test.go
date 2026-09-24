package stream_test

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/stream"
)

// preparing is a chunk.Preparer over the counting memstore: what a pack
// store is to a stream, without the packs. It counts what it prepared,
// what it stored prepared, what it stored by a plain Put and how many of
// those were data rather than index nodes, so a test can see the pipeline
// was taken and by every chunk; its failAt'th store fails when failAt is
// set, whichever way the store came.
type preparing struct {
	*counting
	prepared, viaWorkers, stores, plainData atomic.Int64
	failAt                                  int64
}

var errPut = errors.New("injected: the store refuses")

type prepped struct {
	h    hash.Hash
	data []byte
}

func (p prepped) Hash() hash.Hash { return p.h }
func (p prepped) Len() int        { return len(p.data) }

func (p *preparing) Prepare(data []byte) (chunk.Prepared, error) {
	p.prepared.Add(1)
	return prepped{hash.Sum(data), data}, nil
}

func (p *preparing) PutPrepared(ctx context.Context, q chunk.Prepared) (hash.Hash, error) {
	p.viaWorkers.Add(1)
	return p.store(ctx, q.(prepped).data)
}

func (p *preparing) Put(ctx context.Context, data []byte) (hash.Hash, error) {
	if len(data) == 0 || data[0] != 0x02 { // not an index node (DESIGN §7)
		p.plainData.Add(1)
	}
	return p.store(ctx, data)
}

func (p *preparing) store(ctx context.Context, data []byte) (hash.Hash, error) {
	if n := p.stores.Add(1); p.failAt > 0 && n >= p.failAt {
		return hash.Hash{}, errPut
	}
	return p.counting.Put(ctx, data)
}

// #10: a write on workers stores the serial path's chunks, in its order,
// under its root: written again on two workers and on every core into the
// store that holds the serial write, a stream adds no chunk and stores as
// many, every data chunk of it through a worker, once.
func TestWorkersWriteTheSerialPathsChunks(t *testing.T) {
	data := random("workers", 4<<20)
	s := &preparing{counting: newStore()}
	cfg := small()
	cfg.Workers = 1
	serial := write(t, s, data, cfg)
	if s.prepared.Load() != 0 {
		t.Fatalf("one worker prepared %d chunks: the serial path must not take the pipeline", s.prepared.Load())
	}
	stores := s.stores.Load()
	for _, workers := range []int{2, 0} {
		s.prepared.Store(0)
		s.viaWorkers.Store(0)
		s.stores.Store(0)
		s.plainData.Store(0)
		s.added.Store(0)
		cfg.Workers = workers
		ref := write(t, s, data, cfg)
		if ref != serial {
			t.Fatalf("%d workers: the stream is %+v, the serial path's is %+v: the pipeline changed the stream", workers, ref, serial)
		}
		if s.added.Load() != 0 {
			t.Fatalf("%d workers: the write added %d chunks to a store holding the serial write: the chunks differ", workers, s.added.Load())
		}
		if s.stores.Load() != stores {
			t.Fatalf("%d workers: the write stored %d chunks and nodes, the serial path %d", workers, s.stores.Load(), stores)
		}
		if s.prepared.Load() == 0 || s.viaWorkers.Load() != s.prepared.Load() || s.plainData.Load() != 0 {
			t.Fatalf("%d workers: prepared %d chunks, stored %d of them prepared and %d data chunks by a plain put: every data chunk goes through a worker, once", workers, s.prepared.Load(), s.viaWorkers.Load(), s.plainData.Load())
		}
	}
}

// A source that fails midway fails the write with the source's error, on
// the pipeline as on the serial path, and every goroutine the pipeline
// started is gone once it returns.
func TestAWorkersWriteSurfacesTheSourcesError(t *testing.T) {
	data := random("failing source", 2<<20)
	before := runtime.NumGoroutine()
	for _, workers := range []int{1, 0} {
		cfg := small()
		cfg.Workers = workers
		_, err := stream.Write(ctx, &preparing{counting: newStore()}, iotest.TimeoutReader(bytes.NewReader(data)), cfg)
		if !errors.Is(err, iotest.ErrTimeout) {
			t.Fatalf("%d workers: a source failing after its first read gave %v, want the source's error", workers, err)
		}
	}
	waitForGoroutines(t, before)
}

// A store that fails a put midway fails the write with the store's error,
// on the pipeline as on the serial path, stores nothing after it, and the
// pipeline winds down.
func TestAWorkersWriteSurfacesTheStoresError(t *testing.T) {
	data := random("failing store", 2<<20)
	before := runtime.NumGoroutine()
	for _, workers := range []int{1, 0} {
		cfg := small()
		cfg.Workers = workers
		s := &preparing{counting: newStore(), failAt: 5}
		_, err := stream.Write(ctx, s, bytes.NewReader(data), cfg)
		if !errors.Is(err, errPut) {
			t.Fatalf("%d workers: a store failing its fifth put gave %v, want the store's error", workers, err)
		}
		if s.stores.Load() != 5 {
			t.Fatalf("%d workers: after the fifth put failed the write went on to put %d chunks", workers, s.stores.Load())
		}
	}
	waitForGoroutines(t, before)
}

// waitForGoroutines waits for the goroutine count to fall back to before,
// as a pipeline winding down needs a moment after Write returns; it fails
// if it does not within two seconds.
func waitForGoroutines(t *testing.T, before int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			t.Fatalf("%d goroutines two seconds after the writes returned, %d before them: the pipeline leaks", runtime.NumGoroutine(), before)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
