package stream_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/memstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/stream"
)

// allocated is how many bytes f allocated, by the runtime's own count.
func allocated(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// metered is a memstore that counts the bytes its reads hand back.
type metered struct {
	*memstore.Store
	bytesRead atomic.Int64
}

func (m *metered) Get(ctx context.Context, h hash.Hash) ([]byte, error) {
	b, err := m.Store.Get(ctx, h)
	m.bytesRead.Add(int64(len(b)))
	return b, err
}

// repeated is a stream whose one level-1 index node lists the same data
// chunk n times: valid under content addressing, and n times as long as
// what it stores.
func repeated(t *testing.T, s chunk.Writer, data []byte, n int) stream.Ref {
	t.Helper()
	d := put(t, s, data)
	es := make([]ientry, n)
	for i := range es {
		es[i] = ientry{child: d, size: uint64(len(data))}
	}
	return stream.Ref{Root: put(t, s, encodeIndex(1, es)), Size: uint64(n * len(data)), Depth: 1}
}

// #23: one 4 KiB chunk listed 256 times is a 1 MiB stream stored in
// about 13 KB. A caller that will hold at most a byte less than that must
// be refused before anything is read, not handed the whole of it.
func TestRegression_SC23_ReadAllRefusesAStreamOverItsLimit(t *testing.T) {
	s := memstore.New()
	data := random("sc23 chunk", 4<<10)
	ref := repeated(t, s, data, 256)
	got, err := stream.ReadAll(ctx, s, ref, ref.Size) // positive control: exactly the limit
	if err != nil || !bytes.Equal(got, bytes.Repeat(data, 256)) {
		t.Fatalf("positive control: a %d-byte stream read with a %d-byte limit gave %d bytes, %v", ref.Size, ref.Size, len(got), err)
	}
	const budget = 64 << 10
	for _, limit := range []uint64{ref.Size - 1, 64 << 10} {
		var n int
		used := allocated(func() {
			var b []byte
			b, err = stream.ReadAll(ctx, s, ref, limit)
			n = len(b)
		})
		if !errors.Is(err, stream.ErrTooLarge) || used > budget {
			t.Errorf("a caller holding at most %d bytes asked for a %d-byte stream stored in about 13 KB: "+
				"handed %d bytes (%v), %d bytes allocated; want stream.ErrTooLarge within %d bytes", limit, ref.Size, n, err, used, budget)
		}
	}
}

// #23: a Ref's size is a claim until the bytes are read. A stream whose
// second entry claims 8 MiB over a 1 KiB chunk is corrupt, and finding
// that out must not cost the 8 MiB the claim names.
func TestRegression_SC23_ReadAllGrowsWithWhatItReads(t *testing.T) {
	s := memstore.New()
	a, b := random("sc23 first", 1<<10), random("sc23 second", 1<<10)
	ha, hb := put(t, s, a), put(t, s, b)
	honest := stream.Ref{Root: put(t, s, encodeIndex(1, []ientry{{ha, 1 << 10}, {hb, 1 << 10}})), Size: 2 << 10, Depth: 1}
	if got, err := stream.ReadAll(ctx, s, honest, honest.Size); err != nil || !bytes.Equal(got, append(bytes.Clone(a), b...)) {
		t.Fatalf("positive control: the honest two-chunk stream read as %d bytes, %v", len(got), err)
	}
	claim := uint64(8 << 20)
	forged := stream.Ref{Root: put(t, s, encodeIndex(1, []ientry{{ha, 1 << 10}, {hb, claim}})), Size: 1<<10 + claim, Depth: 1}
	var err error
	used := allocated(func() { _, err = stream.ReadAll(ctx, s, forged, forged.Size) })
	if budget := uint64(1 << 20); !errors.Is(err, chunk.ErrCorrupt) || used > budget {
		t.Fatalf("a 2 KiB stream whose index claims %d bytes: ReadAll = %v after allocating %d bytes; want ErrCorrupt within %d", forged.Size, err, used, budget)
	}
}

// #23: reading a stream reads each index node once for each place it
// stands in the tree, not once for every data chunk beneath it. Here a
// level-2 node lists one level-1 node 16 times, which lists one 1-byte
// chunk 128 times: 2,048 bytes whose index a per-chunk descent re-reads
// 2,048 times over.
func TestRegression_SC23_ASequentialReadReadsEachNodeOncePerPlace(t *testing.T) {
	s := &metered{Store: memstore.New()}
	d := put(t, s, []byte{0x5A})
	inner := make([]ientry, 128)
	for i := range inner {
		inner[i] = ientry{child: d, size: 1}
	}
	innerNode := encodeIndex(1, inner)
	x := put(t, s, innerNode)
	outer := make([]ientry, 16)
	for i := range outer {
		outer[i] = ientry{child: x, size: 128}
	}
	topNode := encodeIndex(2, outer)
	ref := stream.Ref{Root: put(t, s, topNode), Size: 2048, Depth: 2}
	got, err := stream.ReadAll(ctx, s, ref, ref.Size)
	if err != nil || !bytes.Equal(got, bytes.Repeat([]byte{0x5A}, 2048)) {
		t.Fatalf("the 2,048-byte stream read as %d bytes, %v", len(got), err)
	}
	// The top once, the inner node once per entry of the top, each byte once.
	once := int64(len(topNode) + 16*len(innerNode) + 2048)
	if read := s.bytesRead.Load(); read > 2*once {
		t.Fatalf("reading a 2,048-byte stream stored in %d bytes read %d bytes from the store; "+
			"each node once per place it stands is %d, want at most twice that", len(topNode)+len(innerNode)+1, read, once)
	}
}

// #40: a 100-byte object cost about 200 µs of CPU to write, most of it
// zeroing buffers sized for the geometry, not the stream (a 512 KiB chunk
// buffer and 64 KiB read buffers, twice over), and starting the parallel
// path's goroutines and read blocks for a stream of one chunk. A stream
// that says how long it is and is short allocates about what it holds;
// a thousand of them, written on four workers into a store that prepares
// chunks, allocate under 8 KiB apiece, and store what the serial path
// stores.
func TestRegression_SC40_AShortStreamAllocatesWhatItNeeds(t *testing.T) {
	const objects, size, budget = 1000, 100, 8 << 10
	cfg := stream.DefaultConfig()
	bodies := make([][]byte, objects)
	serial := make([]stream.Ref, objects)
	ref := newStore()
	cfg.Workers = 1
	for i := range bodies {
		bodies[i] = random(fmt.Sprintf("sc40/%d", i), size)
		serial[i] = write(t, ref, bodies[i], cfg)
	}
	s := &preparing{counting: newStore()}
	cfg.Workers = 4 // the parallel path, whatever this machine's cores
	refs := make([]stream.Ref, objects)
	var err error
	used := allocated(func() {
		for i, b := range bodies {
			if refs[i], err = stream.Write(ctx, s, bytes.NewReader(b), cfg); err != nil {
				return
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range refs {
		got, err := stream.ReadAll(ctx, s, r, size)
		if r != serial[i] || err != nil || !bytes.Equal(got, bodies[i]) {
			t.Fatalf("object %d: stored as %+v (the serial path: %+v), reads back %d bytes, %v", i, r, serial[i], len(got), err)
		}
	}
	if per := used / objects; per > budget {
		t.Fatalf("writing %d %d-byte objects allocated %d bytes apiece, want under %d: a small object pays for buffers sized to the geometry, or for the parallel path", objects, size, per, budget)
	}
	t.Logf("%d bytes allocated per %d-byte object", used/objects, size)
}
