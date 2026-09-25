// Package stream stores byte sequences of any length as content-defined
// chunks under a content-defined index tree (docs/DESIGN.md §7): data cut by
// core/cdc, index nodes split by core/boundary. Equal bytes are an equal
// stream, and an edit rewrites only the chunks around it.
//
// Reads verify as they go: every chunk by SHA-256 (the chunk store), and at
// every index node on the way down, its level and that its entries add up to
// what its parent (or the Ref) claims; every data chunk must be exactly as
// long as its entry says.
package stream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"runtime"

	"github.com/SmithOperatingSolutions/snapshot-core/core/boundary"
	"github.com/SmithOperatingSolutions/snapshot-core/core/cdc"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/wire"
)

const (
	kindIndex = 0x02
	maxDepth  = 63
	entryMin  = hash.Size + 1 // the smallest encoded entry
)

// Ref names a stream: its top chunk, its length, and how many index levels
// stand above the data (0: Root is the only data chunk).
type Ref struct {
	Root  hash.Hash
	Size  uint64
	Depth uint8
}

// Config is the repo geometry a stream is written with.
type Config struct {
	CDC   cdc.Geometry      // how data is cut
	Nodes boundary.Geometry // how index nodes are split
	// Workers hash and compress chunks in parallel while one goroutine cuts
	// and one appends, in stream order (#10): 0 is runtime.GOMAXPROCS(0),
	// 1 is the serial path. Chunks and hashes are the same at any count.
	Workers int
}

// DefaultConfig is the default repo geometry.
func DefaultConfig() Config {
	return Config{CDC: cdc.DefaultGeometry(), Nodes: boundary.DefaultGeometry()}
}

func corrupt(format string, args ...any) error {
	return fmt.Errorf("%w: stream: %s", chunk.ErrCorrupt, fmt.Sprintf(format, args...))
}

// Write stores everything r yields and returns its Ref.
func Write(ctx context.Context, w chunk.Writer, r io.Reader, c Config) (Ref, error) {
	rule, err := boundary.New(c.Nodes)
	if err != nil {
		return Ref{}, err
	}
	cut, err := cdc.New(r, c.CDC)
	if err != nil {
		return Ref{}, err
	}
	b := &builder{ctx: ctx, w: w, rule: rule}
	defer func() { // the stream is over: what it left pending may be stored now (#10)
		if f, ok := w.(chunk.Flusher); ok {
			_ = f.Flush(ctx) // a failure to start storing surfaces at the publish
		}
	}()
	workers := c.Workers
	if workers == 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	if pw, ok := w.(chunk.Preparer); ok && workers > 1 {
		stop := make(chan struct{}) // the write is over: every goroutine winds down
		defer close(stop)
		par, err := cdc.NewParallel(r, c.CDC, workers)
		if err != nil {
			return Ref{}, err
		}
		defer par.Close()
		return b.parallel(par, pw, workers, stop)
	}
	for {
		data, err := cut.Next()
		if errors.Is(err, io.EOF) {
			return b.finish()
		}
		if err != nil {
			return Ref{}, err
		}
		h, err := w.Put(ctx, data)
		if err != nil {
			return Ref{}, err
		}
		if err := b.add(1, entry{child: h, size: uint64(len(data))}); err != nil {
			return Ref{}, err
		}
	}
}

// job is one chunk on its way through the workers: cut, then prepared,
// then stored in stream order.
type job struct {
	data []byte
	p    chunk.Prepared
	err  error
	done chan struct{}
}

// cutter is what parallel cuts with: cdc.Parallel, whose own goroutines
// read the stream and hash it (cdc.Chunker in tests).
type cutter interface {
	Next() ([]byte, error)
}

// parallel is Write with the chunks hashed and compressed on workers
// goroutines (#10): the cutter's goroutines read and hash the stream and
// its caller places the cuts, the workers prepare, and this goroutine
// stores each prepared chunk in stream order and grows the index. At most
// 2*workers chunks and a few read blocks are in flight, so memory is
// bounded by about 2*workers*MaxChunkSize whatever the stream's size. The stream is the same as the serial path's: the
// cutter is the same and stores happen in its order, so every chunk, hash
// and node is identical. stop, closed by the caller when the write is
// over, winds every goroutine down.
func (b *builder) parallel(cut cutter, w chunk.Preparer, workers int, stop chan struct{}) (Ref, error) {
	work := make(chan *job, workers) // cut, to be prepared
	order := make(chan *job, workers)
	go func() {
		defer close(work)
		defer close(order)
		for {
			data, err := cut.Next()
			j := &job{data: data, err: err, done: make(chan struct{})}
			if err != nil {
				close(j.done) // nothing to prepare; the storer sees the error, or EOF
				select {
				case order <- j:
				case <-stop:
				}
				return
			}
			select {
			case order <- j:
			case <-stop:
				return
			}
			select {
			case work <- j:
			case <-stop:
				return
			}
		}
	}()
	for range workers {
		go func() {
			for j := range work {
				j.p, j.err = w.Prepare(j.data)
				close(j.done)
			}
		}()
	}
	for j := range order {
		<-j.done
		if errors.Is(j.err, io.EOF) {
			return b.finish()
		}
		if j.err != nil {
			return Ref{}, j.err
		}
		h, err := w.PutPrepared(b.ctx, j.p)
		if err != nil {
			return Ref{}, err
		}
		if err := b.add(1, entry{child: h, size: uint64(len(j.data))}); err != nil {
			return Ref{}, err
		}
	}
	return Ref{}, errors.New("stream: the cutter stopped without an end")
}

// builder grows the index tree one entry at a time: each level holds the
// node it is filling, and a finished node becomes an entry one level up.
type builder struct {
	ctx    context.Context
	w      chunk.Writer
	rule   boundary.Rule
	levels []*level // levels[i] is index level i+1
}

type level struct {
	split   *boundary.Splitter
	entries []entry // the node being filled
	total   int     // entries this level has taken
	first   entry
}

func (b *builder) add(lv int, e entry) error {
	if lv > maxDepth {
		return fmt.Errorf("stream: deeper than %d levels", maxDepth)
	}
	if len(b.levels) < lv {
		b.levels = append(b.levels, &level{split: b.rule.Splitter(lv)})
	}
	l := b.levels[lv-1]
	if l.total == 0 {
		l.first = e
	}
	l.total++
	l.entries = append(l.entries, e)
	if !l.split.Append(e.child, entrySize(e)) {
		return nil
	}
	return b.emit(lv)
}

// emit stores the node level lv is filling and hands it up a level.
func (b *builder) emit(lv int) error {
	l := b.levels[lv-1]
	h, err := b.w.Put(b.ctx, encodeIndex(lv, l.entries))
	if err != nil {
		return err
	}
	var size uint64
	for _, e := range l.entries {
		size += e.size
	}
	l.entries = l.entries[:0]
	return b.add(lv+1, entry{child: h, size: size})
}

// finish closes each level's last node, bottom up, until a level has taken
// a single entry: that entry's child is the root, so no top node has one child.
func (b *builder) finish() (Ref, error) {
	if len(b.levels) == 0 { // no data at all: the empty stream is the empty chunk
		h, err := b.w.Put(b.ctx, nil)
		return Ref{Root: h}, err
	}
	for lv := 1; ; lv++ {
		l := b.levels[lv-1]
		if l.total == 1 {
			return Ref{Root: l.first.child, Size: l.first.size, Depth: uint8(lv - 1)}, nil
		}
		if len(l.entries) > 0 {
			if err := b.emit(lv); err != nil {
				return Ref{}, err
			}
		}
	}
}

type entry struct {
	child hash.Hash
	size  uint64
}

func entrySize(e entry) int { return hash.Size + uvarintLen(e.size) }

func uvarintLen(v uint64) int { return (bits.Len64(v|1) + 6) / 7 }

// Index node: 0x02 · level u8 · count uvarint · (child [32] · size uvarint) × count.
func encodeIndex(level int, es []entry) []byte {
	var w wire.Writer
	w.U8(kindIndex)
	w.U8(uint8(level))
	w.Uvarint(uint64(len(es)))
	for _, e := range es {
		w.Raw(e.child[:])
		w.Uvarint(e.size)
	}
	return w.Bytes()
}

// decodeIndex parses an index node: level 1..63, at least one entry, every
// size at least 1 and the sizes' sum inside 64 bits, nothing after the last.
func decodeIndex(b []byte) (int, []entry, error) {
	r := wire.NewReader(b)
	kind, level, n := r.U8(), r.U8(), r.Uvarint()
	if r.Err() != nil || kind != kindIndex || level < 1 || level > maxDepth || n == 0 || n > uint64(len(b)/entryMin) {
		return 0, nil, corrupt("index node header")
	}
	es := make([]entry, 0, n)
	var sum, carry uint64
	for i := uint64(0); i < n; i++ {
		var e entry
		copy(e.child[:], r.Fixed(hash.Size))
		e.size = r.Uvarint()
		sum, carry = bits.Add64(sum, e.size, 0)
		if r.Err() != nil || e.size == 0 || carry != 0 {
			return 0, nil, corrupt("index entry %d", i)
		}
		es = append(es, e)
	}
	if r.Done() != nil {
		return 0, nil, corrupt("index node: bytes past the last entry")
	}
	return int(level), es, nil
}

// Reader reads a stream at any offset, verifying what it reads.
type Reader struct {
	ctx context.Context
	rd  chunk.Reader
	ref Ref
	off int64 // for Read

	cur      []byte // the data chunk read last
	curStart int64
}

// Open checks ref's root and returns a Reader; ctx serves its reads.
func Open(ctx context.Context, rd chunk.Reader, ref Ref) (*Reader, error) {
	if ref.Depth > maxDepth || ref.Size > math.MaxInt64 {
		return nil, corrupt("ref: depth %d, size %d", ref.Depth, ref.Size)
	}
	r := &Reader{ctx: ctx, rd: rd, ref: ref}
	if ref.Size == 0 { // the empty stream: its chunk must be the empty chunk
		b, err := rd.Get(ctx, ref.Root)
		if err != nil {
			return nil, err
		}
		if ref.Depth != 0 || len(b) != 0 {
			return nil, corrupt("an empty stream must be the empty chunk")
		}
		return r, nil
	}
	if _, _, err := r.chunkAt(0); err != nil {
		return nil, err
	}
	return r, nil
}

// Size is the stream's length.
func (r *Reader) Size() int64 { return int64(r.ref.Size) }

// chunkAt returns the data chunk holding byte pos and where it starts,
// checking every node on the way down.
func (r *Reader) chunkAt(pos int64) ([]byte, int64, error) {
	if r.cur != nil && pos >= r.curStart && pos < r.curStart+int64(len(r.cur)) {
		return r.cur, r.curStart, nil
	}
	h, want, start := r.ref.Root, r.ref.Size, int64(0)
	for depth := int(r.ref.Depth); ; depth-- {
		b, err := r.rd.Get(r.ctx, h)
		if err != nil {
			return nil, 0, err
		}
		if depth == 0 {
			if uint64(len(b)) != want {
				return nil, 0, corrupt("a data chunk is %d bytes, its entry says %d", len(b), want)
			}
			r.cur, r.curStart = b, start
			return b, start, nil
		}
		level, es, err := decodeIndex(b)
		if err != nil {
			return nil, 0, err
		}
		if level != depth {
			return nil, 0, corrupt("an index node at depth %d is level %d", depth, level)
		}
		if depth == int(r.ref.Depth) && len(es) < 2 {
			return nil, 0, corrupt("the top index node has one child")
		}
		var sum uint64
		next := -1
		for i, e := range es {
			if next < 0 && uint64(pos-start) < sum+e.size {
				next = i
			}
			sum += e.size
		}
		if sum != want {
			return nil, 0, corrupt("a level-%d node's entries add to %d bytes, its parent says %d", level, sum, want)
		}
		// pos-start < want = sum (callers stay under the Ref's size, and each
		// level's sum is its parent's entry), so some entry holds pos.
		for _, e := range es[:next] {
			start += int64(e.size)
		}
		h, want = es[next].child, es[next].size
	}
}

// ReadAt implements io.ReaderAt.
func (r *Reader) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("stream: negative offset")
	}
	n := 0
	for n < len(p) && off+int64(n) < r.Size() {
		pos := off + int64(n)
		c, start, err := r.chunkAt(pos)
		if err != nil {
			return n, err
		}
		n += copy(p[n:], c[pos-start:])
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// Read implements io.Reader.
func (r *Reader) Read(p []byte) (int, error) {
	n, err := r.ReadAt(p, r.off)
	r.off += int64(n)
	if n > 0 && errors.Is(err, io.EOF) {
		err = nil
	}
	return n, err
}

// Walk calls visit for every chunk the stream at ref is made of, the root
// first. Index nodes are read and checked as a Reader checks them, when
// visit says to go on into them; data chunks are named as leaves (leaf
// true, visit's answer unused) and never read, so walking a stream costs
// its index, not its bytes.
func Walk(ctx context.Context, rd chunk.Reader, ref Ref, visit func(h hash.Hash, leaf bool) (bool, error)) error {
	if ref.Depth > maxDepth || ref.Size > math.MaxInt64 {
		return corrupt("ref: depth %d, size %d", ref.Depth, ref.Size)
	}
	return walk(ctx, rd, ref.Root, int(ref.Depth), ref.Size, true, visit)
}

// walk visits h, a chunk at depth (0: data) whose parent says it holds want
// bytes, and what it reaches.
func walk(ctx context.Context, rd chunk.Reader, h hash.Hash, depth int, want uint64, top bool, visit func(h hash.Hash, leaf bool) (bool, error)) error {
	deeper, err := visit(h, depth == 0)
	if err != nil || !deeper || depth == 0 {
		return err
	}
	b, err := rd.Get(ctx, h)
	if err != nil {
		return err
	}
	level, es, err := decodeIndex(b)
	if err != nil {
		return err
	}
	if level != depth {
		return corrupt("an index node at depth %d is level %d", depth, level)
	}
	if top && len(es) < 2 {
		return corrupt("the top index node has one child")
	}
	var sum uint64
	for _, e := range es {
		sum += e.size // decodeIndex checked the sum fits
	}
	if sum != want {
		return corrupt("a level-%d node's entries add to %d bytes, its parent says %d", level, sum, want)
	}
	for _, e := range es {
		if err := walk(ctx, rd, e.child, depth-1, e.size, false, visit); err != nil {
			return err
		}
	}
	return nil
}

// ReadAll returns the whole stream, which must be at most limit bytes long.
// It grows with the bytes actually read, not with what the Ref claims.
func ReadAll(ctx context.Context, rd chunk.Reader, ref Ref, limit uint64) ([]byte, error) {
	r, err := Open(ctx, rd, ref)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	buf.Grow(int(min(ref.Size, 64<<20)))
	if _, err := io.Copy(&buf, r); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
