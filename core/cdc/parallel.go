package cdc

import (
	"errors"
	"io"
	"math/bits"
	"runtime"
	"sync"
)

// Parallel cuts one stream with its hashing spread over workers goroutines
// (#10). The Buzhash at a byte depends on the 48 bytes before it alone, so
// the workers mark, block by block, where each mask hits, while the
// caller's goroutine places the cuts by the rules, in order: the
// boundaries are the Chunker's byte for byte (core/dnx/compat holds both
// to disknexus). At most a few blocks are in flight. Close stops the
// goroutines; a Parallel abandoned without Close leaks them.
type Parallel struct {
	g          Geometry
	hard, easy uint64
	stop       chan struct{}
	once       sync.Once
	order      chan *block // marked blocks, in stream order
	free       chan []byte // block buffers the placer is done with, for the reader to fill again

	cur  *block
	pos  int    // into cur.data
	n    int    // bytes in the chunk under way
	buf  []byte // the chunk under way, when it spans blocks
	done error  // io.EOF or the read error, once every block is consumed
}

// block is one read of the stream and the marks over it.
type block struct {
	prev []byte // the up to 47 bytes before data, for the window
	data []byte
	err  error    // what the read returned with or after data
	hard []uint64 // bit i: the hard mask hits at data[i]
	easy []uint64 // bit i: the easy mask hits at data[i]
	done chan struct{}
}

const blockSize = readSize // the Chunker's read: both consume a stream the same way, so a read that fails does at the same chunk

// NewParallel returns a parallel chunker over r with workers goroutines
// hashing (0: runtime.GOMAXPROCS(0)). It refuses an invalid geometry.
func NewParallel(r io.Reader, g Geometry, workers int) (*Parallel, error) {
	if err := g.Validate(); err != nil {
		return nil, err
	}
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	p := &Parallel{g: g, hard: g.Mask<<2 | g.Mask, easy: g.Mask >> 2, stop: make(chan struct{}),
		order: make(chan *block, 2*workers), free: make(chan []byte, 2*workers+2)}
	work := make(chan *block, workers)
	go p.read(r, work)
	for range workers {
		go func() {
			for b := range work {
				b.hard, b.easy = p.g.mark(b.prev, b.data)
				close(b.done)
			}
		}()
	}
	return p, nil
}

// read fills blocks from r, each carrying the 47 bytes before it, and
// hands them to the workers and to the placer in order; a read's error
// travels with the block it came with, or in an empty one after.
func (p *Parallel) read(r io.Reader, work chan<- *block) {
	defer close(work)
	defer close(p.order)
	var prev []byte
	empty := 0
	for {
		var data []byte
		select {
		case data = <-p.free: // a block the placer is done with; its chunks were copied out
			data = data[:blockSize]
		default:
			data = make([]byte, blockSize)
		}
		n, err := r.Read(data)
		if n == 0 && err == nil {
			if empty++; empty >= maxConsecutiveEmptyReads {
				err = io.ErrNoProgress
			} else {
				continue
			}
		}
		empty = 0
		b := &block{prev: prev, data: data[:n], err: err, done: make(chan struct{})}
		if n > 0 {
			keep := min(n, window-1)
			prev = append([]byte(nil), data[n-keep:n]...)
			if keep < window-1 && len(b.prev) > 0 { // a short read: carry the older bytes too
				tail := append(append([]byte(nil), b.prev...), data[:n]...)
				prev = tail[max(0, len(tail)-(window-1)):]
			}
		}
		select {
		case p.order <- b:
		case <-p.stop:
			return
		}
		if n > 0 {
			select {
			case work <- b:
			case <-p.stop:
				return
			}
		} else {
			close(b.done)
		}
		if err != nil {
			return
		}
	}
}

// mark rolls the hash over data, the window warmed with prev (the bytes
// before data; zeros stand for what is not there), and returns where the
// hard and the easy mask hit. The byte leaving the window is the one 48
// back, in prev or in data: no ring buffer.
func (g Geometry) mark(prev, data []byte) (hard, easy []uint64) {
	hmask, emask := g.Mask<<2|g.Mask, g.Mask>>2
	words := (len(data) + 63) / 64
	hard, easy = make([]uint64, words), make([]uint64, words)
	var hist [window]byte // the 48 bytes before data[0], zeros where the stream had none
	copy(hist[window-len(prev):], prev)
	hash := zeroWindowHash
	for _, b := range prev {
		hash = bits.RotateLeft64(hash, 1) ^ outTable[0] ^ table[b]
	}
	for i, b := range data {
		var out byte
		if i >= window {
			out = data[i-window]
		} else {
			out = hist[i] //nolint:gosec // G602: i < window here
		}
		hash = bits.RotateLeft64(hash, 1) ^ outTable[out] ^ table[b]
		if hash&hmask == 0 {
			hard[i>>6] |= 1 << (i & 63)
		}
		if hash&emask == 0 {
			easy[i>>6] |= 1 << (i & 63)
		}
	}
	return hard, easy
}

// firstSet returns the first i in [from, to) whose bit is set in bm, or -1.
func firstSet(bm []uint64, from, to int) int {
	for w := from >> 6; w<<6 < to; w++ {
		word := bm[w]
		if w == from>>6 {
			word &= ^uint64(0) << (from & 63)
		}
		if word != 0 {
			i := w<<6 + bits.TrailingZeros64(word)
			if i < to {
				return i
			}
			return -1
		}
	}
	return -1
}

// Next returns the next chunk's bytes, owned by the caller, or io.EOF; a
// read error comes out once the bytes before it are cut, the chunk under
// way when it struck not returned, as from the Chunker.
func (p *Parallel) Next() ([]byte, error) {
	for {
		if p.cur == nil {
			if p.done != nil {
				return nil, p.done
			}
			b, ok := <-p.order
			if !ok {
				return nil, errors.New("cdc: the parallel chunker's reader stopped without an end")
			}
			<-b.done
			p.cur, p.pos = b, 0
		}
		b := p.cur
		for p.pos < len(b.data) {
			from := p.pos
			// Bytes judged by the hard mask: those that leave the chunk under Min.
			if need := p.g.Min - p.n - 1; need > 0 {
				to := min(from+need, len(b.data))
				if i := firstSet(b.hard, from, to); i >= 0 {
					return p.emit(b, i), nil
				}
				p.n += to - from
				p.pos = to
				if to == len(b.data) {
					break
				}
				from = to
			}
			// Bytes judged by the easy mask: those that leave it under Max.
			if need := p.g.Max - p.n - 1; need > 0 {
				to := min(from+need, len(b.data))
				if i := firstSet(b.easy, from, to); i >= 0 {
					return p.emit(b, i), nil
				}
				p.n += to - from
				p.pos = to
				if to == len(b.data) {
					break
				}
				from = to
			}
			// The byte that makes the chunk Max long ends it whatever the hash.
			return p.emit(b, from), nil
		}
		// The block is consumed: what it holds of the chunk under way is kept.
		p.buf = append(p.buf, b.data[len(b.data)-(p.n-len(p.buf)):]...)
		p.release(b)
		if b.err != nil {
			p.done = b.err
			p.cur = nil
			if errors.Is(b.err, io.EOF) && len(p.buf) > 0 {
				out := p.buf
				p.buf, p.n = nil, 0
				return out, nil
			}
			return nil, b.err
		}
		p.cur = nil
	}
}

// emit ends the chunk under way at b.data[i] and returns it: one copy out
// of the block (or onto what earlier blocks contributed), never a slice of
// the block, whose buffer is filled again once the placer is done with it.
func (p *Parallel) emit(b *block, i int) []byte {
	share := b.data[p.chunkStart(b) : i+1]
	var out []byte
	if p.buf == nil {
		out = make([]byte, len(share))
		copy(out, share)
	} else {
		out = p.buf
		out = append(out, share...)
	}
	p.buf, p.n = nil, 0
	p.pos = i + 1
	return out
}

// release hands a consumed block's buffer back to the reader.
func (p *Parallel) release(b *block) {
	select {
	case p.free <- b.data[:0]:
	default: // the reader is far ahead; the buffer is garbage
	}
}

// chunkStart is the index in b.data where the chunk under way begins:
// the bytes of it not yet in buf are the block's.
func (p *Parallel) chunkStart(b *block) int { return p.pos - (p.n - len(p.buf)) }

// Close stops the goroutines; the stream is left where it was.
func (p *Parallel) Close() { p.once.Do(func() { close(p.stop) }) }
