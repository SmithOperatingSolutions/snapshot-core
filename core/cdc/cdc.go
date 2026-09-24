// Package cdc cuts byte streams (files, images, Parquet, model weights, any
// opaque blob) into content-defined chunks, with disknexus's Buzhash and
// cut rules (Storage Core Spec, "Chunkers"), the same boundaries byte for
// byte (core/dnx/compat holds it to disknexus). Geometry is repo-wide and
// immutable: it is recorded in the repo config at init and never changed.
//
// Min is soft: the chunker's hard mask makes a cut below Min rare, not
// impossible (that is what keeps shift-resync working). Max is hard.
package cdc

import (
	"errors"
	"fmt"
	"io"
	"math/bits"
)

// MaxChunkSize is the chunk store's limit (Engine Spec L0: 1 MiB). No geometry
// may cut chunks larger than this.
const MaxChunkSize = 1 << 20

// ErrGeometry is returned for an unusable geometry.
var ErrGeometry = errors.New("cdc: invalid geometry")

// Geometry configures the chunker.
type Geometry struct {
	Min  int    // soft minimum chunk size
	Max  int    // hard maximum chunk size
	Mask uint64 // Buzhash base mask, 2^n - 1; the average chunk is about 2*Min + (Mask+1)/4
}

// DefaultGeometry is disknexus's field-proven default (16 KiB / 512 KiB /
// 0xFFFF, its #83 decision); over random data it averages about 31 KiB.
func DefaultGeometry() Geometry { return Geometry{Min: 16 << 10, Max: 512 << 10, Mask: 0xFFFF} }

// Limits on a geometry. The disknexus chunker accepts anything (max 0 cuts a
// chunk per byte; min > max is silently honored), so these are ours.
const (
	minMin      = 64 // below this, per-chunk overhead dominates
	minMaskBits = 8  // the easy mask (mask>>2) must still need 6 bits
	maxMaskBits = 30
)

// Validate reports whether g is usable.
func (g Geometry) Validate() error {
	switch {
	case g.Min < minMin:
		return fmt.Errorf("%w: min %d is below %d", ErrGeometry, g.Min, minMin)
	case g.Max <= g.Min:
		return fmt.Errorf("%w: max %d must exceed min %d", ErrGeometry, g.Max, g.Min)
	case g.Max > MaxChunkSize:
		return fmt.Errorf("%w: max %d exceeds the %d-byte chunk limit", ErrGeometry, g.Max, MaxChunkSize)
	case g.Mask == 0 || g.Mask&(g.Mask+1) != 0:
		return fmt.Errorf("%w: mask %#x is not of the form 2^n-1", ErrGeometry, g.Mask)
	}
	if n := bits.Len64(g.Mask); n < minMaskBits || n > maxMaskBits {
		return fmt.Errorf("%w: mask %#x has %d bits, want %d..%d", ErrGeometry, g.Mask, n, minMaskBits, maxMaskBits)
	}
	return nil
}

// Chunker cuts one stream. Not safe for concurrent use.
//
// It is disknexus's algorithm, cut here rather than through core/dnx (#10):
// disknexus reads a byte at a time and bounds a write at about 200 MB/s;
// this loop runs over each read buffer and cuts the same boundaries several
// times as fast. core/dnx/compat holds it to disknexus byte for byte.
type Chunker struct {
	r          io.Reader
	g          Geometry
	hard, easy uint64 // the mask below Min (mask<<2 | mask) and from Min up (mask>>2)

	// The rolling hash, carried across chunk boundaries.
	win  [window]byte
	pos  int
	hash uint64

	buf  []byte // the chunk being cut
	rd   []byte // the read buffer
	rpos int
	rend int
	eof  bool
	rerr error // a read error to surface once the buffered bytes are consumed
}

const (
	readSize                 = 64 << 10
	maxConsecutiveEmptyReads = 100 // (0, nil) reads before io.ErrNoProgress, as bufio
)

// New returns a chunker over r. It refuses an invalid geometry.
func New(r io.Reader, g Geometry) (*Chunker, error) {
	if err := g.Validate(); err != nil {
		return nil, err
	}
	return &Chunker{r: r, g: g, hard: g.Mask<<2 | g.Mask, easy: g.Mask >> 2, hash: zeroWindowHash,
		buf: make([]byte, 0, g.Max), rd: make([]byte, readSize)}, nil
}

// Next returns the next chunk's bytes, owned by the caller, or io.EOF. A
// read error is returned as is once the bytes read before it are cut; the
// chunk under way when it struck is not returned.
func (c *Chunker) Next() ([]byte, error) {
	c.buf = c.buf[:0]
	for {
		if c.rpos >= c.rend {
			if err := c.fill(); err != nil {
				if errors.Is(err, io.EOF) && len(c.buf) > 0 {
					return append([]byte(nil), c.buf...), nil
				}
				return nil, err
			}
		}
		if end, cut := c.scan(c.rd[c.rpos:c.rend], len(c.buf)); cut {
			c.buf = append(c.buf, c.rd[c.rpos:c.rpos+end]...)
			c.rpos += end
			return append([]byte(nil), c.buf...), nil
		} else {
			c.buf = append(c.buf, c.rd[c.rpos:c.rpos+end]...)
			c.rpos += end
		}
	}
}

// scan rolls the hash over p with n bytes already in the chunk and returns
// how many bytes of p belong to the chunk and whether it ends there: at a
// hash the hard mask misses below Min, one the easy mask misses from Min
// up, or at Max. The two mask phases are two loops, so the byte loop has
// no branch on which mask applies.
func (c *Chunker) scan(p []byte, n int) (end int, cut bool) {
	hash, pos := c.hash, c.pos
	win := &c.win
	i := 0
	// Bytes judged by the hard mask: those that leave the chunk under Min.
	for lim := min(c.g.Min-n-1, len(p)); i < lim; i++ {
		b := p[i]
		hash = bits.RotateLeft64(hash, 1) ^ outTable[win[pos]] ^ table[b]
		win[pos] = b
		if pos++; pos == window {
			pos = 0
		}
		if hash&c.hard == 0 {
			c.hash, c.pos = hash, pos
			return i + 1, true
		}
	}
	// Bytes judged by the easy mask: those that leave it under Max.
	for lim := min(c.g.Max-n-1, len(p)); i < lim; i++ {
		b := p[i]
		hash = bits.RotateLeft64(hash, 1) ^ outTable[win[pos]] ^ table[b]
		win[pos] = b
		if pos++; pos == window {
			pos = 0
		}
		if hash&c.easy == 0 {
			c.hash, c.pos = hash, pos
			return i + 1, true
		}
	}
	// The byte that makes the chunk Max long ends it whatever the hash.
	if i < len(p) {
		b := p[i]
		hash = bits.RotateLeft64(hash, 1) ^ outTable[win[pos]] ^ table[b]
		win[pos] = b
		if pos++; pos == window {
			pos = 0
		}
		c.hash, c.pos = hash, pos
		return i + 1, true
	}
	c.hash, c.pos = hash, pos
	return len(p), false
}

// fill reads more bytes, serving what a read returned before the error it
// returned with, retrying (0, nil) a bounded number of times, and
// surfacing io.EOF only once everything read is consumed.
func (c *Chunker) fill() error {
	for empty := 0; c.rpos >= c.rend; {
		if c.eof {
			return io.EOF
		}
		if c.rerr != nil {
			err := c.rerr
			c.rerr, c.eof = nil, true // no further reads after a terminal error
			return err
		}
		n, err := c.r.Read(c.rd)
		if n > 0 {
			c.rpos, c.rend = 0, n
		}
		switch {
		case errors.Is(err, io.EOF):
			c.eof = true
		case err != nil:
			c.rerr = err
		case n == 0:
			if empty++; empty >= maxConsecutiveEmptyReads {
				return io.ErrNoProgress
			}
		}
	}
	return nil
}
