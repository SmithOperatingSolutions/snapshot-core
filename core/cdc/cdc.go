// Package cdc cuts byte streams (files, images, Parquet, model weights, any
// opaque blob) into content-defined chunks, using the disknexus chunker
// through core/dnx (Storage Core Spec, "Chunkers"). Geometry is repo-wide and
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

	"github.com/SmithOperatingSolutions/snapshot-core/core/dnx"
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
type Chunker struct{ c *dnx.Chunker }

// New returns a chunker over r. It refuses an invalid geometry.
func New(r io.Reader, g Geometry) (*Chunker, error) {
	if err := g.Validate(); err != nil {
		return nil, err
	}
	return &Chunker{c: dnx.NewChunker(r, dnx.Geometry{Min: g.Min, Max: g.Max, Mask: g.Mask})}, nil
}

// Next returns the next chunk's bytes, owned by the caller, or io.EOF.
func (c *Chunker) Next() ([]byte, error) {
	ch, err := c.c.Next()
	if err != nil {
		return nil, err
	}
	return ch.Data, nil
}
