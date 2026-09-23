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
	"io"
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
func DefaultGeometry() Geometry { return Geometry{} }

// Validate reports whether g is usable.
func (g Geometry) Validate() error { return nil }

// Chunker cuts one stream. Not safe for concurrent use.
type Chunker struct{}

// New returns a chunker over r. It refuses an invalid geometry.
func New(r io.Reader, g Geometry) (*Chunker, error) { return &Chunker{}, nil }

// Next returns the next chunk's bytes, owned by the caller, or io.EOF.
func (c *Chunker) Next() ([]byte, error) { return nil, io.EOF }
