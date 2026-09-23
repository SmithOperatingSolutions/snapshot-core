// Package stream stores byte sequences of any length as content-defined
// chunks under a content-defined index tree (docs/DESIGN.md §7): data cut by
// core/cdc, index nodes split by core/boundary. Equal bytes are an equal
// stream, and an edit rewrites only the chunks around it.
package stream

import (
	"context"
	"io"

	"github.com/SmithOperatingSolutions/snapshot-core/core/boundary"
	"github.com/SmithOperatingSolutions/snapshot-core/core/cdc"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
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
}

// DefaultConfig is the default repo geometry.
func DefaultConfig() Config { return Config{} }

// Write stores everything r yields and returns its Ref.
func Write(ctx context.Context, w chunk.Writer, r io.Reader, c Config) (Ref, error) {
	return Ref{}, nil
}

// Reader reads a stream at any offset, verifying what it reads.
type Reader struct{}

// Open checks ref's root and returns a Reader; ctx serves its reads.
func Open(ctx context.Context, rd chunk.Reader, ref Ref) (*Reader, error) { return &Reader{}, nil }

// Size is the stream's length.
func (r *Reader) Size() int64 { return 0 }

// ReadAt implements io.ReaderAt.
func (r *Reader) ReadAt(p []byte, off int64) (int, error) { return 0, io.EOF }

// Read implements io.Reader.
func (r *Reader) Read(p []byte) (int, error) { return 0, io.EOF }

// ReadAll returns the whole stream.
func ReadAll(ctx context.Context, rd chunk.Reader, ref Ref) ([]byte, error) { return nil, nil }

type entry struct {
	child hash.Hash
	size  uint64
}

func encodeIndex(level int, es []entry) []byte { return nil }

func decodeIndex(b []byte) (int, []entry, error) { return 0, nil, nil }
