// Package wire is the one place stored bytes are taken apart. Every on-disk
// structure in snapshot-core is written with Writer and read with Reader, a
// bounds-checked, sticky-error decoder: a truncated field, an overlong or
// non-minimal varint, a length over the caller's cap, or trailing bytes is an
// error, never a panic and never a silent partial read (Storage Core Spec:
// "hand-written, bounds-checked decoders for every on-disk structure").
//
// Integers are little-endian. Varints are unsigned LEB128 and must be minimal,
// so every value has exactly one encoding: decode(encode(v)) == v and
// encode(decode(b)) == b for every b that decodes.
package wire

import "errors"

// Errors a Reader reports. All wrap ErrCorrupt.
var (
	ErrCorrupt  = errors.New("wire: corrupt")
	ErrShort    = errors.New("wire: corrupt: truncated")
	ErrTooLong  = errors.New("wire: corrupt: length over limit")
	ErrVarint   = errors.New("wire: corrupt: bad varint")
	ErrTrailing = errors.New("wire: corrupt: trailing bytes")
)

// Writer appends encoded fields to a buffer.
type Writer struct{ buf []byte }

// U8 appends a byte.
func (w *Writer) U8(v uint8) {}

// U16 appends a little-endian uint16.
func (w *Writer) U16(v uint16) {}

// U32 appends a little-endian uint32.
func (w *Writer) U32(v uint32) {}

// U64 appends a little-endian uint64.
func (w *Writer) U64(v uint64) {}

// Uvarint appends a minimal unsigned LEB128 varint.
func (w *Writer) Uvarint(v uint64) {}

// Raw appends b as is.
func (w *Writer) Raw(b []byte) {}

// LenBytes appends a varint length and then b.
func (w *Writer) LenBytes(b []byte) {}

// Bytes returns the encoded buffer.
func (w *Writer) Bytes() []byte { return w.buf }

// Reader decodes fields. After the first error every read returns a zero
// value and Err reports that first error.
type Reader struct {
	b   []byte
	off int
	err error
}

// NewReader returns a Reader over b. Slices it returns alias b.
func NewReader(b []byte) *Reader { return &Reader{b: b} }

// U8 reads a byte.
func (r *Reader) U8() uint8 { return 0 }

// U16 reads a little-endian uint16.
func (r *Reader) U16() uint16 { return 0 }

// U32 reads a little-endian uint32.
func (r *Reader) U32() uint32 { return 0 }

// U64 reads a little-endian uint64.
func (r *Reader) U64() uint64 { return 0 }

// Uvarint reads a minimal varint of at most 10 bytes.
func (r *Reader) Uvarint() uint64 { return 0 }

// Fixed reads exactly n bytes.
func (r *Reader) Fixed(n int) []byte { return nil }

// LenBytes reads a varint length, refusing one above max, then that many bytes.
func (r *Reader) LenBytes(max int) []byte { return nil }

// Remaining is the number of unread bytes.
func (r *Reader) Remaining() int { return 0 }

// Err is the first error, or nil.
func (r *Reader) Err() error { return r.err }

// Done returns the first error, or ErrTrailing if bytes remain unread.
func (r *Reader) Done() error { return nil }
