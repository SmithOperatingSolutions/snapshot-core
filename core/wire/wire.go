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

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Errors a Reader reports. All wrap ErrCorrupt.
var (
	ErrCorrupt  = errors.New("wire: corrupt")
	ErrShort    = fmt.Errorf("%w: truncated", ErrCorrupt)
	ErrTooLong  = fmt.Errorf("%w: length over limit", ErrCorrupt)
	ErrVarint   = fmt.Errorf("%w: bad varint", ErrCorrupt)
	ErrTrailing = fmt.Errorf("%w: trailing bytes", ErrCorrupt)
)

// Writer appends encoded fields to a buffer.
type Writer struct{ buf []byte }

// U8 appends a byte.
func (w *Writer) U8(v uint8) { w.buf = append(w.buf, v) }

// U16 appends a little-endian uint16.
func (w *Writer) U16(v uint16) { w.buf = binary.LittleEndian.AppendUint16(w.buf, v) }

// U32 appends a little-endian uint32.
func (w *Writer) U32(v uint32) { w.buf = binary.LittleEndian.AppendUint32(w.buf, v) }

// U64 appends a little-endian uint64.
func (w *Writer) U64(v uint64) { w.buf = binary.LittleEndian.AppendUint64(w.buf, v) }

// Uvarint appends a minimal unsigned LEB128 varint.
func (w *Writer) Uvarint(v uint64) { w.buf = binary.AppendUvarint(w.buf, v) }

// Raw appends b as is.
func (w *Writer) Raw(b []byte) { w.buf = append(w.buf, b...) }

// LenBytes appends a varint length and then b.
func (w *Writer) LenBytes(b []byte) {
	w.Uvarint(uint64(len(b)))
	w.Raw(b)
}

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
func (r *Reader) U8() uint8 {
	b := r.Fixed(1)
	if b == nil {
		return 0
	}
	return b[0]
}

// U16 reads a little-endian uint16.
func (r *Reader) U16() uint16 {
	b := r.Fixed(2)
	if b == nil {
		return 0
	}
	return binary.LittleEndian.Uint16(b)
}

// U32 reads a little-endian uint32.
func (r *Reader) U32() uint32 {
	b := r.Fixed(4)
	if b == nil {
		return 0
	}
	return binary.LittleEndian.Uint32(b)
}

// U64 reads a little-endian uint64.
func (r *Reader) U64() uint64 {
	b := r.Fixed(8)
	if b == nil {
		return 0
	}
	return binary.LittleEndian.Uint64(b)
}

// Uvarint reads a minimal varint of at most 10 bytes.
func (r *Reader) Uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Uvarint(r.b[r.off:])
	switch {
	case n == 0:
		r.err = ErrShort
		return 0
	case n < 0:
		r.err = ErrVarint // overflows 64 bits or longer than 10 bytes
		return 0
	case n > 1 && r.b[r.off+n-1] == 0:
		r.err = ErrVarint // a trailing zero group: not the minimal encoding
		return 0
	}
	r.off += n
	return v
}

// Fixed reads exactly n bytes.
func (r *Reader) Fixed(n int) []byte {
	if r.err != nil {
		return nil
	}
	if n < 0 {
		r.err = fmt.Errorf("%w: negative length %d", ErrCorrupt, n)
		return nil
	}
	if n > len(r.b)-r.off {
		r.err = ErrShort
		return nil
	}
	b := r.b[r.off : r.off+n : r.off+n]
	r.off += n
	return b
}

// LenBytes reads a varint length, refusing one above max, then that many bytes.
func (r *Reader) LenBytes(max int) []byte {
	n := r.Uvarint()
	if r.err != nil {
		return nil
	}
	if n > uint64(max) {
		r.err = ErrTooLong
		return nil
	}
	// n <= max <= MaxInt, so the conversion is exact; Fixed refuses a length
	// past the end without allocating anything (it slices, never copies).
	return r.Fixed(int(n))
}

// Remaining is the number of unread bytes.
func (r *Reader) Remaining() int { return len(r.b) - r.off }

// Err is the first error, or nil.
func (r *Reader) Err() error { return r.err }

// Done returns the first error, or ErrTrailing if bytes remain unread.
func (r *Reader) Done() error {
	if r.err != nil {
		return r.err
	}
	if r.off != len(r.b) {
		return ErrTrailing
	}
	return nil
}
