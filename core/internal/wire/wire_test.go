package wire_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/internal/wire"
)

func TestRoundTripEveryFieldKind(t *testing.T) {
	var w wire.Writer
	w.U8(0xAB)
	w.U16(0xBEEF)
	w.U32(0xDEADBEEF)
	w.U64(math.MaxUint64 - 1)
	for _, v := range []uint64{0, 1, 127, 128, 300, 1 << 35, math.MaxUint64} {
		w.Uvarint(v)
	}
	w.Raw([]byte("raw"))
	w.LenBytes([]byte("length-prefixed"))
	w.LenBytes(nil)

	r := wire.NewReader(w.Bytes())
	if v := r.U8(); v != 0xAB {
		t.Errorf("U8 = %#x", v)
	}
	if v := r.U16(); v != 0xBEEF {
		t.Errorf("U16 = %#x", v)
	}
	if v := r.U32(); v != 0xDEADBEEF {
		t.Errorf("U32 = %#x", v)
	}
	if v := r.U64(); v != math.MaxUint64-1 {
		t.Errorf("U64 = %#x", v)
	}
	for _, want := range []uint64{0, 1, 127, 128, 300, 1 << 35, math.MaxUint64} {
		if v := r.Uvarint(); v != want {
			t.Errorf("Uvarint = %d, want %d", v, want)
		}
	}
	if v := r.Fixed(3); string(v) != "raw" {
		t.Errorf("Fixed(3) = %q", v)
	}
	if v := r.LenBytes(100); string(v) != "length-prefixed" {
		t.Errorf("LenBytes = %q", v)
	}
	if v := r.LenBytes(100); len(v) != 0 {
		t.Errorf("empty LenBytes = %q", v)
	}
	if err := r.Done(); err != nil {
		t.Fatalf("Done after reading everything: %v", err)
	}
}

// The layout is the documented one: little-endian integers, LEB128 varints.
func TestLayoutIsLittleEndianAndLEB128(t *testing.T) {
	var w wire.Writer
	w.U32(0x01020304)
	w.Uvarint(300)
	want := binary.LittleEndian.AppendUint32(nil, 0x01020304)
	want = binary.AppendUvarint(want, 300)
	if !bytes.Equal(w.Bytes(), want) {
		t.Fatalf("encoded % x, want % x", w.Bytes(), want)
	}
}

func TestTruncationIsAnErrorNotAPanic(t *testing.T) {
	var w wire.Writer
	w.U64(7)
	full := w.Bytes()
	for n := 0; n < len(full); n++ {
		r := wire.NewReader(full[:n])
		if v := r.U64(); v != 0 {
			t.Errorf("truncated U64 returned %d", v)
		}
		if !errors.Is(r.Err(), wire.ErrShort) || !errors.Is(r.Err(), wire.ErrCorrupt) {
			t.Errorf("%d of 8 bytes: err = %v, want ErrShort (wrapping ErrCorrupt)", n, r.Err())
		}
	}
	if _, err := read(func(r *wire.Reader) { r.Fixed(5) }, []byte{1, 2}); !errors.Is(err, wire.ErrShort) {
		t.Errorf("Fixed past the end: %v", err)
	}
	if _, err := read(func(r *wire.Reader) { r.Fixed(-1) }, []byte{1, 2}); !errors.Is(err, wire.ErrCorrupt) {
		t.Errorf("Fixed(-1): %v", err)
	}
}

func read(f func(*wire.Reader), b []byte) (*wire.Reader, error) {
	r := wire.NewReader(b)
	f(r)
	return r, r.Err()
}

func TestErrorsAreSticky(t *testing.T) {
	r := wire.NewReader([]byte{1})
	r.U32() // fails
	if v := r.U8(); v != 0 {
		t.Fatalf("a read after an error returned %d; the byte that is there must not be served after a failure", v)
	}
	if !errors.Is(r.Err(), wire.ErrShort) {
		t.Fatalf("first error lost: %v", r.Err())
	}
}

func TestVarintsMustBeMinimalAndBounded(t *testing.T) {
	cases := map[string][]byte{
		"non-minimal zero":   {0x80, 0x00},
		"non-minimal 1":      {0x81, 0x00},
		"eleven bytes":       {0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01},
		"overflows uint64":   {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x02},
		"unterminated":       {0x80},
		"unterminated empty": {},
	}
	for name, b := range cases {
		if _, err := read(func(r *wire.Reader) { r.Uvarint() }, b); !errors.Is(err, wire.ErrCorrupt) {
			t.Errorf("%s (% x): err = %v, want a corrupt varint — two encodings of one value would give "+
				"one structure two hashes", name, b, err)
		}
	}
	// Positive control: the largest value is accepted in its minimal form.
	max := binary.AppendUvarint(nil, math.MaxUint64)
	if r, err := read(func(r *wire.Reader) { r.Uvarint() }, max); err != nil || r.Remaining() != 0 {
		t.Errorf("minimal MaxUint64 varint refused: %v", err)
	}
}

func TestLenBytesHonorsTheCallersLimit(t *testing.T) {
	var w wire.Writer
	w.LenBytes(bytes.Repeat([]byte{1}, 100))
	if _, err := read(func(r *wire.Reader) { r.LenBytes(100) }, w.Bytes()); err != nil {
		t.Fatalf("positive control: 100 bytes under a 100 limit refused: %v", err)
	}
	if _, err := read(func(r *wire.Reader) { r.LenBytes(99) }, w.Bytes()); !errors.Is(err, wire.ErrTooLong) {
		t.Fatalf("100 bytes under a 99 limit: err = %v, want ErrTooLong", err)
	}
	// A length claiming more than exists must fail before any allocation of that size.
	huge := binary.AppendUvarint(nil, 1<<40)
	if _, err := read(func(r *wire.Reader) { r.LenBytes(math.MaxInt) }, huge); !errors.Is(err, wire.ErrShort) {
		t.Fatalf("a 1 TiB length over 5 bytes: err = %v, want ErrShort", err)
	}
}

func TestDoneRefusesTrailingBytes(t *testing.T) {
	r := wire.NewReader([]byte{1, 2})
	r.U8()
	if err := r.Done(); !errors.Is(err, wire.ErrTrailing) {
		t.Fatalf("Done with a byte unread: %v, want ErrTrailing", err)
	}
	r = wire.NewReader([]byte{1})
	r.U8()
	if err := r.Done(); err != nil {
		t.Fatalf("positive control: Done after reading everything: %v", err)
	}
}

// Fuzz: any input decodes to values or an error, never a panic; and whatever
// decodes re-encodes to the same bytes (canonical encoding).
func FuzzReaderIsCanonical(f *testing.F) {
	var w wire.Writer
	w.Uvarint(300)
	w.LenBytes([]byte("abc"))
	w.U32(9)
	f.Add(w.Bytes())
	f.Add([]byte{0x80, 0x00})
	f.Fuzz(func(t *testing.T, b []byte) {
		r := wire.NewReader(b)
		v := r.Uvarint()
		s := r.LenBytes(1 << 20)
		x := r.U32()
		if r.Err() != nil {
			return
		}
		var w wire.Writer
		w.Uvarint(v)
		w.LenBytes(s)
		w.U32(x)
		if consumed := len(b) - r.Remaining(); !bytes.Equal(w.Bytes(), b[:consumed]) {
			t.Fatalf("decoded % x but re-encoded % x: the encoding is not canonical", b[:consumed], w.Bytes())
		}
	})
}
