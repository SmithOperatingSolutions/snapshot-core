package blob

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
)

// These helpers are how every backend applies the port's rules identically.

// CheckPut validates a Put's name and declared size.
func CheckPut(name string, size int64) error {
	if err := ValidName(name); err != nil {
		return err
	}
	if size < 0 {
		return fmt.Errorf("%w: negative size %d", ErrSizeMismatch, size)
	}
	if size > MaxObjectSize {
		return fmt.Errorf("%w: %d bytes exceeds %d", ErrTooLarge, size, MaxObjectSize)
	}
	return nil
}

// CopyExact copies exactly size bytes from src to dst and confirms src is
// then exhausted. Fewer or more bytes is ErrSizeMismatch.
func CopyExact(dst io.Writer, src io.Reader, size int64) error {
	n, err := io.CopyN(dst, src, size)
	if errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: body ended after %d of %d bytes", ErrSizeMismatch, n, size)
	}
	if err != nil {
		return err
	}
	var one [1]byte
	m, err := io.ReadFull(src, one[:])
	if m > 0 {
		return fmt.Errorf("%w: body is longer than %d bytes", ErrSizeMismatch, size)
	}
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return err
	}
	return nil
}

// Range validates a Get range against an object's size and returns the byte
// span to serve: a range running past the end is cut at the end.
func Range(off, n, size int64) (start, length int64, err error) {
	if off < 0 || n < -1 || off > size {
		return 0, 0, fmt.Errorf("%w: off=%d n=%d size=%d", ErrInvalidRange, off, n, size)
	}
	length = size - off
	if n >= 0 && n < length {
		length = n
	}
	return off, length, nil
}

// CheckList validates a List call.
func CheckList(prefix string, limit int) error {
	if err := ValidPrefix(prefix); err != nil {
		return err
	}
	if limit < 1 || limit > MaxListPage {
		return fmt.Errorf("%w: %d outside 1..%d", ErrInvalidLimit, limit, MaxListPage)
	}
	return nil
}

// CheckRootValue validates a SwapRoot value.
func CheckRootValue(next []byte) error {
	if len(next) == 0 {
		return ErrEmptyRoot
	}
	if len(next) > MaxRootSize {
		return fmt.Errorf("%w: root value is %d bytes, over %d", ErrTooLarge, len(next), MaxRootSize)
	}
	return nil
}

// NewVersion returns a fresh random version token. 128 random bits: a token
// is never handed out twice in practice, which is what rules out ABA.
func NewVersion() (Version, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return NoVersion, err
	}
	return Version(hex.EncodeToString(b[:])), nil
}
