package prolly

import (
	"bytes"
	"testing"
)

// Whatever decodes as a node re-encodes to the same bytes: one encoding per
// node, and hostile bytes never panic.
func FuzzDecodeNode(f *testing.F) {
	f.Add([]byte{0x01, 0x00, 0x00})
	f.Add([]byte{0x01, 0x00, 0x01, 0x01, 'a', 0x00, 0x01, 'x'})
	f.Add([]byte{0x01, 0x01, 0x01, 0x01, 'a',
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 0x05})
	f.Fuzz(func(t *testing.T, b []byte) {
		n, err := decodeNode(b, 100)
		if err != nil {
			return
		}
		if !bytes.Equal(n.encode(), b) {
			t.Fatal("a node decoded that does not re-encode to itself")
		}
	})
}

// #23: a Config that names no limit (every one written before MaxValue
// existed) reads and takes values up to DefaultMaxValue, 64 MiB, not
// without limit; one that names a limit gets it.
func TestRegression_SC23_NoLimitNamedIsTheDefault(t *testing.T) {
	if got := (Config{}).maxValue(); got != DefaultMaxValue || DefaultMaxValue != 64<<20 {
		t.Fatalf("a map configured with no value limit holds values of up to %d bytes (DefaultMaxValue %d), want 64 MiB", got, DefaultMaxValue)
	}
	if got := (Config{MaxValue: 55}).maxValue(); got != 55 {
		t.Fatalf("a map configured to hold values of up to 55 bytes holds %d", got)
	}
}
