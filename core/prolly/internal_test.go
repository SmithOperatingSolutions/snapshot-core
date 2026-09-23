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
