package stream

import (
	"bytes"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
)

// Whatever decodes as an index node re-encodes to the same bytes: one
// encoding per node.
func FuzzDecodeIndex(f *testing.F) {
	f.Add(encodeIndex(1, []entry{{child: hash.Sum([]byte("a")), size: 5}, {child: hash.Sum([]byte("b")), size: 300}}))
	f.Add([]byte{0x02, 1, 0})
	f.Fuzz(func(t *testing.T, b []byte) {
		level, es, err := decodeIndex(b)
		if err != nil {
			return
		}
		if !bytes.Equal(encodeIndex(level, es), b) {
			t.Fatal("an index node decoded that does not re-encode to itself")
		}
	})
}
