package tree

import (
	"bytes"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
)

// Whatever decodes as an entry re-encodes to the same bytes, and hostile
// bytes never panic.
func FuzzDecodeEntry(f *testing.F) {
	f.Add(Entry{Mode: 0o644, ModTime: 1, Content: model.Root{Size: 5, Format: 1}}.Encode())
	f.Add(make([]byte, EntrySize))
	f.Fuzz(func(t *testing.T, b []byte) {
		e, err := DecodeEntry(b)
		if err != nil {
			return
		}
		if !bytes.Equal(e.Encode(), b) {
			t.Fatal("an entry decoded that does not re-encode to itself")
		}
	})
}
