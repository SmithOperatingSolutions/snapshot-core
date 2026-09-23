package object_test

import (
	"bytes"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/model"
	"github.com/SmithOperatingSolutions/snapshot-core/core/object"
)

// Whatever decodes as an object reference re-encodes to the same bytes and
// keeps the grammar (a model and a format, no flags in v1); hostile bytes
// never panic. The seeds break one rule each.
func FuzzDecodeRef(f *testing.F) {
	good := object.Ref{Model: 1, Root: model.Root{Hash: hash.Sum([]byte("o")), Size: 5, Depth: 2, Format: 1}}.Encode()
	f.Add(good)
	for _, at := range []int{0, 1, 2, 3, 4} { // model 0, format 0, flags 1 (bytes 0-1, 2-3, 4)
		b := bytes.Clone(good)
		switch at {
		case 0, 1:
			b[0], b[1] = 0, 0
		case 2, 3:
			b[2], b[3] = 0, 0
		case 4:
			b[4] = 1
		}
		f.Add(b)
	}
	f.Add(append(bytes.Clone(good), 0))
	f.Add(good[:len(good)-1])
	f.Fuzz(func(t *testing.T, b []byte) {
		r, err := object.DecodeRef(b)
		if err != nil {
			return
		}
		if !bytes.Equal(r.Encode(), b) {
			t.Fatal("a reference decoded that does not re-encode to itself")
		}
		if r.Model == 0 || r.Root.Format == 0 || r.Flags != 0 {
			t.Fatalf("a reference outside the grammar decoded: model %d, format %d, flags %#x", r.Model, r.Root.Format, r.Flags)
		}
	})
}
