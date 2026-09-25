package cache

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
)

// A cache entry file is read back by two decoders: readHeaderName when a
// process reloads the directory, decodeEntry on every hit. Whatever bytes
// sit in the directory, neither panics; decodeEntry serves content only
// for an entry that re-encodes to exactly those bytes under the name asked
// for (so the name, the sum and the content all agree); readHeaderName
// yields only a valid object name, the one the bytes begin with.
func FuzzDecodeEntry(f *testing.F) {
	const name = "packs/ab/abcdef"
	good := encodeEntry(name, []byte("cached ciphertext"))
	damaged := bytes.Clone(good)
	damaged[len(damaged)-1] ^= 1
	f.Add(good, name)
	f.Add(good, "index/0123")                                 // another object's entry
	f.Add(damaged, name)                                      // content that does not match its sum
	f.Add(encodeEntry("../escape", []byte("x")), "../escape") // a name no backend would hold
	f.Add(append([]byte("SCCF"), good[4:]...), name)          // another magic
	f.Add([]byte("SCCE"), name)
	f.Fuzz(func(t *testing.T, b []byte, asked string) {
		if content, ok := decodeEntry(b, asked); ok && !bytes.Equal(encodeEntry(asked, content), b) {
			t.Fatalf("a cache entry served %d bytes for %q from a file that is not that entry: "+
				"a damaged or foreign cache file would be served as the object", len(content), asked)
		}
		path := filepath.Join(t.TempDir(), "entry")
		if err := os.WriteFile(path, b, 0o600); err != nil {
			t.Fatal(err)
		}
		got, err := readHeaderName(path)
		if err != nil {
			return
		}
		head := encodeEntry(got, nil) // magic, name, the sum of nothing
		if blob.ValidName(got) != nil || !bytes.HasPrefix(b, head[:len(head)-32]) {
			t.Fatalf("reloading the cache indexed a file under %q, which it does not begin with or no backend could name", got)
		}
	})
}
