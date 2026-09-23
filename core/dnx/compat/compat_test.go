package compat_test

import (
	"bufio"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"testing/iotest"

	"golang.org/x/crypto/argon2"

	"github.com/SmithOperatingSolutions/snapshot-core/core/dnx"
)

var update = flag.Bool("update", false, "rewrite the golden boundary files (only when disknexus and the reference implementation agree)")

// The repository's default byte-stream geometry (core/cdc) and a second, finer
// one, so a change in how the chunker reads its parameters shows up even if
// the default happens to survive it.
var (
	repoGeometry = dnx.Geometry{Min: 16 << 10, Max: 512 << 10, Mask: 0xFFFF}
	fineGeometry = dnx.Geometry{Min: 4 << 10, Max: 64 << 10, Mask: 0x1FFF}
)

// corpus returns n bytes of SHA-256 in counter mode. It is specified entirely
// by this function, so the golden files cannot drift with a PRNG change in Go.
func corpus(n int) []byte {
	out := make([]byte, 0, n+sha256.Size)
	var ctr [8]byte
	for i := uint64(0); len(out) < n; i++ {
		binary.BigEndian.PutUint64(ctr[:], i)
		h := sha256.Sum256(append([]byte("snapshot-core/compat/corpus/v1"), ctr[:]...))
		out = append(out, h[:]...)
	}
	return out[:n]
}

// --- chunk identity ---

func TestChunkIdentityIsSHA256OfExactBytes(t *testing.T) {
	inputs := map[string][]byte{
		"empty":        {},
		"one byte":     {0x42},
		"zero run":     make([]byte, 1<<20), // what a disknexus normalizer would rewrite
		"corpus 1 MiB": corpus(1 << 20),
	}
	for name, b := range inputs {
		got := dnx.Identify(b).Strong
		if want := sha256.Sum256(b); got != want {
			t.Errorf("%s: chunk identity is %x, want SHA-256 of the exact bytes %x — "+
				"every reader re-verifies chunks against this, so a changed identity makes the whole repository unreadable",
				name, got[:8], want[:8])
		}
	}
}

// --- byte-stream chunking ---

func dnxLengths(t *testing.T, r io.Reader, g dnx.Geometry) []int {
	t.Helper()
	c := dnx.NewChunker(r, g)
	var lens []int
	var next int64
	for {
		ch, err := c.Next()
		if errors.Is(err, io.EOF) {
			return lens
		}
		if err != nil {
			t.Fatalf("chunker: %v", err)
		}
		if ch.Offset != next {
			t.Fatalf("chunk %d starts at %d, want %d (offsets must be contiguous)", len(lens), ch.Offset, next)
		}
		next += int64(len(ch.Data))
		lens = append(lens, len(ch.Data))
	}
}

// refLengths is an independent implementation of the chunking algorithm as
// disknexus documents it: Buzhash over a 48-byte window with a table of
// little-endian uint64s from SHA-256("disknexus-buzhash-v1-<i>"), a hard mask
// (mask<<2 | mask) below Min, an easy mask (mask>>2) up to Max, a forced cut at
// Max, and hash state carried across boundaries. It is the authority the
// golden files are checked against: two derivations must agree.
func refLengths(data []byte, g dnx.Geometry) []int {
	const window = 48
	var table [256]uint64
	for i := range table {
		h := sha256.Sum256([]byte("disknexus-buzhash-v1-" + strconv.Itoa(i)))
		table[i] = binary.LittleEndian.Uint64(h[:8])
	}
	rotl := func(v uint64, n int) uint64 { s := uint(n) & 63; return v<<s | v>>(64-s) }
	var hash uint64
	for i := 0; i < window; i++ {
		hash ^= rotl(table[0], window-1-i)
	}
	var win [window]byte
	pos := 0
	hard, easy := g.Mask<<2|g.Mask, g.Mask>>2
	var lens []int
	n := 0
	for _, b := range data {
		out := win[pos]
		win[pos] = b
		pos = (pos + 1) % window
		hash = rotl(hash, 1) ^ rotl(table[out], window) ^ table[b]
		n++
		cut := false
		switch {
		case n < g.Min:
			cut = hash&hard == 0
		case n < g.Max:
			cut = hash&easy == 0
		default:
			cut = true
		}
		if cut {
			lens = append(lens, n)
			n = 0
		}
	}
	if n > 0 {
		lens = append(lens, n)
	}
	return lens
}

func goldenPath(name string) string { return filepath.Join("testdata", name) }

func writeGolden(t *testing.T, name, header string, lens []int) {
	t.Helper()
	var b strings.Builder
	b.WriteString(header)
	for _, l := range lens {
		fmt.Fprintf(&b, "%d\n", l)
	}
	if err := os.WriteFile(goldenPath(name), []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

func readGolden(t *testing.T, name string) []int {
	t.Helper()
	f, err := os.Open(goldenPath(name))
	if err != nil {
		t.Fatalf("golden %s missing (%v): generate it with `go test ./core/dnx/compat -update`, "+
			"which refuses unless disknexus and the reference implementation agree", name, err)
	}
	defer f.Close()
	var lens []int
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		n, err := strconv.Atoi(line)
		if err != nil {
			t.Fatalf("golden %s: %v", name, err)
		}
		lens = append(lens, n)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return lens
}

func firstDifference(a, b []int) string {
	off := 0
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return fmt.Sprintf("chunk %d at offset %d: %d bytes vs %d", i, off, a[i], b[i])
		}
		off += a[i]
	}
	return fmt.Sprintf("%d chunks vs %d", len(a), len(b))
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func checkGolden(t *testing.T, name string, size int, g dnx.Geometry) {
	t.Helper()
	data := corpus(size)
	got := dnxLengths(t, bytes.NewReader(data), g)
	ref := refLengths(data, g)
	if !equalInts(got, ref) {
		t.Fatalf("disknexus and the reference implementation disagree on %s: %s — "+
			"the chunker no longer implements the algorithm this repository depends on",
			name, firstDifference(got, ref))
	}
	if *update {
		header := fmt.Sprintf("# snapshot-core core/dnx/compat golden CDC chunk lengths\n"+
			"# corpus: SHA-256 counter mode, seed \"snapshot-core/compat/corpus/v1\", %d bytes\n"+
			"# geometry: min=%d max=%d mask=%#x\n", size, g.Min, g.Max, g.Mask)
		writeGolden(t, name, header, got)
	}
	want := readGolden(t, name)
	if !equalInts(got, want) {
		t.Fatalf("CDC boundaries for %s changed: %s — existing repositories would stop deduplicating "+
			"against everything already stored; a disknexus upgrade that does this must not ship",
			name, firstDifference(got, want))
	}
}

func TestCDCGoldenBoundariesRepoGeometry64MiB(t *testing.T) {
	checkGolden(t, "cdc_repo_64MiB.golden", 64<<20, repoGeometry)
}

func TestCDCGoldenBoundariesFineGeometry(t *testing.T) {
	checkGolden(t, "cdc_fine_4MiB.golden", 4<<20, fineGeometry)
}

// variedReader returns reads of 1..8191 bytes in a fixed pattern.
type variedReader struct {
	data []byte
	n    int
}

func (r *variedReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	r.n = (r.n*7 + 13) % 8191
	k := min(r.n+1, len(p), len(r.data))
	copy(p, r.data[:k])
	r.data = r.data[k:]
	return k, nil
}

func TestCDCBoundariesDoNotDependOnReadSizes(t *testing.T) {
	data := corpus(8 << 20)
	whole := dnxLengths(t, bytes.NewReader(data), repoGeometry)
	if len(whole) < 10 {
		t.Fatalf("8 MiB produced %d chunks; the fixture cannot distinguish anything", len(whole))
	}
	for name, r := range map[string]io.Reader{
		"one byte at a time": iotest.OneByteReader(bytes.NewReader(data)),
		"varied reads":       &variedReader{data: data},
		"half reads":         iotest.HalfReader(bytes.NewReader(data)),
	} {
		if got := dnxLengths(t, r, repoGeometry); !equalInts(got, whole) {
			t.Errorf("%s: boundaries moved with the reader's read sizes (%s) — two machines reading the same "+
				"file differently would not deduplicate", name, firstDifference(got, whole))
		}
	}
}

func TestCDCMaxIsHardAndChunksReassemble(t *testing.T) {
	data := corpus(16 << 20)
	c := dnx.NewChunker(bytes.NewReader(data), repoGeometry)
	var joined []byte
	chunks := 0
	for {
		ch, err := c.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if len(ch.Data) > repoGeometry.Max {
			t.Fatalf("chunk %d is %d bytes, above the %d maximum — the chunk store refuses chunks over its limit",
				chunks, len(ch.Data), repoGeometry.Max)
		}
		joined = append(joined, ch.Data...)
		chunks++
	}
	if chunks < 10 {
		t.Fatalf("16 MiB produced %d chunks; the fixture cannot distinguish anything", chunks)
	}
	if !bytes.Equal(joined, data) {
		t.Fatal("chunks do not reassemble to the input: a file read back would differ from the file written")
	}
}

// The core keeps chunk data after asking for the next chunk; it must be ours.
func TestCDCChunkDataIsOwnedByTheCaller(t *testing.T) {
	data := corpus(4 << 20)
	c := dnx.NewChunker(bytes.NewReader(data), fineGeometry)
	var kept [][]byte
	for {
		ch, err := c.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		kept = append(kept, ch.Data)
	}
	if len(kept) < 10 {
		t.Fatalf("got %d chunks; the fixture cannot distinguish anything", len(kept))
	}
	if joined := bytes.Join(kept, nil); !bytes.Equal(joined, data) {
		t.Fatal("chunk data retained across Next calls was overwritten: the chunker reuses its buffer")
	}
}

// --- AEAD ---

func testKey() []byte {
	k := sha256.Sum256([]byte("snapshot-core/compat/aead-key"))
	return k[:]
}

// The format is the contract: nonce || ciphertext || tag, AES-256-GCM, with the
// domain tag as associated data. Checked by opening with the standard library.
func TestAEADFormatIsNonceCiphertextTag(t *testing.T) {
	a, err := dnx.NewAEAD(testKey())
	if err != nil {
		t.Fatal(err)
	}
	defer a.Destroy()
	pt := []byte("a chunk's compressed bytes")
	aad := []byte("vdb/chunk/v1\x00context")
	sealed, err := a.Seal(pt, aad)
	if err != nil {
		t.Fatal(err)
	}
	if len(sealed) != len(pt)+dnx.Overhead {
		t.Fatalf("sealed length %d, want plaintext + %d", len(sealed), dnx.Overhead)
	}
	block, err := aes.NewCipher(testKey())
	if err != nil {
		t.Fatal(err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}
	got, err := gcm.Open(nil, sealed[:dnx.NonceSize], sealed[dnx.NonceSize:], aad)
	if err != nil {
		t.Fatalf("the standard library cannot open disknexus's sealed bytes (%v): the format changed "+
			"and every existing pack would be unreadable", err)
	}
	if !bytes.Equal(got, pt) {
		t.Fatalf("opened %q, want %q", got, pt)
	}
	back, err := a.Open(sealed, aad)
	if err != nil || !bytes.Equal(back, pt) {
		t.Fatalf("round trip: %q, %v", back, err)
	}
}

func TestAEADBindsTheDomainTag(t *testing.T) {
	a, err := dnx.NewAEAD(testKey())
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := a.Seal([]byte("payload"), []byte("vdb/chunk/v1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Open(sealed, []byte("vdb/chunk/v1")); err != nil {
		t.Fatalf("positive control: the right tag did not open: %v", err)
	}
	if _, err := a.Open(sealed, []byte("vdb/refs/v1")); err == nil {
		t.Error("a chunk ciphertext opened under the refs tag: no domain separation")
	}
	if _, err := a.Open(sealed, nil); err == nil {
		t.Error("an untagged Open was accepted: the untagged disknexus calls must not be reachable")
	}
	if _, err := a.Seal([]byte("payload"), nil); err == nil {
		t.Error("an untagged Seal was accepted: the untagged disknexus calls must not be reachable")
	}
	tampered := bytes.Clone(sealed)
	tampered[dnx.NonceSize] ^= 1
	if _, err := a.Open(tampered, []byte("vdb/chunk/v1")); err == nil {
		t.Error("a flipped ciphertext byte opened: no authentication")
	}
}

func TestAEADSealIsRandomizedAndKeySized(t *testing.T) {
	a, err := dnx.NewAEAD(testKey())
	if err != nil {
		t.Fatal(err)
	}
	s1, err1 := a.Seal([]byte("same"), []byte("tag"))
	s2, err2 := a.Seal([]byte("same"), []byte("tag"))
	if err1 != nil || err2 != nil {
		t.Fatal(err1, err2)
	}
	if bytes.Equal(s1[:dnx.NonceSize], s2[:dnx.NonceSize]) {
		t.Error("two seals reused a nonce: GCM with a repeated nonce leaks the key stream")
	}
	for _, n := range []int{0, 16, 31, 33} {
		if _, err := dnx.NewAEAD(make([]byte, n)); err == nil {
			t.Errorf("a %d-byte key was accepted for AES-256", n)
		}
	}
}

// --- Argon2id ---

func TestArgon2idKEKIsPlainArgon2id(t *testing.T) {
	p := dnx.Argon2Params{Time: 1, Memory: 8 * 1024, Threads: 2}
	salt := []byte("0123456789abcdef")
	got, err := dnx.DeriveKEK([]byte("correct horse"), salt, p)
	if err != nil {
		t.Fatal(err)
	}
	want := argon2.IDKey([]byte("correct horse"), salt, p.Time, p.Memory, p.Threads, 32)
	if !bytes.Equal(got, want) {
		t.Fatalf("DeriveKEK is not Argon2id with the given parameters: %x vs %x — "+
			"every passphrase-wrapped key would stop opening", got[:8], want[:8])
	}
}

func TestArgon2RefusesUnusableParams(t *testing.T) {
	salt := []byte("0123456789abcdef")
	if _, err := dnx.DeriveKEK([]byte("pw"), salt, dnx.Argon2Params{Time: 1, Memory: 64, Threads: 1}); err != nil {
		t.Fatalf("positive control: minimal valid params refused: %v", err)
	}
	for name, p := range map[string]dnx.Argon2Params{
		"time 0":    {Time: 0, Memory: 64, Threads: 1},
		"threads 0": {Time: 1, Memory: 64, Threads: 0},
		"memory":    {Time: 1, Memory: 7, Threads: 1},
	} {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("%s: DeriveKEK panicked (%v) instead of refusing", name, r)
				}
			}()
			if _, err := dnx.DeriveKEK([]byte("pw"), salt, p); err == nil {
				t.Errorf("%s: unusable Argon2 parameters were accepted", name)
			}
		}()
	}
}

func TestDefaultArgon2ParamsAreDisknexusDefaults(t *testing.T) {
	if got, want := dnx.DefaultArgon2Params(), (dnx.Argon2Params{Time: 3, Memory: 64 * 1024, Threads: 4}); got != want {
		t.Fatalf("default Argon2 params %+v, want %+v", got, want)
	}
}

// --- X25519 secret wrapping ---

// The layout is ephemeral public key (32) || nonce (12) || AES-GCM(secret),
// keyed by HKDF-SHA-256(X25519 shared secret, no salt, "disknexus-secret-wrap-v1").
// Checked by unwrapping with the standard library alone.
func TestX25519WrapLayoutIsECIES(t *testing.T) {
	pub, priv, err := dnx.GenerateX25519()
	if err != nil {
		t.Fatal(err)
	}
	secret := bytes.Repeat([]byte{0xA5}, 32)
	wrapped, err := dnx.WrapSecret(pub, secret)
	if err != nil {
		t.Fatal(err)
	}
	if len(wrapped) != len(secret)+dnx.SecretWrapOverhead {
		t.Fatalf("wrapped length %d, want %d", len(wrapped), len(secret)+dnx.SecretWrapOverhead)
	}
	sk, err := ecdh.X25519().NewPrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	eph, err := ecdh.X25519().NewPublicKey(wrapped[:32])
	if err != nil {
		t.Fatal(err)
	}
	shared, err := sk.ECDH(eph)
	if err != nil {
		t.Fatal(err)
	}
	key, err := hkdf.Key(sha256.New, shared, nil, "disknexus-secret-wrap-v1", 32)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := aes.NewCipher(key)
	gcm, _ := cipher.NewGCM(block)
	got, err := gcm.Open(nil, wrapped[32:32+dnx.NonceSize], wrapped[32+dnx.NonceSize:], nil)
	if err != nil {
		t.Fatalf("the standard library cannot unwrap the secret (%v): the wrap layout changed", err)
	}
	if !bytes.Equal(got, secret) {
		t.Fatal("unwrapped secret differs")
	}
	back, err := dnx.UnwrapSecret(priv, wrapped)
	if err != nil || !bytes.Equal(back, secret) {
		t.Fatalf("round trip: %x, %v", back, err)
	}
	wrapped[len(wrapped)-1] ^= 1
	if _, err := dnx.UnwrapSecret(priv, wrapped); err == nil {
		t.Error("a tampered wrapped secret unwrapped")
	}
}
