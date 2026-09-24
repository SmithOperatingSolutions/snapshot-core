package cdc

import (
	"crypto/sha256"
	"encoding/binary"
	"math/bits"
	"strconv"
)

// The Buzhash disknexus cuts with (core/dnx/compat's goldens pin it): a
// 48-byte window, a table of 256 uint64s each the little-endian first eight
// bytes of SHA-256("disknexus-buzhash-v1-<i>"), and the rolling update
// hash = rotl(hash, 1) ^ rotl(table[out], window) ^ table[in], starting from
// the hash of an all-zero window and carried across chunk boundaries.
const window = 48

// table is the Buzhash table; outTable[b] is rotl(table[b], window), the
// contribution a byte leaving the window takes with it.
var table, outTable = buildTables()

// zeroWindowHash is the hash of a window of 48 zero bytes, the state a
// stream starts in.
var zeroWindowHash = func() uint64 {
	var h uint64
	for i := range window {
		h ^= bits.RotateLeft64(table[0], window-1-i)
	}
	return h
}()

func buildTables() (in, out [256]uint64) {
	for i := range in {
		h := sha256.Sum256([]byte("disknexus-buzhash-v1-" + strconv.Itoa(i)))
		in[i] = binary.LittleEndian.Uint64(h[:8])
		out[i] = bits.RotateLeft64(in[i], window)
	}
	return in, out
}
