package local

import "github.com/SmithOperatingSolutions/snapshot-core/core/blob"

// WithFSType returns Options whose filesystem detector is replaced, so a test
// can present NFS, SMB or an unknown filesystem without mounting one.
func WithFSType(f func(path string) (string, error)) Options { return Options{fsType: f} }

// DetectFS is the real detector.
func DetectFS(path string) (string, error) { return detectFS(path) }

// FSName exposes the magic-number table.
func FSName(magic int64) string { return fsName(magic) }

// DecodeRoot and EncodeRoot expose the root file codec for fuzzing.
func DecodeRoot(b []byte) ([]byte, string, error) {
	r, err := decodeRoot(b)
	return r.Value, string(r.Version), err
}

// EncodeRoot is the root file encoder.
func EncodeRoot(version string, value []byte) []byte { return encodeRoot(blob.Version(version), value) }

// ParseMarker exposes the marker decoder for fuzzing.
func ParseMarker(b []byte) (string, error) { return parseMarker(b) }
