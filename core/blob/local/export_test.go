package local

// WithFSType returns Options whose filesystem detector is replaced, so a test
// can present NFS, SMB or an unknown filesystem without mounting one.
func WithFSType(f func(path string) (string, error)) Options { return Options{fsType: f} }

// DetectFS is the real detector.
func DetectFS(path string) (string, error) { return detectFS(path) }

// FSName exposes the magic-number table.
func FSName(magic int64) string { return fsName(magic) }
