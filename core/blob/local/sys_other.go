//go:build !unix

package local

// The local backend runs on Linux and macOS only; elsewhere Create and Open
// fail at the filesystem check, and these are never reached.

func syncDir(string) error { return ErrUnsupportedFilesystem }

func lockFile(string) (func(), error) { return nil, ErrUnsupportedFilesystem }
