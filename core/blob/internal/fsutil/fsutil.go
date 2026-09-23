// Package fsutil is the durable-filesystem toolkit the disk backends share:
// directory fsync, flock, atomic file replacement, a directory's identity,
// and free space. One audited copy, so blob/local and blob/multivol cannot
// drift apart on the details that make a crash safe.
package fsutil

import (
	"os"
	"path/filepath"
)

// FilePerm and DirPerm are the only modes the disk backends create.
const (
	FilePerm = 0o600
	DirPerm  = 0o700
)

// WriteFileAtomic replaces path with b: write a temp file beside it, fsync,
// rename over path, fsync the directory. A crash leaves the old file or the
// new one, never a torn one.
func WriteFileAtomic(path string, b []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer os.Remove(name) // no-op after the rename
	_, err = tmp.Write(b)
	if err == nil {
		err = tmp.Chmod(FilePerm)
	}
	if err == nil {
		err = tmp.Sync()
	}
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	return SyncDir(dir)
}
