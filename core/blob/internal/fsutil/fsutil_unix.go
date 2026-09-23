//go:build unix

package fsutil

import (
	"errors"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// SyncDir makes a directory's entries (a new file's name, a rename) durable.
func SyncDir(dir string) error {
	d, err := os.Open(dir) //nolint:gosec // G304: a directory inside the store, never user input
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}

// Lock takes an exclusive flock on path. The kernel drops it when the process
// dies, so a kill -9 never leaves the store locked.
func Lock(path string) (func(), error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, FilePerm) //nolint:gosec // G304: the store's own lock file
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX); err != nil {
		_ = f.Close()
		return nil, err
	}
	return func() {
		_ = unix.Flock(int(f.Fd()), unix.LOCK_UN)
		_ = f.Close()
	}, nil
}

// Identity is a directory's device and inode. An unmount or a swap changes it.
func Identity(path string) (dev, ino uint64, err error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, 0, err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, errors.New("fsutil: no device identity on this platform")
	}
	return uint64(st.Dev), st.Ino, nil //nolint:unconvert // Dev is int32 on darwin, uint64 on linux
}

// FreeSpace reports the bytes available to an unprivileged writer, and the total.
func FreeSpace(path string) (free, total uint64, err error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return 0, 0, err
	}
	return st.Bavail * uint64(st.Bsize), st.Blocks * uint64(st.Bsize), nil
}
