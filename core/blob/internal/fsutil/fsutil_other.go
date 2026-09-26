//go:build !unix

package fsutil

import (
	"errors"
	"os"
)

// The disk backends run on Linux and macOS only.
var errUnsupported = errors.New("fsutil: unsupported platform")

// SyncDir is unsupported here.
func SyncDir(string) error { return errUnsupported }

// Lock is unsupported here.
func Lock(string) (func(), error) { return nil, errUnsupported }

// ErrLocked is TryLock's answer when another holder has the lock.
var ErrLocked = errors.New("fsutil: locked by another holder")

// TryLock is unsupported here.
func TryLock(*os.File, bool) error { return errUnsupported }

// Identity is unsupported here.
func Identity(string) (uint64, uint64, error) { return 0, 0, errUnsupported }

// FreeSpace is unsupported here.
func FreeSpace(string) (uint64, uint64, error) { return 0, 0, errUnsupported }
