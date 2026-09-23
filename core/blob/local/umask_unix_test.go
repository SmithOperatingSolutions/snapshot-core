//go:build unix

package local_test

import "golang.org/x/sys/unix"

func setUmask(m int) func() {
	old := unix.Umask(m)
	return func() { unix.Umask(old) }
}
