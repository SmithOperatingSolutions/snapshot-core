//go:build darwin

package local

import "golang.org/x/sys/unix"

func detectFS(path string) (string, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return "", err
	}
	return unix.ByteSliceToString(st.Fstypename[:]), nil
}
