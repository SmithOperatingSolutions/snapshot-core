//go:build linux

package local

import "golang.org/x/sys/unix"

func detectFS(path string) (string, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return "", err
	}
	return fsName(int64(st.Type)), nil //nolint:unconvert // Type is int64 on amd64/arm64 but int32 on some linux arches
}
