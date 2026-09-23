//go:build !linux && !darwin

package local

func detectFS(path string) (string, error) { return "", ErrUnsupportedFilesystem }
