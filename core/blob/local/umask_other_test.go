//go:build !unix

package local_test

func setUmask(int) func() { return func() {} }
