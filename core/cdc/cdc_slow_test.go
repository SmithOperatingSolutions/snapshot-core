//go:build slow

package cdc_test

import "testing"

func TestSlowOneByteInsertChangesAtMostThreeChunks1GiB(t *testing.T) {
	checkInsert(t, 1<<30)
}
