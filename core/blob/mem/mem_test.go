package mem_test

import (
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/contract"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/mem"
)

func TestContract(t *testing.T) {
	contract.Run(t, func(t *testing.T) blob.BlobStore { return mem.New() }, contract.Options{})
}
