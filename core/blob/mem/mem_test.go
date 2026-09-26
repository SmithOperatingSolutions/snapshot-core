package mem_test

import (
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/contract"
	jcontract "github.com/SmithOperatingSolutions/snapshot-core/core/blob/journal/contract"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/mem"
)

func TestContract(t *testing.T) {
	contract.Run(t, func(t *testing.T) blob.BlobStore { return mem.New() }, contract.Options{})
}

// The journal's contract (#34); a store opened again is the same memory.
func TestJournalContract(t *testing.T) {
	jcontract.Run(t, func(t *testing.T) (blob.Journaler, func(*testing.T) blob.Journaler) {
		s := mem.New()
		return s, func(*testing.T) blob.Journaler { return s }
	})
}
