package memstore_test

import (
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/contract"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/memstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
)

func TestContract(t *testing.T) {
	contract.Run(t, func(t *testing.T) contract.Subject {
		s := memstore.New()
		return contract.Subject{
			Store: s,
			Tamper: func(t *testing.T, h hash.Hash) {
				if !s.Tamper(h) {
					t.Fatalf("no chunk %s to tamper with", h.Short())
				}
			},
		}
	}, contract.Options{})
}
