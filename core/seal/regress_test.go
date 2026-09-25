package seal_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
)

// errNotPaid stops a derivation a test only wanted to see requested.
var errNotPaid = errors.New("test: derivation observed, not paid for")

// #25: FuzzOpenKeyFile reached the Argon2 ceiling, a gibibyte and ten
// passes, by rewriting four bytes of its seed: 1 GB for 8 s a run, so four
// fuzz workers held 4 GiB and GOMEMLIMIT (soft) could not stop them. The
// fuzzer must decode such a file and never derive above its budget. The
// derivation is observed and refused here, so this test pays for none.
func TestRegression_SC25_TheKeyFileFuzzerNeverDerivesAboveItsBudget(t *testing.T) {
	seed, err := seal.NewKeyFile(mustKeyring(t), []byte("pw"), fast)
	if err != nil {
		t.Fatal(err)
	}
	var asked []seal.Argon2Params
	record := func(p seal.Argon2Params) error {
		asked = append(asked, p)
		if !withinFuzzBudget(p) {
			return errNotPaid
		}
		return nil
	}

	// Positive control: the seed, at the floor, reaches its one derivation.
	fuzzOpenKeyFile(t, seed, record)
	if len(asked) != 1 || asked[0] != fast {
		t.Fatalf("positive control: the fuzzer's seed asked for derivations %+v, want one at %+v: "+
			"the fixture never reaches key derivation, so it proves nothing", asked, fast)
	}

	// Four bytes of the seed rewritten: memory (offset 10) to the ceiling
	// and time (offset 6) to ten passes. It still decodes and validates.
	atCeiling := bytes.Clone(seed)
	binary.LittleEndian.PutUint32(atCeiling[6:], 10)
	binary.LittleEndian.PutUint32(atCeiling[10:], 1024*1024)
	asked = nil
	fuzzOpenKeyFile(t, atCeiling, record)
	for _, p := range asked {
		if !withinFuzzBudget(p) {
			t.Errorf("the key-file fuzzer let a derivation run at %d MiB and %d passes (budget %d MiB, %d passes): "+
				"each such run holds that memory for seconds, per fuzz worker, and took the host down",
				p.Memory/1024, p.Time, fuzzKDFBudget.Memory/1024, fuzzKDFBudget.Time)
		}
	}
}
