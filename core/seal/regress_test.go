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

// #25: with the fuzzer kept under its budget, nothing fast exercises the
// ceiling; this does, without deriving. A key file at exactly the ceiling
// (a gibibyte, ten passes, 64 threads) passes validation and reaches its
// derivation, which is observed and not paid for; one step over on any
// cost is refused before derivation. The derivation at the ceiling itself
// is TestSlowAKeyFileAtTheCeilingOpens (-tags slow).
func TestRegression_SC25_AKeyFileAtTheCeilingIsAccepted(t *testing.T) {
	seed, err := seal.NewKeyFile(mustKeyring(t), []byte("pw"), fast)
	if err != nil {
		t.Fatal(err)
	}
	ceiling := seal.Argon2Params{Time: 10, Memory: 1024 * 1024, Threads: 64}
	file := func(p seal.Argon2Params) []byte {
		b := bytes.Clone(seed)
		binary.LittleEndian.PutUint32(b[6:], p.Time)
		binary.LittleEndian.PutUint32(b[10:], p.Memory)
		b[14] = p.Threads
		return b
	}
	var asked []seal.Argon2Params
	observe := func(p seal.Argon2Params) error {
		asked = append(asked, p)
		return errNotPaid
	}

	if _, err := seal.OpenKeyFileObserved(file(ceiling), []byte("pw"), observe); !errors.Is(err, errNotPaid) ||
		len(asked) != 1 || asked[0] != ceiling {
		t.Fatalf("a key file at the ceiling %+v: %v, derivations asked %+v; want it accepted and derived: "+
			"a host whose key file was written at the ceiling could not open its repository", ceiling, err, asked)
	}
	for name, p := range map[string]seal.Argon2Params{
		"eleven passes":             {Time: 11, Memory: ceiling.Memory, Threads: ceiling.Threads},
		"a gibibyte and a kibibyte": {Time: ceiling.Time, Memory: ceiling.Memory + 1, Threads: ceiling.Threads},
		"65 threads":                {Time: ceiling.Time, Memory: ceiling.Memory, Threads: 65},
	} {
		asked = nil
		if _, err := seal.OpenKeyFileObserved(file(p), []byte("pw"), observe); !errors.Is(err, seal.ErrParams) || len(asked) != 0 {
			t.Errorf("a key file one step over the ceiling (%s): %v, derivations asked %+v; want ErrParams before any derivation",
				name, err, asked)
		}
	}
}
