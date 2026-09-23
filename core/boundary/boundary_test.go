package boundary_test

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/boundary"
)

func rule(t *testing.T) boundary.Rule {
	t.Helper()
	r, err := boundary.New(boundary.DefaultGeometry())
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// The documented constants, and λ against an independent float computation.
func TestDefaultGeometryAndScale(t *testing.T) {
	if g := boundary.DefaultGeometry(); g != (boundary.Geometry{Min: 512, Target: 4096, Max: 16384}) {
		t.Fatalf("DefaultGeometry = %+v, want 512 / 4096 / 16384 (Engine Spec L1)", g)
	}
	r := rule(t)
	if want := uint64(math.Round(4096 / math.Gamma(1.25))); r.Lambda() != want || want != 4519 {
		t.Fatalf("λ = %d, want round(4096 / Γ(5/4)) = %d (DESIGN §7: 4519)", r.Lambda(), want)
	}
}

// T = ⌊(after⁴ − before⁴) · 2³² / λ⁴⌋, checked with math/big as the authority.
func TestThresholdIsTheEntrysShareOfTheHazard(t *testing.T) {
	r := rule(t)
	lambda4 := new(big.Int).Exp(big.NewInt(4519), big.NewInt(4), nil)
	for _, c := range [][2]int{{0, 600}, {500, 600}, {4000, 4100}, {4096, 8192}, {10000, 16383}, {1000, 1001}} {
		b, a := big.NewInt(int64(c[0])), big.NewInt(int64(c[1]))
		d := new(big.Int).Sub(new(big.Int).Exp(a, big.NewInt(4), nil), new(big.Int).Exp(b, big.NewInt(4), nil))
		want := new(big.Int).Div(new(big.Int).Lsh(d, 32), lambda4)
		if got := r.Threshold(c[0], c[1]); want.Cmp(new(big.Int).SetUint64(got)) != 0 {
			t.Errorf("Threshold(%d, %d) = %d, want %s", c[0], c[1], got, want)
		}
	}
	if r.Threshold(0, 600) >= r.Threshold(4000, 4600) || r.Threshold(4000, 4600) >= r.Threshold(8000, 8600) {
		t.Error("the threshold for the same 600 bytes does not rise with the node's size")
	}
}

// Window L < 8 is bytes [4L, 4L+4) big-endian; L ≥ 8 is that window of
// SHA-256(digest ‖ byte(L/8)).
func TestWindowsAreFreshPerLevel(t *testing.T) {
	d := sha256.Sum256([]byte("a key"))
	for l := 0; l < 8; l++ {
		if got, want := boundary.Window(d, l), binary.BigEndian.Uint32(d[4*l:]); got != want {
			t.Errorf("Window(level %d) = %08x, want %08x", l, got, want)
		}
	}
	for _, l := range []int{8, 13, 16, 63} {
		re := sha256.Sum256(append(d[:], byte(l/8)))
		if got, want := boundary.Window(d, l), binary.BigEndian.Uint32(re[4*(l%8):]); got != want {
			t.Errorf("Window(level %d) = %08x, want %08x", l, got, want)
		}
	}
}

func digest(i int) [32]byte { return sha256.Sum256([]byte(fmt.Sprintf("key-%d", i))) }

// A digest whose level-0 window is 0 ends a node wherever the rule may end
// one; one whose window is all ones ends it only where it must.
func eager() (d [32]byte) { return }

func reluctant() (d [32]byte) {
	for i := range d {
		d[i] = 0xff
	}
	return
}

func TestHardMinimumAndMaximum(t *testing.T) {
	r := rule(t)
	s := r.Splitter(0)
	if s.Append(eager(), 511) {
		t.Error("a leaf ended at 511 bytes, under the 512-byte minimum")
	}
	if !s.Append(eager(), 1) {
		t.Error("positive control: an eager digest did not end a leaf at 512 bytes")
	}
	// 20-byte entries never carry a whole unit of hazard below 16 KiB
	// (4·s³·20 < λ⁴), so only the maximum can end this leaf.
	s = r.Splitter(0)
	for size := 0; size < 16384-20; size += 20 {
		if s.Append(reluctant(), 20) {
			t.Fatalf("a reluctant digest ended a leaf at %d bytes", size+20)
		}
	}
	if !s.Append(reluctant(), 20) {
		t.Error("a leaf reached 16 KiB and did not end")
	}
}

// At levels ≥ 1 a node takes two entries before it may end, so every level
// shrinks and height is bounded; a leaf may end after one.
func TestInternalNodesTakeTwoEntries(t *testing.T) {
	r := rule(t)
	if !r.Splitter(0).Append(eager(), 4200) {
		t.Error("positive control: one large leaf entry did not end its leaf")
	}
	for _, level := range []int{1, 2, 9} {
		s := r.Splitter(level)
		if s.Append(eager(), 4200) {
			t.Errorf("level %d: a node ended after one entry", level)
		}
		if !s.Append(eager(), 4200) {
			t.Errorf("level %d: a second eager entry did not end the node", level)
		}
	}
}

// Nodes cluster around the target: over many random entries every node but
// the last is within the bounds, and the mean is near 4 KiB.
func TestNodeSizesClusterAroundTheTarget(t *testing.T) {
	r := rule(t)
	s := r.Splitter(0)
	var sizes []int
	size := 0
	for i := 0; i < 200000; i++ {
		n := 20 + i%180 // 20..199 bytes
		size += n
		if s.Append(digest(i), n) {
			sizes = append(sizes, size)
			size = 0
		}
	}
	if len(sizes) < 1000 {
		t.Fatalf("200,000 entries made %d nodes, want thousands", len(sizes))
	}
	total := 0
	for _, n := range sizes {
		total += n
		if n < 512 || n >= 16384+200 {
			t.Fatalf("a node of %d bytes, outside 512 B .. 16 KiB", n)
		}
	}
	if mean := total / len(sizes); mean < 3700 || mean > 4500 {
		t.Fatalf("mean node size %d, want about 4096", mean)
	}
}

// The same entries end nodes at the same places, pinned to a golden digest
// of the boundary positions (a change to the rule changes every tree).
func TestBoundariesAreAPureFunctionOfContent(t *testing.T) {
	positions := func() []byte {
		s := rule(t).Splitter(0)
		var out []byte
		for i := 0; i < 50000; i++ {
			if s.Append(digest(i), 40+i%60) {
				out = binary.BigEndian.AppendUint32(out, uint32(i))
			}
		}
		return out
	}
	a, b := positions(), positions()
	if len(a) == 0 || string(a) != string(b) {
		t.Fatalf("two runs over the same entries ended %d and %d nodes, or at different places", len(a)/4, len(b)/4)
	}
	const golden = "a5fdc1c9157bd7fa7edff3ddb23fd12bf74448c3a200d70e533f8f779b359464" // from an independent reimplementation of DESIGN §7: 834 nodes
	if got := fmt.Sprintf("%x", sha256.Sum256(a)); got != golden {
		t.Fatalf("boundary positions digest %s, want %s (%d nodes)", got, golden, len(a)/4)
	}
}

func TestGeometryIsValidated(t *testing.T) {
	if _, err := boundary.New(boundary.DefaultGeometry()); err != nil {
		t.Fatalf("positive control: %v", err)
	}
	for name, g := range map[string]boundary.Geometry{
		"zero":               {},
		"min over target":    {Min: 5000, Target: 4096, Max: 16384},
		"target over max":    {Min: 512, Target: 20000, Max: 16384},
		"min under 64":       {Min: 32, Target: 4096, Max: 16384},
		"max over the chunk": {Min: 512, Target: 4096, Max: 2 << 20},
		"target under 1 KiB": {Min: 128, Target: 512, Max: 16384},
	} {
		if _, err := boundary.New(g); !errors.Is(err, boundary.ErrGeometry) {
			t.Errorf("%s: New(%+v) = %v, want ErrGeometry", name, g, err)
		}
	}
}
