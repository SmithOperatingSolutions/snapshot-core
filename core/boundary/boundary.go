// Package boundary is the content-defined split rule that prolly trees and
// stream index trees draw their nodes with (docs/DESIGN.md §7): a node ends
// after an entry when a window of the entry's digest falls under a threshold
// that rises with the node's size, computed in integers so that every
// platform draws the same tree.
package boundary

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"

	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
)

// ErrGeometry is returned for an unusable geometry.
var ErrGeometry = errors.New("boundary: invalid geometry")

const (
	minMin    = 64
	minTarget = 1 << 10
	// maxMax keeps after⁴ under 2⁶⁰ and λ⁴ inside 64 bits, so a threshold
	// is one 128-bit multiply and divide.
	maxMax = 32 << 10
	// Γ(5/4) to nine places, for λ = round(Target / Γ(5/4)).
	gammaNum = 906_402_477
	gammaDen = 1_000_000_000
	always   = 1 << 32
)

// Geometry bounds the nodes the rule draws, in encoded bytes of entries.
type Geometry struct {
	Min    int // a node never ends below this
	Target int // the mean node size
	Max    int // a node always ends at or above this
}

// DefaultGeometry is the Engine Spec's 512 B / 4 KiB / 16 KiB.
func DefaultGeometry() Geometry { return Geometry{Min: 512, Target: 4 << 10, Max: 16 << 10} }

// Validate reports whether the rule can draw with g.
func (g Geometry) Validate() error {
	switch {
	case g.Min < minMin || g.Target < minTarget || g.Max > maxMax:
		return fmt.Errorf("%w: %+v outside min ≥ %d, target ≥ %d, max ≤ %d", ErrGeometry, g, minMin, minTarget, maxMax)
	case g.Min >= g.Target || g.Target >= g.Max:
		return fmt.Errorf("%w: %+v is not min < target < max", ErrGeometry, g)
	}
	return nil
}

// Rule is a validated geometry with its scale precomputed. Use New; the
// zero Rule is not usable.
type Rule struct {
	g       Geometry
	lambda  uint64
	lambda4 uint64
}

// New validates g and precomputes its scale.
func New(g Geometry) (Rule, error) {
	if err := g.Validate(); err != nil {
		return Rule{}, err
	}
	lambda := (uint64(g.Target)*gammaDen + gammaNum/2) / gammaNum
	return Rule{g: g, lambda: lambda, lambda4: lambda * lambda * lambda * lambda}, nil
}

// Lambda is the rule's scale, round(Target / Γ(5/4)).
func (r Rule) Lambda() uint64 { return r.lambda }

// Threshold is T for an entry that takes a node from before to after bytes:
// the node ends when the entry's window is under T. 1<<32 or more is always,
// as is any size at or over Max.
func (r Rule) Threshold(before, after int) uint64 {
	if after >= r.g.Max {
		return always
	}
	before = max(before, 0)
	if after <= before {
		return 0 // an entry that adds nothing adds no hazard
	}
	a, b := uint64(after), uint64(before)
	hi, lo := bits.Mul64(a*a*a*a-b*b*b*b, 1<<32)
	t, _ := bits.Div64(hi, lo, r.lambda4) // hi < 2²⁸ < λ⁴: no overflow
	return t
}

// Window is the level-th 4-byte window of a digest, big-endian: bytes
// [4L, 4L+4) for L < 8, and that window of SHA-256(digest ‖ byte(L/8)) above.
func Window(digest [32]byte, level int) uint32 {
	if level >= 8 {
		digest = hash.Sum(append(digest[:], byte(level/8)))
	}
	return binary.BigEndian.Uint32(digest[4*(level%8):])
}

// Splitter tracks the node being built at one level.
type Splitter struct {
	rule  Rule
	level int
	size  int // encoded bytes of the node's entries so far
	n     int // entries so far
}

// Splitter starts the first node of a level.
func (r Rule) Splitter(level int) *Splitter { return &Splitter{rule: r, level: level} }

// Append adds an entry of size encoded bytes whose digest is d, and reports
// whether the node ends after it; if so, the next entry starts a new node.
func (s *Splitter) Append(d [32]byte, size int) bool {
	before := s.size
	s.size += size
	s.n++
	if !s.ends(d, before) {
		return false
	}
	s.size, s.n = 0, 0
	return true
}

func (s *Splitter) ends(d [32]byte, before int) bool {
	switch {
	case s.level >= 1 && s.n < 2:
		return false // every level shrinks, so height is bounded
	case s.size < s.rule.g.Min:
		return false
	}
	t := s.rule.Threshold(before, s.size) // always at or over Max
	return t >= always || uint64(Window(d, s.level)) < t
}
