// Package boundary is the content-defined split rule that prolly trees and
// stream index trees draw their nodes with (docs/DESIGN.md §7): a node ends
// after an entry when a window of the entry's digest falls under a threshold
// that rises with the node's size, computed in integers so that every
// platform draws the same tree.
package boundary

import "errors"

// ErrGeometry is returned for an unusable geometry.
var ErrGeometry = errors.New("boundary: invalid geometry")

// Geometry bounds the nodes the rule draws, in encoded bytes of entries.
type Geometry struct {
	Min    int // a node never ends below this
	Target int // the mean node size
	Max    int // a node always ends at or above this
}

// DefaultGeometry is the Engine Spec's 512 B / 4 KiB / 16 KiB.
func DefaultGeometry() Geometry { return Geometry{} }

// Rule is a validated geometry with its scale precomputed.
type Rule struct{}

// New validates g and precomputes its scale.
func New(g Geometry) (Rule, error) { return Rule{}, nil }

// Lambda is the rule's scale, round(Target / Γ(5/4)).
func (r Rule) Lambda() uint64 { return 0 }

// Threshold is T for an entry that takes a node from before to after bytes:
// the node ends when the entry's window is under T. 1<<32 or more is always.
func (r Rule) Threshold(before, after int) uint64 { return 0 }

// Window is the level-th 4-byte window of a digest, big-endian.
func Window(digest [32]byte, level int) uint32 { return 0 }

// Splitter tracks the node being built at one level.
type Splitter struct{}

// Splitter starts the first node of a level.
func (r Rule) Splitter(level int) *Splitter { return &Splitter{} }

// Append adds an entry of size encoded bytes whose digest is d, and reports
// whether the node ends after it; if so, the next entry starts a new node.
func (s *Splitter) Append(d [32]byte, size int) bool { return false }
