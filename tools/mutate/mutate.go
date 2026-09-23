// Command mutate re-runs the repository's checked-in mutants: each one severs
// a load-bearing guard, and the named test must go red (docs/TESTING.md §5).
// Mutants run in a throwaway copy of the tree, never the working tree (§5);
// every mutant is build-checked so a compile error cannot pose as a survivor
// (§13); and a survivor is reported, never dropped (§10).
package main

import (
	"context"
	"io"
)

// Mutant is one deliberate defect and the test that must catch it.
type Mutant struct {
	ID      string // short name, unique in the file
	File    string // path relative to the repository root
	Find    string // exact text; must occur exactly once in File
	Replace string // what it becomes
	Pkg     string // package to test, e.g. ./core/blob/mem
	Run     string // -run pattern naming the guarding test
	Line    int    // line in the mutants file, for messages
}

// Outcome is what happened to one mutant.
type Outcome struct {
	Mutant Mutant
	Status Status
	Detail string
}

// Status classifies an outcome.
type Status int

const (
	Killed   Status = iota // the test went red: the guard has teeth
	Survived               // the test stayed green: the guard is decoration
	Invalid                // the mutant could not be applied or did not compile
)

func (s Status) String() string {
	switch s {
	case Killed:
		return "killed"
	case Survived:
		return "SURVIVED"
	default:
		return "INVALID"
	}
}

// Parse reads a mutants file.
func Parse(r io.Reader) ([]Mutant, error) {
	return nil, nil // stub
}

// Options configures a run.
type Options struct {
	Root  string    // repository root to copy
	GoCmd string    // defaults to "go"
	Log   io.Writer // progress; nil discards
}

// Run applies each mutant in a throwaway copy of Root and reports outcomes.
func Run(ctx context.Context, o Options, ms []Mutant) ([]Outcome, error) {
	return nil, nil // stub
}
