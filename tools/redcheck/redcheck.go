// Command redcheck proves that every `test:` commit on a branch was red: it
// checks each one out in a scratch worktree, runs only the Test functions that
// commit added, and requires every one of them to FAIL on an assertion. A test
// that passes against the old code is not testing the change; a build failure
// or a panic is the wrong red (CONTRIBUTING.md, Engine Spec "fail-first TDD").
//
// It also requires every `feat:`/`fix:` commit to be preceded by at least one
// `test:` commit since the previous `feat:`/`fix:` — behavior arrives behind a
// test that failed first.
package main

import (
	"context"
	"io"
)

// Options configures a check.
type Options struct {
	Dir   string    // repository root
	Base  string    // the branch point, e.g. "main" or "origin/main"
	Head  string    // defaults to HEAD
	GoCmd string    // defaults to "go"
	Log   io.Writer // progress; nil discards
}

// Violation is one broken rule.
type Violation struct {
	Commit  string // abbreviated hash
	Subject string
	Test    string // the offending test, when there is one
	Reason  string
}

// Report is the outcome of a check. Checked and TestsRun exist so that a check
// that scanned nothing cannot look like a pass (docs/TESTING.md §8).
type Report struct {
	Commits    int // commits in base..head
	Checked    int // test: commits checked
	TestsRun   int // added tests executed across all test: commits
	Violations []Violation
}

// Check runs the red-check over base..head.
func Check(ctx context.Context, o Options) (Report, error) {
	return Report{}, nil // stub: judges nothing
}
