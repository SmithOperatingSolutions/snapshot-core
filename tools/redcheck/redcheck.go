// Command redcheck proves that every `test:` commit on a branch was red: it
// checks each one out in a scratch worktree, runs only the Test functions that
// commit added or changed, and requires every one of them to FAIL on an
// assertion. A test that passes against the old code is not testing the
// change; a build failure, a panic or a skip is the wrong red
// (CONTRIBUTING.md, Engine Spec "fail-first TDD").
//
// It also requires every `feat:`/`fix:` commit to be preceded by at least one
// `test:` commit since the previous `feat:`/`fix:` — behavior arrives behind a
// test that failed first.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/SmithOperatingSolutions/snapshot-core/tools/internal/mutation"
)

// Options configures a check.
type Options struct {
	Dir   string    // repository root
	Base  string    // the branch point, e.g. "main" or "origin/main"
	Head  string    // defaults to HEAD
	GoCmd string    // defaults to "go"
	Log   io.Writer // progress; nil discards

	// MutantsFile is where a backfill commit's mutants are defined, relative
	// to the repository root at that commit. Defaults to tools/mutate/mutants.txt.
	MutantsFile string
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
	TestsRun   int // added or changed tests executed across all test: commits
	Violations []Violation
}

var (
	testSubject = regexp.MustCompile(`^test(\([^)]*\))?!?:`)
	featSubject = regexp.MustCompile(`^(feat|fix)(\([^)]*\))?!?:`)
	testFunc    = regexp.MustCompile(`^Test([^a-z].*)?$`)
	redTrailer  = regexp.MustCompile(`(?m)^Red-Check:\s*mutants\s+(.+)$`)
)

// Check runs the red-check over base..head.
func Check(ctx context.Context, o Options) (Report, error) {
	if o.Head == "" {
		o.Head = "HEAD"
	}
	if o.GoCmd == "" {
		o.GoCmd = "go"
	}
	if o.Log == nil {
		o.Log = io.Discard
	}
	if o.MutantsFile == "" {
		o.MutantsFile = "tools/mutate/mutants.txt"
	}
	c := checker{ctx: ctx, o: o}

	out, err := c.git("rev-list", "--reverse", o.Base+".."+o.Head)
	if err != nil {
		return Report{}, err
	}
	var rep Report
	sawTest := false
	for _, commit := range strings.Fields(out) {
		rep.Commits++
		subject, err := c.git("log", "-1", "--format=%s", commit)
		if err != nil {
			return rep, err
		}
		subject = strings.TrimSpace(subject)
		short := commit[:min(len(commit), 9)]
		switch {
		case testSubject.MatchString(subject):
			sawTest = true
			rep.Checked++
			body, err := c.git("log", "-1", "--format=%B", commit)
			if err != nil {
				return rep, err
			}
			vs, ran, err := c.checkTestCommit(commit, backfillMutants(body))
			if err != nil {
				return rep, fmt.Errorf("%s %q: %w", short, subject, err)
			}
			rep.TestsRun += ran
			for _, v := range vs {
				v.Commit, v.Subject = short, subject
				rep.Violations = append(rep.Violations, v)
			}
		case featSubject.MatchString(subject):
			if !sawTest {
				rep.Violations = append(rep.Violations, Violation{Commit: short, Subject: subject,
					Reason: "no test: commit since the previous feat:/fix: — behavior must arrive behind a test that failed first"})
			}
			sawTest = false
		}
	}
	return rep, nil
}

type checker struct {
	ctx context.Context
	o   Options
}

func (c checker) git(args ...string) (string, error) {
	cmd := exec.CommandContext(c.ctx, "git", args...)
	cmd.Dir = c.o.Dir
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return string(out), nil
}

// changedTests returns, per package directory, the Test functions that exist
// at commit and either did not exist at its parent or have a different body.
func (c checker) changedTests(commit string) (map[string][]string, error) {
	parent := commit + "^"
	if _, err := c.git("rev-parse", "--verify", "--quiet", parent); err != nil {
		parent = "" // root commit: everything is new
	}
	var files string
	var err error
	if parent == "" {
		files, err = c.git("show", "--name-only", "--format=", commit)
	} else {
		files, err = c.git("diff", "--name-only", "--diff-filter=AM", parent, commit)
	}
	if err != nil {
		return nil, err
	}
	changed := map[string][]string{}
	for _, f := range strings.Fields(files) {
		if !strings.HasSuffix(f, "_test.go") {
			continue
		}
		now, err := c.git("show", commit+":"+f)
		if err != nil {
			return nil, err
		}
		before := ""
		if parent != "" {
			before, _ = c.git("show", parent+":"+f) // absent at parent: new file
		}
		nowFns, err := testBodies(f, now)
		if err != nil {
			return nil, err
		}
		beforeFns, _ := testBodies(f, before) // an unparsable parent just means "all new"
		for name, body := range nowFns {
			if prev, ok := beforeFns[name]; !ok || prev != body {
				dir := path.Dir(f)
				changed[dir] = append(changed[dir], name)
			}
		}
	}
	for _, names := range changed {
		sort.Strings(names)
	}
	return changed, nil
}

// testBodies maps each top-level Test function in src to its source text.
func testBodies(name, src string) (map[string]string, error) {
	fns := map[string]string{}
	if src == "" {
		return fns, nil
	}
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, name, src, parser.SkipObjectResolution)
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %w", name, err)
	}
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Recv != nil || !testFunc.MatchString(fd.Name.Name) {
			continue
		}
		fns[fd.Name.Name] = src[fset.Position(fd.Pos()).Offset:fset.Position(fd.End()).Offset]
	}
	return fns, nil
}

// backfillMutants returns the mutant IDs a "Red-Check: mutants a, b" trailer
// names, or nil for an ordinary red test commit.
func backfillMutants(body string) []string {
	m := redTrailer.FindStringSubmatch(body)
	if m == nil {
		return nil
	}
	var ids []string
	for _, id := range strings.Split(m[1], ",") {
		if id = strings.TrimSpace(id); id != "" {
			ids = append(ids, id)
		}
	}
	return ids
}

func (c checker) checkTestCommit(commit string, mutants []string) ([]Violation, int, error) {
	changed, err := c.changedTests(commit)
	if err != nil {
		return nil, 0, err
	}
	if len(changed) == 0 {
		return []Violation{{Reason: "adds or changes no Test function, so there is nothing to see fail"}}, 0, nil
	}

	wt, err := os.MkdirTemp("", "redcheck-*")
	if err != nil {
		return nil, 0, err
	}
	defer os.RemoveAll(wt)
	if _, err := c.git("worktree", "add", "--detach", "--force", wt, commit); err != nil {
		return nil, 0, err
	}
	defer func() { _, _ = c.git("worktree", "remove", "--force", wt) }()

	dirs := make([]string, 0, len(changed))
	for d := range changed {
		dirs = append(dirs, d)
	}
	sort.Strings(dirs)
	var vs []Violation
	ran := 0
	for _, dir := range dirs {
		names := changed[dir]
		fmt.Fprintf(c.o.Log, "redcheck: %.9s ./%s %v\n", commit, dir, names)
		results, err := c.runTests(wt, dir, names)
		if err != nil {
			return nil, ran, err
		}
		if results.buildFailed {
			vs = append(vs, Violation{Test: strings.Join(names, ","),
				Reason: "the package did not build — a red must be an assertion, not a compile error (add a stub)"})
			continue
		}
		for _, n := range names {
			ran++
			if v, bad := judge(n, results.tests[n], mutants != nil); bad {
				vs = append(vs, v)
			}
		}
	}
	if mutants != nil {
		mvs, err := c.checkMutants(wt, changed, mutants)
		if err != nil {
			return nil, ran, err
		}
		vs = append(vs, mvs...)
	}
	return vs, ran, nil
}

// judge classifies one test's outcome. An ordinary test: commit wants a fail;
// a backfill wants a pass, because the behavior it guards already exists.
func judge(name string, r *testResult, backfill bool) (Violation, bool) {
	switch {
	case r.panicked:
		return Violation{Test: name, Reason: "panicked — a red must be an assertion, not a panic"}, true
	case r.action == "skip":
		return Violation{Test: name, Reason: "was skipped — a skip is a deleted test"}, true
	case r.action == "":
		return Violation{Test: name, Reason: "did not run"}, true
	case backfill && r.action == "fail":
		return Violation{Test: name, Reason: "backfilled test fails at its own commit — it must guard behavior that exists"}, true
	case !backfill && r.action == "pass":
		return Violation{Test: name, Reason: "passed without the change — it is not testing it"}, true
	}
	return Violation{}, false
}

// checkMutants proves a backfill: every named mutant exists at this commit and
// is killed by the tests this commit added or changed — not by some other test.
func (c checker) checkMutants(wt string, changed map[string][]string, ids []string) ([]Violation, error) {
	f, err := os.Open(filepath.Join(wt, filepath.FromSlash(c.o.MutantsFile)))
	if err != nil {
		return []Violation{{Reason: fmt.Sprintf("names mutants %v but %s is unreadable at this commit: %v", ids, c.o.MutantsFile, err)}}, nil
	}
	defined, err := mutation.Parse(f)
	f.Close()
	if err != nil {
		return []Violation{{Reason: fmt.Sprintf("%s at this commit does not parse: %v", c.o.MutantsFile, err)}}, nil
	}
	byID := map[string]mutation.Mutant{}
	for _, m := range defined {
		byID[m.ID] = m
	}
	var vs []Violation
	for _, id := range ids {
		m, ok := byID[id]
		if !ok {
			vs = append(vs, Violation{Reason: fmt.Sprintf("names mutant %q, which %s at this commit does not define", id, c.o.MutantsFile)})
			continue
		}
		names := changed[strings.TrimPrefix(path.Clean(m.Pkg), "./")]
		if len(names) == 0 {
			vs = append(vs, Violation{Reason: fmt.Sprintf("mutant %q is judged in %s, where this commit changed no test", id, m.Pkg)})
			continue
		}
		m.Run = "^(" + strings.Join(names, "|") + ")$"
		outs, err := mutation.Run(c.ctx, mutation.Options{Root: wt, GoCmd: c.o.GoCmd, Log: c.o.Log}, []mutation.Mutant{m})
		if err != nil {
			return nil, err
		}
		switch o := outs[0]; o.Status {
		case mutation.Killed:
		case mutation.Survived:
			vs = append(vs, Violation{Reason: fmt.Sprintf("mutant %q survived this commit's tests: %s", id, o.Detail)})
		default:
			vs = append(vs, Violation{Reason: fmt.Sprintf("mutant %q is invalid: %s", id, o.Detail)})
		}
	}
	return vs, nil
}

type testResult struct {
	action   string
	panicked bool
}

type runResults struct {
	buildFailed bool
	tests       map[string]*testResult
}

type testEvent struct {
	Action string
	Test   string
	Output string
}

func (c checker) runTests(wt, dir string, names []string) (runResults, error) {
	pattern := "^(" + strings.Join(names, "|") + ")$"
	cmd := exec.CommandContext(c.ctx, c.o.GoCmd, "test", "-count=1", "-json", "-run", pattern, "./"+dir)
	cmd.Dir = wt
	cmd.Env = append(os.Environ(), "GOFLAGS=")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	var exitErr *exec.ExitError
	if err != nil && !errors.As(err, &exitErr) {
		return runResults{}, fmt.Errorf("go test ./%s: %w", dir, err)
	}
	res := runResults{tests: map[string]*testResult{}}
	for _, n := range names {
		res.tests[n] = &testResult{}
	}
	sc := bufio.NewScanner(bytes.NewReader(out))
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var ev testEvent
		if json.Unmarshal(sc.Bytes(), &ev) != nil {
			continue
		}
		switch {
		case ev.Action == "build-fail":
			res.buildFailed = true
		case ev.Test == "" && ev.Action == "fail" && strings.Contains(ev.Output, "[build failed]"):
			res.buildFailed = true
		}
		top, _, _ := strings.Cut(ev.Test, "/")
		r, ok := res.tests[top]
		if !ok {
			if ev.Action == "output" && strings.HasPrefix(ev.Output, "panic: ") {
				for _, r := range res.tests { // unattributed panic: it took the run down
					r.panicked = true
				}
			}
			continue
		}
		switch ev.Action {
		case "output":
			if strings.HasPrefix(ev.Output, "panic: ") {
				r.panicked = true
			}
		case "pass", "fail", "skip":
			if ev.Test == top {
				r.action = ev.Action
			}
		}
	}
	if strings.Contains(stderr.String(), "build failed") || strings.Contains(stderr.String(), "setup failed") {
		res.buildFailed = true
	}
	return res, sc.Err()
}
