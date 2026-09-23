package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// fixture is a throwaway git repository holding a one-package Go module. Each
// test builds the exact commit history whose verdict it asserts.
type fixture struct {
	t    *testing.T
	dir  string
	base string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{t: t, dir: t.TempDir()}
	f.git("init", "-q", "-b", "main")
	f.git("config", "user.email", "fixture@example.com")
	f.git("config", "user.name", "fixture")
	f.git("config", "commit.gpgsign", "false")
	f.write("go.mod", "module example.com/fx\n\ngo 1.27\n")
	f.write("calc/calc.go", "package calc\n\n// Add is a stub.\nfunc Add(a, b int) int { return 0 }\n")
	f.commit("chore: init")
	f.base = strings.TrimSpace(f.git("rev-parse", "HEAD"))
	f.git("checkout", "-q", "-b", "work")
	return f
}

func (f *fixture) git(args ...string) string {
	f.t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = f.dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		f.t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return string(out)
}

func (f *fixture) write(rel, body string) {
	f.t.Helper()
	p := filepath.Join(f.dir, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) commit(subject string) {
	f.t.Helper()
	f.git("add", "-A")
	f.git("commit", "-q", "-m", subject)
}

func (f *fixture) check() Report {
	f.t.Helper()
	rep, err := Check(context.Background(), Options{Dir: f.dir, Base: f.base})
	if err != nil {
		f.t.Fatalf("Check: %v", err)
	}
	return rep
}

const addTest = `package calc

import "testing"

func TestAdd(t *testing.T) {
	if got := Add(1, 2); got != 3 {
		t.Fatalf("Add(1, 2) = %d, want 3", got)
	}
}
`

const addImpl = "package calc\n\n// Add adds.\nfunc Add(a, b int) int { return a + b }\n"

func reasons(rep Report) string {
	var b strings.Builder
	for _, v := range rep.Violations {
		b.WriteString(v.Subject + " / " + v.Test + ": " + v.Reason + "\n")
	}
	return b.String()
}

// The positive control every refusal below is measured against: a test commit
// whose test fails on an assertion, then the feature that makes it pass.
func TestRedThenGreenPasses(t *testing.T) {
	f := newFixture(t)
	f.write("calc/calc_test.go", addTest)
	f.commit("test(calc): Add adds")
	f.write("calc/calc.go", addImpl)
	f.commit("feat(calc): Add adds")

	rep := f.check()
	if len(rep.Violations) != 0 {
		t.Fatalf("a correct red-then-green history was blocked:\n%s", reasons(rep))
	}
	if rep.Checked != 1 || rep.TestsRun != 1 {
		t.Fatalf("red-check looked at %d test commits and ran %d tests; want 1 and 1 — "+
			"a check that runs nothing passes everything", rep.Checked, rep.TestsRun)
	}
}

// The M0 exit criterion: a deliberately passing "red" test blocks the PR.
func TestPassingRedTestIsBlocked(t *testing.T) {
	f := newFixture(t)
	f.write("calc/calc.go", addImpl)
	f.commit("chore: sneak the implementation in first")
	f.write("calc/calc_test.go", addTest)
	f.commit("test(calc): Add adds")

	rep := f.check()
	if len(rep.Violations) != 1 || rep.Violations[0].Test != "TestAdd" ||
		!strings.Contains(rep.Violations[0].Reason, "passed") {
		t.Fatalf("a test that passes against the old code was not blocked as passing:\n%s", reasons(rep))
	}
}

func TestBuildFailureIsTheWrongRed(t *testing.T) {
	f := newFixture(t)
	f.write("calc/calc_test.go", strings.Replace(addTest, "Add(1, 2)", "Sub(1, 2)", 1))
	f.commit("test(calc): Sub subtracts")

	rep := f.check()
	if len(rep.Violations) != 1 || !strings.Contains(rep.Violations[0].Reason, "build") {
		t.Fatalf("a test commit that does not compile was accepted as red:\n%s", reasons(rep))
	}
}

func TestPanicIsTheWrongRed(t *testing.T) {
	f := newFixture(t)
	f.write("calc/calc_test.go", `package calc

import "testing"

func TestAdd(t *testing.T) {
	var m map[string]int
	m["x"] = Add(1, 2)
}
`)
	f.commit("test(calc): Add adds")

	rep := f.check()
	if len(rep.Violations) != 1 || !strings.Contains(rep.Violations[0].Reason, "panic") {
		t.Fatalf("a test that panics was accepted as an assertion failure:\n%s", reasons(rep))
	}
}

func TestSkipIsADeletedTest(t *testing.T) {
	f := newFixture(t)
	f.write("calc/calc_test.go", `package calc

import "testing"

func TestAdd(t *testing.T) { t.Skip("later") }
`)
	f.commit("test(calc): Add adds")

	rep := f.check()
	if len(rep.Violations) != 1 || !strings.Contains(rep.Violations[0].Reason, "skip") {
		t.Fatalf("a skipped test was accepted as red:\n%s", reasons(rep))
	}
}

// Only the tests the commit ADDED are held to failing: an existing passing
// test in the same file must not turn the commit's verdict.
func TestOnlyAddedTestsMustFail(t *testing.T) {
	f := newFixture(t)
	f.write("calc/calc_test.go", "package calc\n\nimport \"testing\"\n\nfunc TestTrivial(t *testing.T) {}\n")
	f.commit("chore: a passing test already exists")
	f.write("calc/calc_test.go", "package calc\n\nimport \"testing\"\n\nfunc TestTrivial(t *testing.T) {}\n"+
		strings.TrimPrefix(addTest, "package calc\n\nimport \"testing\"\n"))
	f.commit("test(calc): Add adds")
	f.write("calc/calc.go", addImpl)
	f.commit("feat(calc): Add adds")

	rep := f.check()
	if len(rep.Violations) != 0 || rep.TestsRun != 1 {
		t.Fatalf("want only TestAdd judged (ran %d):\n%s", rep.TestsRun, reasons(rep))
	}
}

func TestFeatWithoutRedIsBlocked(t *testing.T) {
	f := newFixture(t)
	f.write("calc/calc.go", addImpl)
	f.commit("feat(calc): Add adds")

	rep := f.check()
	if len(rep.Violations) != 1 || !strings.Contains(rep.Violations[0].Reason, "no test: commit") {
		t.Fatalf("a feat: commit with no test: commit before it was accepted:\n%s", reasons(rep))
	}
}

// Non-behavioral commits need no red.
func TestDocsAndChoresNeedNoRed(t *testing.T) {
	f := newFixture(t)
	f.write("README.md", "hi\n")
	f.commit("docs: readme")

	rep := f.check()
	if len(rep.Violations) != 0 || rep.Commits != 1 {
		t.Fatalf("a docs-only history was blocked (commits=%d):\n%s", rep.Commits, reasons(rep))
	}
}

// A test: commit that strengthens an EXISTING test is judged on that test.
func TestChangedTestIsJudged(t *testing.T) {
	f := newFixture(t)
	f.write("calc/calc_test.go", "package calc\n\nimport \"testing\"\n\nfunc TestAdd(t *testing.T) { _ = Add(1, 2) }\n")
	f.commit("chore: a test that asserts nothing")
	f.write("calc/calc_test.go", addTest)
	f.commit("test(calc): TestAdd asserts the sum")
	f.write("calc/calc.go", addImpl)
	f.commit("feat(calc): Add adds")

	rep := f.check()
	if len(rep.Violations) != 0 || rep.TestsRun != 1 {
		t.Fatalf("a strengthened existing test was not judged as the commit's red (ran %d):\n%s", rep.TestsRun, reasons(rep))
	}
}
