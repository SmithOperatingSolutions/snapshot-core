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

func (f *fixture) commitBody(subject, body string) {
	f.t.Helper()
	f.git("add", "-A")
	f.git("commit", "-q", "-m", subject, "-m", body)
}

const addMutants = `id: add-sign
file: calc/calc.go
find: return a + b
replace: return a - b
pkg: ./calc
run: ^TestAdd$

id: add-commutes
file: calc/calc.go
find: return a + b
replace: return b + a
pkg: ./calc
run: ^TestAdd$
`

// A test for behavior that already exists cannot be red against its parent;
// its red is a mutant the test kills (docs/TESTING.md §5, §11).
func TestBackfillProvenByMutantPasses(t *testing.T) {
	f := newFixture(t)
	f.write("calc/calc.go", addImpl)
	f.commit("feat(calc): Add adds") // blocked on its own (no red), which is not what this test judges
	f.write("calc/calc_test.go", addTest)
	f.write("tools/mutate/mutants.txt", addMutants)
	f.commitBody("test(calc): pin Add's sign", "Red-Check: mutants add-sign")

	rep := f.check()
	var others []Violation
	for _, v := range rep.Violations {
		if !strings.HasPrefix(v.Subject, "feat(calc)") {
			others = append(others, v)
		}
	}
	if len(others) != 0 {
		t.Fatalf("a backfill whose mutant is killed was blocked:\n%s", reasons(Report{Violations: others}))
	}
	if rep.TestsRun != 1 {
		t.Fatalf("ran %d tests for the backfill commit, want 1", rep.TestsRun)
	}
}

func backfillViolations(t *testing.T, trailer string) []Violation {
	t.Helper()
	f := newFixture(t)
	f.write("calc/calc.go", addImpl)
	f.commit("chore: implementation predates its test")
	f.write("calc/calc_test.go", addTest)
	f.write("tools/mutate/mutants.txt", addMutants)
	f.commitBody("test(calc): pin Add", trailer)
	return f.check().Violations
}

func TestBackfillWhoseMutantSurvivesIsBlocked(t *testing.T) {
	vs := backfillViolations(t, "Red-Check: mutants add-sign, add-commutes")
	if len(vs) != 1 || !strings.Contains(vs[0].Reason, "add-commutes") || !strings.Contains(vs[0].Reason, "survived") {
		t.Fatalf("a backfill naming a mutant its test cannot kill was accepted:\n%s", reasons(Report{Violations: vs}))
	}
}

func TestBackfillNamingAnUnknownMutantIsBlocked(t *testing.T) {
	vs := backfillViolations(t, "Red-Check: mutants add-sing")
	if len(vs) != 1 || !strings.Contains(vs[0].Reason, "add-sing") {
		t.Fatalf("a backfill naming a mutant that does not exist was accepted:\n%s", reasons(Report{Violations: vs}))
	}
}

// A backfill guards behavior that exists, so its test must pass at its own commit.
func TestBackfillThatFailsIsBlocked(t *testing.T) {
	f := newFixture(t) // Add is still the stub
	f.write("calc/calc_test.go", addTest)
	f.write("tools/mutate/mutants.txt", "id: add-sign\nfile: calc/calc.go\nfind: return 0\nreplace: return 1\npkg: ./calc\nrun: ^TestAdd$\n")
	f.commitBody("test(calc): pin Add", "Red-Check: mutants add-sign")

	vs := f.check().Violations
	if len(vs) == 0 || !strings.Contains(vs[0].Reason, "fails at its own commit") {
		t.Fatalf("a backfill test that fails at its own commit was accepted:\n%s", reasons(Report{Violations: vs}))
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

// A test behind a build tag (the scale guards use -tags slow) is judged with
// that tag; without it, it would never run and could never be red.
func TestBuildTaggedTestIsRunWithItsTag(t *testing.T) {
	f := newFixture(t)
	f.write("calc/calc_slow_test.go", "//go:build slow\n\n"+strings.Replace(addTest, "TestAdd", "TestSlowAdd", 1))
	f.commit("test(calc): Add adds, at scale")
	f.write("calc/calc.go", addImpl)
	f.commit("feat(calc): Add adds")

	rep := f.check()
	if len(rep.Violations) != 0 || rep.TestsRun != 1 {
		t.Fatalf("a //go:build slow test was not judged under its tag (ran %d):\n%s", rep.TestsRun, reasons(rep))
	}
}

// A backfill whose test carries a build tag has its mutants judged under
// that tag too: without it the test is not in the build, so no mutant could
// ever be seen killed by it.
func TestTaggedBackfillIsProvenUnderItsTag(t *testing.T) {
	f := newFixture(t)
	f.write("calc/calc.go", addImpl)
	f.commit("chore: implementation predates its test")
	f.write("calc/calc_slow_test.go", "//go:build slow\n\n"+strings.Replace(addTest, "TestAdd", "TestSlowAdd", 1))
	f.write("tools/mutate/mutants.txt", addMutants)
	f.commitBody("test(calc): pin Add's sign, at scale", "Red-Check: mutants add-sign")

	rep := f.check()
	if len(rep.Violations) != 0 || rep.TestsRun != 1 {
		t.Fatalf("a backfill whose //go:build slow test kills its mutant was blocked (ran %d):\n%s", rep.TestsRun, reasons(rep))
	}
}

// A fuzz target is a test too: under -run it runs its seed corpus. A
// backfill that adds one is judged on it, and its mutants are proven by
// the seeds.
func TestAFuzzTargetIsJudgedLikeATest(t *testing.T) {
	f := newFixture(t)
	f.write("calc/calc.go", addImpl)
	f.commit("chore: implementation predates its test")
	f.write("calc/calc_fuzz_test.go", "package calc\n\nimport \"testing\"\n\nfunc FuzzAdd(f *testing.F) {\n\tf.Add(2, 3)\n"+
		"\tf.Fuzz(func(t *testing.T, a, b int) {\n\t\tif got := Add(a, b); got != a+b {\n\t\t\tt.Fatalf(\"Add(%d, %d) = %d\", a, b, got)\n\t\t}\n\t})\n}\n")
	f.write("tools/mutate/mutants.txt", addMutants)
	f.commitBody("test(calc): fuzz Add", "Red-Check: mutants add-sign")

	rep := f.check()
	if len(rep.Violations) != 0 || rep.TestsRun != 1 {
		t.Fatalf("a backfill adding a fuzz target whose seeds kill its mutant was blocked (ran %d):\n%s", rep.TestsRun, reasons(rep))
	}
}

const fuzzAdd = "package calc\n\nimport \"testing\"\n\nfunc FuzzAdd(f *testing.F) {\n\tf.Add(2, 3)\n" +
	"\tf.Fuzz(func(t *testing.T, a, b int) {\n\t\tif got := Add(a, b); got != a+b && got != 0 {\n\t\t\tt.Fatalf(\"Add(%d, %d) = %d\", a, b, got)\n\t\t}\n\t})\n}\n"

// In a red commit, a fuzz target that passes against the stub stands beside
// the failing test (its property may hold of a stub that answers nothing);
// a red commit of passing fuzz targets alone showed nothing failing.
func TestAFuzzTargetNeedNotFailBesideARedTest(t *testing.T) {
	f := newFixture(t)
	f.write("calc/calc_test.go", addTest)
	f.write("calc/calc_fuzz_test.go", fuzzAdd)
	f.commit("test(calc): Add adds, and is fuzzed")
	f.write("calc/calc.go", addImpl)
	f.commit("feat(calc): Add adds")
	if rep := f.check(); len(rep.Violations) != 0 || rep.TestsRun != 2 {
		t.Fatalf("a red test beside a fuzz target its stub passes was blocked (ran %d):\n%s", rep.TestsRun, reasons(rep))
	}

	g := newFixture(t)
	g.write("calc/calc_fuzz_test.go", fuzzAdd)
	g.commit("test(calc): Add is fuzzed")
	g.write("calc/calc.go", addImpl)
	g.commit("feat(calc): Add adds")
	vs := g.check().Violations
	if len(vs) != 1 || !strings.Contains(vs[0].Reason, "nothing was seen to fail") {
		t.Fatalf("a red commit of fuzz targets that all pass was accepted:\n%s", reasons(Report{Violations: vs}))
	}
}

// Pairs may interleave across scopes: test(a), test(b), feat(b), feat(a).
func TestInterleavedPairsAreMatchedByScope(t *testing.T) {
	f := newFixture(t)
	f.write("calc/calc_test.go", addTest)
	f.commit("test(calc): Add adds")
	f.write("tool/tool.go", "package tool\n\n// Two is a stub.\nfunc Two() int { return 0 }\n")
	f.write("tool/tool_test.go", "package tool\n\nimport \"testing\"\n\nfunc TestTwo(t *testing.T) {\n\tif Two() != 2 {\n\t\tt.Fatal(\"Two() != 2\")\n\t}\n}\n")
	f.commit("test(tool): Two is two")
	f.write("tool/tool.go", "package tool\n\n// Two is two.\nfunc Two() int { return 2 }\n")
	f.commit("feat(tool): Two is two")
	f.write("calc/calc.go", addImpl)
	f.commit("feat(calc): Add adds")

	rep := f.check()
	if len(rep.Violations) != 0 {
		t.Fatalf("interleaved test/feat pairs of two scopes were blocked:\n%s", reasons(rep))
	}
	// And a feat whose scope never had a red is still blocked.
	f.write("calc/more.go", "package calc\n\n// Three is three.\nfunc Three() int { return 3 }\n")
	f.commit("feat(calc): Three")
	rep = f.check()
	if len(rep.Violations) != 1 || !strings.Contains(rep.Violations[0].Subject, "Three") {
		t.Fatalf("a second feat(calc) with no new test(calc) was not blocked:\n%s", reasons(rep))
	}
}

// TestMain is the test binary's entry point, not a test.
func TestTestMainIsNotJudged(t *testing.T) {
	f := newFixture(t)
	f.write("calc/main_test.go", "package calc\n\nimport (\n\t\"os\"\n\t\"testing\"\n)\n\n"+
		"func TestMain(m *testing.M) { os.Exit(m.Run()) }\n")
	f.write("calc/calc_test.go", addTest)
	f.commit("test(calc): Add adds")
	f.write("calc/calc.go", addImpl)
	f.commit("feat(calc): Add adds")

	rep := f.check()
	if len(rep.Violations) != 0 || rep.TestsRun != 1 {
		t.Fatalf("TestMain was judged as a test (ran %d):\n%s", rep.TestsRun, reasons(rep))
	}
}

// A port's contract suite lives in a non-test file (<pkg>/contract), so a
// change to it touches no Test function, yet it changes every test that runs
// it (Engine Spec: "contract tests updated if a port changed").
const contractV0 = `// Package contract is the suite every Add must pass.
package contract

import "testing"

// Run runs the suite.
func Run(t *testing.T, add func(a, b int) int) { _ = add(1, 2) }
`

const contractV1 = `// Package contract is the suite every Add must pass.
package contract

import "testing"

// Run runs the suite.
func Run(t *testing.T, add func(a, b int) int) {
	if got := add(1, 2); got != 3 {
		t.Fatalf("add(1, 2) = %d, want 3", got)
	}
}
`

// contractFixture commits a contract that asserts nothing yet and a test
// file that runs it (importing it as alias, when one is given) beside a test
// that does not.
func contractFixture(t *testing.T, alias string) *fixture {
	t.Helper()
	f := newFixture(t)
	name, spec := "contract", `"example.com/fx/calc/contract"`
	if alias != "" {
		name, spec = alias, alias+" "+spec
	}
	f.write("calc/contract/contract.go", contractV0)
	f.write("calc/calc_test.go", "package calc\n\nimport (\n\t\"testing\"\n\n\t"+spec+"\n)\n\n"+
		"func TestContract(t *testing.T) { "+name+".Run(t, Add) }\n\nfunc TestOther(t *testing.T) {}\n")
	f.commit("chore: the implementation runs a contract that asserts nothing yet")
	return f
}

func TestContractChangeIsJudgedOnTheTestsThatRunIt(t *testing.T) {
	f := contractFixture(t, "")
	f.write("calc/contract/contract.go", contractV1)
	f.commit("test(calc): the contract requires Add to add")
	f.write("calc/calc.go", addImpl)
	f.commit("feat(calc): Add adds")

	rep := f.check()
	if len(rep.Violations) != 0 || rep.Checked != 1 || rep.TestsRun != 1 {
		t.Fatalf("a red contract change was not judged on the one test that runs the contract "+
			"(checked %d, ran %d):\n%s", rep.Checked, rep.TestsRun, reasons(rep))
	}
}

func TestPassingContractChangeIsBlocked(t *testing.T) {
	f := contractFixture(t, "ctr")
	f.write("calc/calc.go", addImpl)
	f.commit("chore: the implementation lands first")
	f.write("calc/contract/contract.go", contractV1)
	f.commit("test(calc): the contract requires Add to add")

	rep := f.check()
	if len(rep.Violations) != 1 || rep.Violations[0].Test != "TestContract" ||
		!strings.Contains(rep.Violations[0].Reason, "passed") {
		t.Fatalf("a contract change the implementation already passes was not blocked as passing "+
			"on the (alias-importing) test that runs it:\n%s", reasons(rep))
	}
}

// With no base named, redcheck checks from main; in a clone with no main
// (a new repository whose first branch is the work branch), from the root
// commit, so every commit after it is checked instead of none.
func TestBaseDefaultsToMainThenTheRootCommit(t *testing.T) {
	f := newFixture(t)
	f.write("calc/calc_test.go", addTest)
	f.commit("test(calc): Add adds")

	rep, err := Check(context.Background(), Options{Dir: f.dir})
	if err != nil || rep.Base != "main" || rep.Commits != 1 || rep.Checked != 1 {
		t.Fatalf("with main present and no base named: base %q, %d commits, %d checked (%v); want main, 1, 1",
			rep.Base, rep.Commits, rep.Checked, err)
	}
	f.git("branch", "-D", "main")
	rep, err = Check(context.Background(), Options{Dir: f.dir})
	if err != nil || rep.Base != f.base || rep.Commits != 1 || rep.Checked != 1 {
		t.Fatalf("with no main and no base named: base %q, %d commits, %d checked (%v); want the root commit %s, 1, 1",
			rep.Base, rep.Commits, rep.Checked, err, f.base)
	}
}

const remoteContractTest = `package calc

import (
	"testing"

	"example.com/fx/calc/contract"
)

func TestContractRemote(t *testing.T) {
	t.Skip("set an endpoint to run the remote tier")
	contract.Run(t, Add)
}
`

// A test brought in only because it calls a changed contract may need an
// environment the check does not have (the S3 tier skips without an
// endpoint): its skip is not a deleted test, and it is not judged. A contract
// change must still be seen to fail somewhere, so every caller skipping is.
func TestAContractCallerThatSkipsIsNotJudged(t *testing.T) {
	f := contractFixture(t, "")
	f.write("calc/remote_test.go", remoteContractTest)
	f.commit("chore: a remote contract run that needs an endpoint")
	f.write("calc/contract/contract.go", contractV1)
	f.commit("test(calc): the contract requires Add to add")
	f.write("calc/calc.go", addImpl)
	f.commit("feat(calc): Add adds")

	rep := f.check()
	if len(rep.Violations) != 0 || rep.TestsRun != 1 {
		t.Fatalf("a contract caller that skipped for want of an environment blocked the commit "+
			"or was counted as run (ran %d):\n%s", rep.TestsRun, reasons(rep))
	}

	g := newFixture(t)
	g.write("calc/contract/contract.go", contractV0)
	g.write("calc/remote_test.go", remoteContractTest)
	g.commit("chore: only a remote contract run")
	g.write("calc/contract/contract.go", contractV1)
	g.commit("test(calc): the contract requires Add to add")
	rep = g.check()
	if len(rep.Violations) != 1 || !strings.Contains(rep.Violations[0].Reason, "skipped") || rep.TestsRun != 0 {
		t.Fatalf("a contract change whose every caller skipped was accepted (ran %d):\n%s", rep.TestsRun, reasons(rep))
	}
}

// A test the commit wrote is judged strictly even when it also calls the
// changed contract: a new test that only skips is still a deleted test.
func TestAWrittenTestThatCallsTheContractIsJudgedStrictly(t *testing.T) {
	f := contractFixture(t, "")
	f.write("calc/contract/contract.go", contractV1)
	f.write("calc/remote_test.go", remoteContractTest)
	f.commit("test(calc): the contract requires Add to add, remotely too")

	rep := f.check()
	if len(rep.Violations) != 1 || rep.Violations[0].Test != "TestContractRemote" ||
		!strings.Contains(rep.Violations[0].Reason, "skip") || rep.TestsRun != 2 {
		t.Fatalf("a skipping test the commit wrote escaped judgment by calling the contract (ran %d):\n%s",
			rep.TestsRun, reasons(rep))
	}
}
