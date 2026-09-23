package mutation

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

const mutantsFile = `# A comment, then two mutants separated by a blank line.
id: add-sign
file: calc/calc.go
find: return a + b
replace: return a - b
pkg: ./calc
run: ^TestAdd$
env: CALC_MODE=strict

id: add-weak
file: calc/calc.go
find: return a + b
replace: return b + a
pkg: ./calc
run: ^TestAdd$
`

func TestParseReadsEveryField(t *testing.T) {
	ms, err := Parse(strings.NewReader(mutantsFile))
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 2 {
		t.Fatalf("parsed %d mutants from a file holding 2", len(ms))
	}
	want := Mutant{ID: "add-sign", File: "calc/calc.go", Find: "return a + b", Replace: "return a - b",
		Pkg: "./calc", Run: "^TestAdd$", Env: []string{"CALC_MODE=strict"}, Line: 2}
	if !reflect.DeepEqual(ms[0], want) {
		t.Fatalf("first mutant = %+v\nwant %+v", ms[0], want)
	}
}

func TestParseRefusesIncompleteAndDuplicateMutants(t *testing.T) {
	for name, src := range map[string]string{
		"missing run": "id: a\nfile: f.go\nfind: x\nreplace: y\npkg: ./p\n",
		"duplicate":   "id: a\nfile: f.go\nfind: x\nreplace: y\npkg: ./p\nrun: T\n\nid: a\nfile: f.go\nfind: x\nreplace: z\npkg: ./p\nrun: T\n",
		"unknown key": "id: a\nfile: f.go\nfind: x\nreplace: y\npkg: ./p\nrun: T\nwhat: ever\n",
		"no-op":       "id: a\nfile: f.go\nfind: x\nreplace: x\npkg: ./p\nrun: T\n",
	} {
		if _, err := Parse(strings.NewReader(src)); err == nil {
			t.Errorf("%s: a malformed mutants file was accepted", name)
		}
	}
}

func fixtureModule(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	write := func(rel, body string) {
		p := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("go.mod", "module example.com/fx\n\ngo 1.27\n")
	write("calc/calc.go", "package calc\n\n// Add adds.\nfunc Add(a, b int) int {\n\treturn a + b\n}\n")
	write("calc/calc_test.go", "package calc\n\nimport \"testing\"\n\nfunc TestAdd(t *testing.T) {\n"+
		"\tif Add(2, 3) != 5 {\n\t\tt.Fatal(\"Add(2, 3) != 5\")\n\t}\n}\n")
	return dir
}

func outcomeOf(t *testing.T, outs []Outcome, id string) Outcome {
	t.Helper()
	for _, o := range outs {
		if o.Mutant.ID == id {
			return o
		}
	}
	t.Fatalf("no outcome for mutant %q (got %d outcomes)", id, len(outs))
	return Outcome{}
}

func TestRunClassifiesKilledSurvivedAndInvalid(t *testing.T) {
	root := fixtureModule(t)
	ms := []Mutant{
		{ID: "killed", File: "calc/calc.go", Find: "return a + b", Replace: "return a - b", Pkg: "./calc", Run: "^TestAdd$"},
		{ID: "survives", File: "calc/calc.go", Find: "return a + b", Replace: "return b + a", Pkg: "./calc", Run: "^TestAdd$"},
		{ID: "no-compile", File: "calc/calc.go", Find: "return a + b", Replace: "return a +", Pkg: "./calc", Run: "^TestAdd$"},
		{ID: "not-found", File: "calc/calc.go", Find: "return a * b", Replace: "return 0", Pkg: "./calc", Run: "^TestAdd$"},
	}
	outs, err := Run(context.Background(), Options{Root: root}, ms)
	if err != nil {
		t.Fatal(err)
	}
	if o := outcomeOf(t, outs, "killed"); o.Status != Killed {
		t.Errorf("a mutant the test catches was reported %v (%s)", o.Status, o.Detail)
	}
	if o := outcomeOf(t, outs, "survives"); o.Status != Survived {
		t.Errorf("a mutant the test cannot see was reported %v — survivors must never be hidden", o.Status)
	}
	if o := outcomeOf(t, outs, "no-compile"); o.Status != Invalid {
		t.Errorf("a mutant that does not compile was reported %v — a compile error must not pose as a kill or a survivor", o.Status)
	}
	if o := outcomeOf(t, outs, "not-found"); o.Status != Invalid {
		t.Errorf("a mutant whose text is absent was reported %v", o.Status)
	}
}

// The working tree is never touched: mutants run in a copy (docs/TESTING.md §5).
func TestRunNeverTouchesTheWorkingTree(t *testing.T) {
	root := fixtureModule(t)
	before, err := os.ReadFile(filepath.Join(root, "calc/calc.go"))
	if err != nil {
		t.Fatal(err)
	}
	ms := []Mutant{{ID: "killed", File: "calc/calc.go", Find: "return a + b", Replace: "return a - b", Pkg: "./calc", Run: "^TestAdd$"}}
	outs, err := Run(context.Background(), Options{Root: root}, ms)
	if err != nil {
		t.Fatal(err)
	}
	if len(outs) != 1 {
		t.Fatalf("got %d outcomes for 1 mutant", len(outs))
	}
	after, err := os.ReadFile(filepath.Join(root, "calc/calc.go"))
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatalf("mutating left the working tree changed:\n%s", after)
	}
}

// Some guards are only seen at scale (the crash harness needs 1,000 kills to
// catch a torn write); a mutant can set the test's environment.
func TestParseAndRunHonorEnv(t *testing.T) {
	ms, err := Parse(strings.NewReader("id: e\nfile: calc/calc.go\nfind: return a + b\nreplace: return a - b\n" +
		"pkg: ./calc\nrun: ^TestEnv$\nenv: CALC_STRICT=1\nenv: OTHER=2\n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 1 || strings.Join(ms[0].Env, ",") != "CALC_STRICT=1,OTHER=2" {
		t.Fatalf("parsed env %v, want [CALC_STRICT=1 OTHER=2]", ms)
	}
	root := fixtureModule(t)
	// A test that only checks when CALC_STRICT is set: without the env the
	// mutant survives, with it the mutant is killed.
	test := "package calc\n\nimport (\n\t\"os\"\n\t\"testing\"\n)\n\nfunc TestEnv(t *testing.T) {\n" +
		"\tif os.Getenv(\"CALC_STRICT\") == \"1\" && Add(2, 3) != 5 {\n\t\tt.Fatal(\"wrong\")\n\t}\n}\n"
	if err := os.WriteFile(filepath.Join(root, "calc/env_test.go"), []byte(test), 0o644); err != nil {
		t.Fatal(err)
	}
	outs, err := Run(context.Background(), Options{Root: root}, ms)
	if err != nil {
		t.Fatal(err)
	}
	if outs[0].Status != Killed {
		t.Fatalf("with env CALC_STRICT=1 the mutant was %v (%s): the env did not reach the test", outs[0].Status, outs[0].Detail)
	}
}

// A mutant that makes its test hang is killed at the run's timeout, not
// after go test's default ten minutes.
func TestAHangingMutantIsKilledAtTheTimeout(t *testing.T) {
	root := fixtureModule(t)
	ms := []Mutant{{ID: "hang", File: "calc/calc.go", Find: "return a + b", Replace: "for {\n\t}", Pkg: "./calc", Run: "^TestAdd$"}}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second) // a net, should the timeout not apply
	defer cancel()
	start := time.Now()
	outs, err := Run(ctx, Options{Root: root, Timeout: 2 * time.Second}, ms)
	if err != nil {
		t.Fatal(err)
	}
	if o := outs[0]; o.Status != Killed || !strings.Contains(o.Detail, "timed out") {
		t.Fatalf("a mutant that hangs its test was %v (%s), want killed at the timeout", o.Status, o.Detail)
	}
	if el := time.Since(start); el > 40*time.Second {
		t.Fatalf("killing a hanging mutant took %v with a 2s timeout", el)
	}
}
