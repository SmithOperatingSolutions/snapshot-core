package mutation

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fixtureWithMeetingTests is a module whose calc package has two tests that
// meet: each writes a marker under MEET_DIR and waits for the other's, then
// checks Add. Run one after another, the first waits for a marker that
// never comes; run at once, both meet and judge their mutant.
func fixtureWithMeetingTests(t *testing.T) (root, meet string) {
	t.Helper()
	root = fixtureModule(t)
	meet = t.TempDir()
	test := `package calc

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func meet(t *testing.T, mine, theirs string) {
	dir := os.Getenv("MEET_DIR")
	if err := os.WriteFile(filepath.Join(dir, mine), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(20 * time.Second); ; {
		if _, err := os.Stat(filepath.Join(dir, theirs)); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("waited 20s for %s", theirs)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestMeetA(t *testing.T) {
	meet(t, "a", "b")
	if Add(2, 3) != 5 {
		t.Fatal("Add(2, 3) != 5")
	}
}

func TestMeetB(t *testing.T) {
	meet(t, "b", "a")
	if Add(2, 3) != 5 {
		t.Fatal("Add(2, 3) != 5")
	}
}
`
	if err := os.WriteFile(filepath.Join(root, "calc/meet_test.go"), []byte(test), 0o644); err != nil {
		t.Fatal(err)
	}
	return root, meet
}

// Mutants run on Workers goroutines, each in a copy of its own, and the
// outcomes come back in the catalog's order. Two mutants whose tests meet
// through marker files are both judged by their assertions under two
// workers; one after another, the first would wait for a marker no one
// writes until its timeout.
func TestMutantsRunAtOnceOnWorkers(t *testing.T) {
	root, meet := fixtureWithMeetingTests(t)
	env := []string{"MEET_DIR=" + meet}
	ms := []Mutant{
		{ID: "first", File: "calc/calc.go", Find: "return a + b", Replace: "return a - b", Pkg: "./calc", Run: "^TestMeetA$", Env: env},
		{ID: "second", File: "calc/calc.go", Find: "return a + b", Replace: "return a * b", Pkg: "./calc", Run: "^TestMeetB$", Env: env},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	outs, err := Run(ctx, Options{Root: root, Timeout: 5 * time.Second, Workers: 2}, ms)
	if err != nil {
		t.Fatal(err)
	}
	if len(outs) != 2 || outs[0].Mutant.ID != "first" || outs[1].Mutant.ID != "second" {
		t.Fatalf("outcomes %v, want the two in the catalog's order", outs)
	}
	for _, o := range outs {
		if o.Status != Killed || !strings.Contains(o.Detail, "--- FAIL") {
			t.Fatalf("mutant %s under two workers was %v (%s), want killed by its test's assertion: the two tests must have run at once", o.Mutant.ID, o.Status, o.Detail)
		}
	}
}

// A mutant whose test times out while others run is not judged on that:
// it is run again alone, and the verdict is the run alone. Its test starts
// twice, as the count it keeps shows.
func TestAHangUnderLoadIsRunAgainAlone(t *testing.T) {
	root := fixtureModule(t)
	counts := t.TempDir()
	test := `package calc

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCounted(t *testing.T) {
	f, err := os.OpenFile(filepath.Join(os.Getenv("COUNT_DIR"), "starts"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString("x")
	f.Close()
	if Add(2, 3) != 5 {
		t.Fatal("Add(2, 3) != 5")
	}
}
`
	if err := os.WriteFile(filepath.Join(root, "calc/counted_test.go"), []byte(test), 0o644); err != nil {
		t.Fatal(err)
	}
	ms := []Mutant{
		{ID: "hang", File: "calc/calc.go", Find: "return a + b", Replace: "for {\n\t}", Pkg: "./calc", Run: "^TestCounted$", Env: []string{"COUNT_DIR=" + counts}},
		{ID: "other", File: "calc/calc.go", Find: "return a + b", Replace: "return a - b", Pkg: "./calc", Run: "^TestAdd$"},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	outs, err := Run(ctx, Options{Root: root, Timeout: 2 * time.Second, Workers: 2}, ms)
	if err != nil {
		t.Fatal(err)
	}
	o := outcomeOf(t, outs, "hang")
	if o.Status != Killed || !strings.Contains(o.Detail, "timed out") || !strings.Contains(o.Detail, "alone") {
		t.Fatalf("a mutant that hangs was %v (%s), want killed by a timeout in a run alone", o.Status, o.Detail)
	}
	starts, _ := os.ReadFile(filepath.Join(counts, "starts"))
	if len(starts) != 2 {
		t.Fatalf("the hanging mutant's test started %d times, want twice: once among the others, once alone before the verdict", len(starts))
	}
}
