// Command mutate re-runs the repository's checked-in mutants: each one severs
// a load-bearing guard, and the named test must go red (docs/TESTING.md §5).
// Mutants run in a throwaway copy of the tree, never the working tree (§5);
// the copy is restored from the original bytes, never from git (§5); every
// mutant is build-checked so a compile error cannot pose as a survivor (§13);
// and a survivor is reported, never dropped (§10).
//
// The mutants file is stanzas of "key: value" lines separated by blank lines;
// '#' starts a comment. Keys: id, file, find, replace, pkg, run. In find and
// replace, \n, \t and \\ are escapes.
package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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
	var (
		ms   []Mutant
		cur  map[string]string
		at   int
		seen = map[string]int{}
	)
	flush := func() error {
		if cur == nil {
			return nil
		}
		m := Mutant{ID: cur["id"], File: cur["file"], Find: unescape(cur["find"]),
			Replace: unescape(cur["replace"]), Pkg: cur["pkg"], Run: cur["run"], Line: at}
		for _, k := range []string{"id", "file", "find", "pkg", "run"} {
			if cur[k] == "" {
				return fmt.Errorf("line %d: mutant is missing %q", at, k)
			}
		}
		if _, ok := cur["replace"]; !ok {
			return fmt.Errorf("line %d: mutant %q is missing \"replace\"", at, m.ID)
		}
		if m.Find == m.Replace {
			return fmt.Errorf("line %d: mutant %q replaces its text with itself", at, m.ID)
		}
		if prev, ok := seen[m.ID]; ok {
			return fmt.Errorf("line %d: mutant id %q already used at line %d", at, m.ID, prev)
		}
		seen[m.ID] = at
		ms = append(ms, m)
		cur = nil
		return nil
	}
	sc := bufio.NewScanner(r)
	for n := 1; sc.Scan(); n++ {
		line := sc.Text()
		trimmed := strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(trimmed, "#"):
			continue
		case trimmed == "":
			if err := flush(); err != nil {
				return nil, err
			}
			continue
		}
		key, val, ok := strings.Cut(line, ":")
		if !ok {
			return nil, fmt.Errorf("line %d: want \"key: value\"", n)
		}
		key = strings.TrimSpace(key)
		switch key {
		case "id", "file", "find", "replace", "pkg", "run":
		default:
			return nil, fmt.Errorf("line %d: unknown key %q", n, key)
		}
		if cur == nil {
			cur, at = map[string]string{}, n
		}
		if _, dup := cur[key]; dup {
			return nil, fmt.Errorf("line %d: %q given twice", n, key)
		}
		cur[key] = strings.TrimPrefix(val, " ")
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return ms, nil
}

func unescape(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			switch s[i+1] {
			case 'n':
				b.WriteByte('\n')
				i++
				continue
			case 't':
				b.WriteByte('\t')
				i++
				continue
			case '\\':
				b.WriteByte('\\')
				i++
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// Options configures a run.
type Options struct {
	Root  string    // repository root to copy
	GoCmd string    // defaults to "go"
	Log   io.Writer // progress; nil discards
}

// Run applies each mutant in a throwaway copy of Root and reports outcomes.
func Run(ctx context.Context, o Options, ms []Mutant) ([]Outcome, error) {
	if o.GoCmd == "" {
		o.GoCmd = "go"
	}
	if o.Log == nil {
		o.Log = io.Discard
	}
	work, err := os.MkdirTemp("", "mutate-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(work)
	if err := copyTree(o.Root, work); err != nil {
		return nil, fmt.Errorf("copying the tree: %w", err)
	}
	outs := make([]Outcome, 0, len(ms))
	for _, m := range ms {
		if err := ctx.Err(); err != nil {
			return outs, err
		}
		fmt.Fprintf(o.Log, "mutate: %s (%s)\n", m.ID, m.File)
		outs = append(outs, runOne(ctx, o, work, m))
	}
	return outs, nil
}

func runOne(ctx context.Context, o Options, work string, m Mutant) Outcome {
	path := filepath.Join(work, filepath.FromSlash(m.File))
	orig, err := os.ReadFile(path)
	if err != nil {
		return Outcome{Mutant: m, Status: Invalid, Detail: err.Error()}
	}
	if n := bytes.Count(orig, []byte(m.Find)); n != 1 {
		return Outcome{Mutant: m, Status: Invalid, Detail: fmt.Sprintf("find text occurs %d times in %s (want exactly 1)", n, m.File)}
	}
	mutated := bytes.Replace(orig, []byte(m.Find), []byte(m.Replace), 1)
	if err := os.WriteFile(path, mutated, 0o644); err != nil {
		return Outcome{Mutant: m, Status: Invalid, Detail: err.Error()}
	}
	// Restore from the original bytes, whatever happens (docs/TESTING.md §5).
	defer func() { _ = os.WriteFile(path, orig, 0o644) }()

	// Build-check: compile the package and its tests without running any.
	if out, err := goCmd(ctx, o, work, "test", "-count=1", "-run", "^$", m.Pkg); err != nil {
		return Outcome{Mutant: m, Status: Invalid, Detail: "does not compile: " + firstLines(out, 3)}
	}
	out, err := goCmd(ctx, o, work, "test", "-count=1", "-run", m.Run, m.Pkg)
	if strings.Contains(out, "no tests to run") {
		return Outcome{Mutant: m, Status: Invalid, Detail: fmt.Sprintf("-run %q matches no test in %s", m.Run, m.Pkg)}
	}
	var exitErr *exec.ExitError
	switch {
	case err == nil:
		return Outcome{Mutant: m, Status: Survived, Detail: fmt.Sprintf("%s stayed green under the mutant", m.Run)}
	case errors.As(err, &exitErr):
		return Outcome{Mutant: m, Status: Killed, Detail: firstFailure(out)}
	default:
		return Outcome{Mutant: m, Status: Invalid, Detail: err.Error()}
	}
}

func goCmd(ctx context.Context, o Options, dir string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, o.GoCmd, args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func firstLines(s string, n int) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) > n {
		lines = lines[:n]
	}
	return strings.Join(lines, " | ")
}

func firstFailure(out string) string {
	for _, l := range strings.Split(out, "\n") {
		if strings.HasPrefix(strings.TrimSpace(l), "--- FAIL") {
			return strings.TrimSpace(l)
		}
	}
	return "test failed"
}

// copyTree copies regular files and directories from src to dst, skipping
// .git: the copy is a scratch tree, not a clone.
func copyTree(src, dst string) error {
	return filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return err
		}
		if d.IsDir() && d.Name() == ".git" {
			return filepath.SkipDir
		}
		target := filepath.Join(dst, rel)
		switch {
		case d.IsDir():
			return os.MkdirAll(target, 0o755)
		case d.Type().IsRegular():
			b, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			return os.WriteFile(target, b, 0o644)
		default:
			return nil // symlinks, sockets: not part of a Go tree we mutate
		}
	})
}
