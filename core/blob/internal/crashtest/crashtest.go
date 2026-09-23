// Package crashtest is the kill -9 harness for disk backends (Storage Core
// Spec: "kill -9 during SwapRoot on local and multivol, 1,000 times: reopened
// root is old or new, never torn"). The test binary re-executes itself as the
// child; the child announces each operation on stdout before and after it,
// the parent kills it with SIGKILL at a random moment, reopens the store and
// checks that what it finds is exactly the old state or the new one.
//
// Iterations come from SNAPSHOT_CRASH_ITERATIONS (default 20; `mise run crash`
// and the nightly job use 1,000).
package crashtest

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
)

const (
	envMode  = "SNAPSHOT_CRASH_CHILD"
	envDir   = "SNAPSHOT_CRASH_DIR"
	envStart = "SNAPSHOT_CRASH_START"
)

// Opener opens an existing store in dir (in the child and, afterwards, in the
// parent to inspect it).
type Opener func(dir string) (blob.BlobStore, error)

// Iterations is how many kills to perform.
func Iterations() int {
	if n, err := strconv.Atoi(os.Getenv("SNAPSHOT_CRASH_ITERATIONS")); err == nil && n > 0 {
		return n
	}
	return 20
}

// Value is what the child's n-th root swap writes.
func Value(n int) []byte { return []byte(fmt.Sprintf("crash-root-%08d", n)) }

// Body is the content of the child's n-th Put.
func Body(n int) []byte {
	out := make([]byte, 0, 96<<10)
	for i := 0; len(out) < 96<<10; i++ {
		h := sha256.Sum256([]byte(fmt.Sprintf("crash-body-%d-%d", n, i)))
		out = append(out, h[:]...)
	}
	return out
}

// ObjectName is the name of the child's n-th Put.
func ObjectName(n int) string { return fmt.Sprintf("crash/obj-%08d", n) }

// Main is the package's TestMain: in a harness child it runs the child loop
// (and never returns); otherwise it runs the tests.
func Main(m *testing.M, open Opener) {
	if os.Getenv(envMode) == "" {
		os.Exit(m.Run())
	}
	child(open)
}

func child(open Opener) {
	mode := os.Getenv(envMode)
	start, _ := strconv.Atoi(os.Getenv(envStart))
	s, err := open(os.Getenv(envDir))
	if err != nil {
		fmt.Printf("error open %v\n", err)
		os.Exit(3)
	}
	ctx := context.Background()
	switch mode {
	case "swap":
		r, err := s.Root(ctx)
		if err != nil {
			fmt.Printf("error root %v\n", err)
			os.Exit(3)
		}
		v := r.Version
		for n := start; ; n++ {
			fmt.Printf("try %d\n", n)
			if v, err = s.SwapRoot(ctx, v, Value(n)); err != nil {
				fmt.Printf("error swap %v\n", err)
				os.Exit(3)
			}
			fmt.Printf("done %d\n", n)
		}
	case "put":
		for n := start; ; n++ {
			fmt.Printf("try %d\n", n)
			b := Body(n)
			if err := s.Put(ctx, ObjectName(n), bytes.NewReader(b), int64(len(b))); err != nil {
				fmt.Printf("error put %v\n", err)
				os.Exit(3)
			}
			fmt.Printf("done %d\n", n)
		}
	}
}

// progress is what a killed child announced.
type progress struct{ lastTry, lastDone int }

func runChild(t *testing.T, mode, dir string, start int) progress {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^$", "-test.count=1")
	cmd.Env = append(os.Environ(), envMode+"="+mode, envDir+"="+dir, envStart+"="+strconv.Itoa(start))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := progress{lastTry: start - 1, lastDone: start - 1}
	sc := bufio.NewScanner(stdout)
	lines := make(chan string)
	go func() {
		for sc.Scan() {
			lines <- sc.Text()
		}
		close(lines)
	}()
	record := func(l string) {
		f := strings.Fields(l)
		if len(f) >= 2 {
			n, err := strconv.Atoi(f[1])
			switch {
			case f[0] == "try" && err == nil:
				p.lastTry = n
			case f[0] == "done" && err == nil:
				p.lastDone = n
			case f[0] == "error":
				t.Errorf("child failed: %s", l)
			}
		}
	}
	// Hold until the child is inside its loop, then kill at a random moment.
	select {
	case l, ok := <-lines:
		if !ok {
			_ = cmd.Wait()
			t.Fatal("the crash child exited before starting its loop")
		}
		record(l)
	case <-time.After(30 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("the crash child never started")
	}
	// The window spans a few operations: a fsync'd Put takes milliseconds, a
	// swap less, and the kill must land both between and inside them.
	window := 20000
	if mode == "put" {
		window = 80000
	}
	time.Sleep(time.Duration(rand.IntN(window)) * time.Microsecond)
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	for l := range lines {
		record(l)
	}
	_ = cmd.Wait()
	return p
}

// SwapRoot kills a child mid root-swap repeatedly. After every kill the root
// must read back intact as the last completed value or the one in flight.
func SwapRoot(t *testing.T, dir string, open Opener) {
	ctx := context.Background()
	next := 0
	current := -1
	iters := Iterations()
	for i := 0; i < iters; i++ {
		p := runChild(t, "swap", dir, next)
		s, err := open(dir)
		if err != nil {
			t.Fatalf("iteration %d: the store does not reopen after kill -9: %v", i, err)
		}
		r, err := s.Root(ctx)
		if err != nil {
			t.Fatalf("iteration %d: the root does not read after kill -9 (%v): a crash mid-swap tore it", i, err)
		}
		okValues := map[string]int{}
		if p.lastDone >= next {
			okValues[string(Value(p.lastDone))] = p.lastDone
		} else if current >= 0 {
			okValues[string(Value(current))] = current
		}
		if p.lastTry > p.lastDone {
			okValues[string(Value(p.lastTry))] = p.lastTry
		}
		n, ok := okValues[string(r.Value)]
		if !ok && !(current < 0 && p.lastDone < next && len(r.Value) == 0) {
			t.Fatalf("iteration %d: after kill -9 the root is %q; want the last completed swap or the one in "+
				"flight (%v): the store is neither old nor new", i, r.Value, okValues)
		}
		if ok {
			current = n
		}
		next = max(p.lastTry, p.lastDone) + 1
	}
	if current < 0 {
		t.Fatalf("no swap completed in %d iterations; the harness never reached SwapRoot", iters)
	}
}

// Put kills a child mid-Put repeatedly. Every object that exists afterwards
// must be complete, and every Put that returned must have left its object.
func Put(t *testing.T, dir string, open Opener) {
	ctx := context.Background()
	next := 0
	iters := Iterations()
	completed := 0
	for i := 0; i < iters; i++ {
		p := runChild(t, "put", dir, next)
		s, err := open(dir)
		if err != nil {
			t.Fatalf("iteration %d: the store does not reopen after kill -9: %v", i, err)
		}
		for n := next; n <= p.lastDone; n++ {
			// The authority is the bytes: an acknowledged Put must read back whole.
			rc, err := s.Get(ctx, ObjectName(n), 0, -1)
			if err != nil {
				t.Fatalf("iteration %d: object %d was acknowledged but does not read after kill -9: %v", i, n, err)
			}
			b, err := io.ReadAll(rc)
			rc.Close()
			if err != nil || !bytes.Equal(b, Body(n)) {
				t.Fatalf("iteration %d: acknowledged object %d reads %d bytes that differ from the %d written (err=%v)",
					i, n, len(b), len(Body(n)), err)
			}
			completed++
		}
		if p.lastTry > p.lastDone {
			rc, err := s.Get(ctx, ObjectName(p.lastTry), 0, -1)
			if err == nil {
				b, rerr := io.ReadAll(rc)
				rc.Close()
				if rerr != nil || !bytes.Equal(b, Body(p.lastTry)) {
					t.Fatalf("iteration %d: the Put killed in flight left a partial object (%d of %d bytes)",
						i, len(b), len(Body(p.lastTry)))
				}
			}
		}
		next = max(p.lastTry, p.lastDone) + 1
	}
	if completed == 0 {
		t.Fatalf("no Put completed in %d iterations; the harness never exercised Put", iters)
	}
}
