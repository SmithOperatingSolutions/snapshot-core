package main

import (
	"slices"
	"testing"
)

// Fuzzing on a developer's machine leaves room for everything else on it.
// Go starts one fuzz worker per core unless told otherwise, and 17 of them
// at 3.5 GB each took a 16-core, 62 GB host out of memory twice, killing
// the terminals and agent sessions beside them (#22).
func TestFuzzWorkersAreCappedByDefault(t *testing.T) {
	for _, c := range []struct{ cpus, want int }{
		{1, 1}, {2, 1}, {4, 2}, {8, 4}, {16, 4}, {64, 4},
	} {
		if got := defaultFuzzParallel(c.cpus); got != c.want {
			t.Errorf("on a %d-core machine the fuzz step starts %d workers per target, want %d: one per core is how a fuzz run took the host down", c.cpus, got, c.want)
		}
	}
}

// Every fuzz run is told its worker count and a soft memory limit, so each
// worker collects garbage before it grows without bound, and an operator
// can raise or lower both.
func TestAFuzzRunIsToldItsWorkersAndMemoryLimit(t *testing.T) {
	for _, c := range []struct {
		parallel int
		limit    string
		flag     string
	}{
		{4, "2GiB", "4"},
		{1, "512MiB", "1"},
		{12, "6GiB", "12"},
	} {
		cfg := &config{fuzzTime: "30s", fuzzParallel: c.parallel, fuzzMemLimit: c.limit}
		env, args := fuzzCommand(cfg, "example.com/m/p", "FuzzDecode")
		i := slices.Index(args, "-parallel")
		if i < 0 || i+1 >= len(args) || args[i+1] != c.flag {
			t.Errorf("asked for %d fuzz workers, the step runs go %v: without -parallel %s Go starts one worker per core", c.parallel, args, c.flag)
		}
		if !slices.Contains(env, "GOMEMLIMIT="+c.limit) {
			t.Errorf("asked for a %s memory limit, the step's environment is %v: without GOMEMLIMIT=%s a worker grows until the kernel kills something", c.limit, env, c.limit)
		}
		if !slices.Contains(args, "^FuzzDecode$") || !slices.Contains(args, "example.com/m/p") || !slices.Contains(args, "30s") {
			t.Errorf("the fuzz command lost its target, package or time: go %v", args)
		}
	}
}
