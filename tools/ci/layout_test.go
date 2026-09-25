package main

import "testing"

// The runner is not the storage core's alone: told a module path and which
// directories are its product and its adapters, it gates that module. A
// consumer with its own layout (here lib/, model/ and api/, no adapters)
// runs it as a Go tool.
func TestTheRunnerTakesTheModulesLayout(t *testing.T) {
	l := newLayout("example.com/consumer", "lib/,model/,api/", "")
	if l.module != "example.com/consumer/" {
		t.Errorf("module = %q, want the path with a trailing slash", l.module)
	}
	for rel, want := range map[string]bool{"lib": true, "model/kv": true, "api": true, "tools/ci": false, "e2e": false} {
		if got := l.isProduct(rel); got != want {
			t.Errorf("isProduct(%q) = %v, want %v", rel, got, want)
		}
	}
	for rel, want := range map[string]float64{
		"lib":            90,
		"model/kv":       90,
		"api":            90,
		"model/contract": 0, // a test suite, exercised by its callers
		"tools/ci":       0, // not the product
		"core/dnx":       0, // not this module's product at all
	} {
		if got := l.Threshold(rel); got != want {
			t.Errorf("Threshold(%q) = %v, want %v", rel, got, want)
		}
	}
	if got := l.rel("example.com/consumer/model/kv"); got != "model/kv" {
		t.Errorf("rel = %q, want model/kv", got)
	}
	profile := "mode: set\nexample.com/consumer/lib/lib.go:1.1,2.2 4 1\nexample.com/consumer/lib/lib.go:3.1,4.2 6 0\n"
	cs := l.CoverageFromProfile(profile)
	if len(cs) != 1 || cs[0].Pkg != "lib" || cs[0].Percent != 40 {
		t.Errorf("coverage from the consumer's profile = %+v, want lib at 40%%", cs)
	}
	core := coreLayout("github.com/SmithOperatingSolutions/snapshot-core")
	if core.Threshold("core/dnx") != 80 || core.Threshold("core/blob/s3") != 80 || core.Threshold("core/prolly") != 90 {
		t.Errorf("the core's own layout lost its adapter gates: %+v", core)
	}
}
