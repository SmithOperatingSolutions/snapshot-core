package main

import "testing"

// The runner is not the storage core's alone: told a module path and which
// directories are its product and its adapters, it gates that module. The
// engine repository (merge/, model/, engine/, no adapters) is the first
// other user.
func TestTheRunnerTakesTheModulesLayout(t *testing.T) {
	l := newLayout("github.com/SmithOperatingSolutions/snapshot-engine", "merge/,model/,engine/", "")
	if l.module != "github.com/SmithOperatingSolutions/snapshot-engine/" {
		t.Errorf("module = %q, want the path with a trailing slash", l.module)
	}
	for rel, want := range map[string]bool{"merge": true, "model/kv": true, "engine": true, "tools/ci": false, "e2e": false} {
		if got := l.isProduct(rel); got != want {
			t.Errorf("isProduct(%q) = %v, want %v", rel, got, want)
		}
	}
	for rel, want := range map[string]float64{
		"merge":          90,
		"model/kv":       90,
		"engine":         90,
		"model/contract": 0, // a test suite, exercised by its callers
		"tools/ci":       0, // not the product
		"core/dnx":       0, // not this module's product at all
	} {
		if got := l.Threshold(rel); got != want {
			t.Errorf("Threshold(%q) = %v, want %v", rel, got, want)
		}
	}
	if got := l.rel("github.com/SmithOperatingSolutions/snapshot-engine/model/kv"); got != "model/kv" {
		t.Errorf("rel = %q, want model/kv", got)
	}
	profile := "mode: set\ngithub.com/SmithOperatingSolutions/snapshot-engine/merge/merge.go:1.1,2.2 4 1\ngithub.com/SmithOperatingSolutions/snapshot-engine/merge/merge.go:3.1,4.2 6 0\n"
	cs := l.CoverageFromProfile(profile)
	if len(cs) != 1 || cs[0].Pkg != "merge" || cs[0].Percent != 40 {
		t.Errorf("coverage from the engine's profile = %+v, want merge at 40%%", cs)
	}
	core := coreLayout("github.com/SmithOperatingSolutions/snapshot-core")
	if core.Threshold("core/dnx") != 80 || core.Threshold("core/blob/s3") != 80 || core.Threshold("core/prolly") != 90 {
		t.Errorf("the core's own layout lost its adapter gates: %+v", core)
	}
}
