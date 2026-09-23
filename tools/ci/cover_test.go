package main

import "testing"

const coverOut = `ok  	github.com/SmithOperatingSolutions/snapshot-core/core/blob/mem	0.012s	coverage: 97.1% of statements
ok  	github.com/SmithOperatingSolutions/snapshot-core/core/prolly	1.234s	coverage: 88.9% of statements
?   	github.com/SmithOperatingSolutions/snapshot-core/core/limits	[no test files]
ok  	github.com/SmithOperatingSolutions/snapshot-core/core/dnx	0.010s	coverage: 81.0% of statements
ok  	github.com/SmithOperatingSolutions/snapshot-core/core/hash	(cached)	coverage: 100.0% of statements
	github.com/SmithOperatingSolutions/snapshot-core/core/blob/contract		coverage: 0.0% of statements
FAIL	github.com/SmithOperatingSolutions/snapshot-core/core/vcs	0.5s
`

func TestParseCoverReadsEveryReportedPackage(t *testing.T) {
	cs := ParseCover(coverOut)
	got := map[string]PackageCoverage{}
	for _, c := range cs {
		got[c.Pkg] = c
	}
	want := map[string]PackageCoverage{
		"core/blob/mem":      {Pkg: "core/blob/mem", Percent: 97.1},
		"core/prolly":        {Pkg: "core/prolly", Percent: 88.9},
		"core/limits":        {Pkg: "core/limits", NoTests: true},
		"core/dnx":           {Pkg: "core/dnx", Percent: 81.0},
		"core/hash":          {Pkg: "core/hash", Percent: 100},
		"core/blob/contract": {Pkg: "core/blob/contract", Percent: 0},
	}
	if len(got) != len(want) {
		t.Fatalf("parsed %d packages, want %d: %+v", len(got), len(want), cs)
	}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("%s: parsed %+v, want %+v", k, got[k], w)
		}
	}
}

func TestThresholdsFollowTheEngineSpec(t *testing.T) {
	for pkg, want := range map[string]float64{
		"core/prolly":         90,
		"model/tree":          90,
		"core/dnx":            80, // the adapter over disknexus
		"core/blob/s3":        80, // the adapter over the AWS SDK
		"core/blob/contract":  0,  // a test-suite package: exercised by its callers
		"core/chunk/contract": 0,
		"core/blob/s3/s3fake": 0, // test infrastructure
		"model/contract":      0,
	} {
		if got := Threshold(pkg); got != want {
			t.Errorf("Threshold(%q) = %v, want %v", pkg, got, want)
		}
	}
}

func TestGateFailsLowCoverageAndUntestedCorePackages(t *testing.T) {
	fs := GateCoverage(ParseCover(coverOut))
	got := map[string]bool{}
	for _, f := range fs {
		got[f.Pkg] = true
	}
	if !got["core/prolly"] {
		t.Error("core/prolly at 88.9% passed a 90% gate")
	}
	if !got["core/limits"] {
		t.Error("a core package with no tests at all passed the coverage gate")
	}
	if got["core/dnx"] {
		t.Error("core/dnx at 81% failed its 80% adapter gate")
	}
	if got["core/blob/mem"] || got["core/hash"] || got["core/blob/contract"] {
		t.Errorf("a package at or above its threshold failed: %+v", fs)
	}
	if len(fs) != 2 {
		t.Errorf("want exactly core/prolly and core/limits to fail, got %+v", fs)
	}
}
