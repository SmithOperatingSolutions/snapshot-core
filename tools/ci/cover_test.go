package main

import "testing"

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

func TestGateFailsLowCoverageAndUnreachedCorePackages(t *testing.T) {
	fs := GateCoverage([]PackageCoverage{
		{Pkg: "core/blob/mem", Percent: 97.1},
		{Pkg: "core/prolly", Percent: 88.9},
		{Pkg: "core/limits", Percent: 0}, // no test reaches it
		{Pkg: "core/dnx", Percent: 81.0},
		{Pkg: "core/hash", Percent: 100},
		{Pkg: "core/blob/contract", Percent: 0},
	})
	got := map[string]bool{}
	for _, f := range fs {
		got[f.Pkg] = true
	}
	if !got["core/prolly"] {
		t.Error("core/prolly at 88.9% passed a 90% gate")
	}
	if !got["core/limits"] {
		t.Error("a core package no test reaches passed the coverage gate")
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

// Two test binaries report the same blocks; a block covered by either counts.
const profile = `mode: set
github.com/SmithOperatingSolutions/snapshot-core/core/dnx/dnx.go:10.1,12.2 2 0
github.com/SmithOperatingSolutions/snapshot-core/core/dnx/dnx.go:14.1,20.2 8 0
github.com/SmithOperatingSolutions/snapshot-core/core/hash/hash.go:5.1,9.2 4 1
github.com/SmithOperatingSolutions/snapshot-core/core/dnx/dnx.go:10.1,12.2 2 1
github.com/SmithOperatingSolutions/snapshot-core/core/dnx/dnx.go:14.1,20.2 8 1
github.com/SmithOperatingSolutions/snapshot-core/core/hash/hash.go:5.1,9.2 4 0
github.com/SmithOperatingSolutions/snapshot-core/core/hash/hash.go:11.1,13.2 6 0
`

func TestCoverageFromProfileMergesBlocksAcrossTestBinaries(t *testing.T) {
	got := map[string]float64{}
	for _, c := range CoverageFromProfile(profile) {
		got[c.Pkg] = c.Percent
	}
	// core/dnx: both blocks covered by the second binary -> 10/10.
	// core/hash: 4 of 10 statements covered (the 6-statement block never) -> 40%.
	want := map[string]float64{"core/dnx": 100, "core/hash": 40}
	if len(got) != len(want) {
		t.Fatalf("got packages %v, want %v", got, want)
	}
	for pkg, w := range want {
		if got[pkg] != w {
			t.Errorf("%s: %.1f%%, want %.1f%% — a block covered by any test binary is covered, "+
				"and each block counts once however many binaries report it", pkg, got[pkg], w)
		}
	}
}
