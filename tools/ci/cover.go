package main

import (
	"strconv"
	"strings"
)

// Coverage gate (Engine Spec: 90% on core packages, 80% on adapters). Go
// measures statement coverage, not branch coverage; statements are what the
// gate holds.

const modulePrefix = "github.com/SmithOperatingSolutions/snapshot-core/"

// PackageCoverage is one package's statement coverage.
type PackageCoverage struct {
	Pkg     string
	Percent float64
}

// Threshold is the minimum statement coverage a package must reach.
func Threshold(pkg string) float64 {
	segs := strings.Split(pkg, "/")
	last := segs[len(segs)-1]
	switch {
	case segs[0] != "core" && segs[0] != "model":
		return 0 // tools and anything outside the product
	case last == "contract" || strings.HasSuffix(last, "fake") || strings.HasSuffix(last, "test"):
		return 0 // test suites and test infrastructure, exercised by their callers
	case pkg == "core/dnx" || pkg == "core/blob/s3":
		return 80 // adapters over a third-party API
	default:
		return 90
	}
}

// CoverFailure is a package below its threshold.
type CoverFailure struct {
	Pkg       string
	Percent   float64
	Threshold float64
}

// GateCoverage returns the packages below their threshold. A package no test
// reaches is in the profile at 0% and fails: zero is not "not applicable".
func GateCoverage(cs []PackageCoverage) []CoverFailure {
	var fs []CoverFailure
	for _, c := range cs {
		th := Threshold(c.Pkg)
		if th == 0 {
			continue
		}
		if c.Percent < th {
			fs = append(fs, CoverFailure{Pkg: c.Pkg, Percent: c.Percent, Threshold: th})
		}
	}
	return fs
}

// CoverageFromProfile computes per-package statement coverage from a merged
// -coverpkg profile: a statement counts as covered if ANY test binary covered
// it, so a package exercised only by another package's tests (core/dnx by its
// compat suite, a contract suite by its backends) is measured as it is.
func CoverageFromProfile(profile string) []PackageCoverage {
	type block struct {
		pkg     string
		stmts   int
		covered bool
	}
	blocks := map[string]*block{} // file:range -> merged block
	var order []string
	for _, line := range strings.Split(profile, "\n") {
		if line == "" || strings.HasPrefix(line, "mode:") {
			continue
		}
		// github.com/.../core/dnx/dnx.go:10.1,12.2 2 1
		fields := strings.Fields(line)
		if len(fields) != 3 {
			continue
		}
		stmts, err1 := strconv.Atoi(fields[1])
		count, err2 := strconv.Atoi(fields[2])
		file, _, ok := strings.Cut(fields[0], ":")
		if err1 != nil || err2 != nil || !ok {
			continue
		}
		b, seen := blocks[fields[0]]
		if !seen {
			dir := file[:max(strings.LastIndex(file, "/"), 0)]
			b = &block{pkg: strings.TrimPrefix(dir, modulePrefix), stmts: stmts}
			blocks[fields[0]] = b
			order = append(order, fields[0])
		}
		b.covered = b.covered || count > 0
	}
	type tally struct{ total, covered int }
	per := map[string]*tally{}
	var pkgs []string
	for _, k := range order {
		b := blocks[k]
		t, ok := per[b.pkg]
		if !ok {
			t = &tally{}
			per[b.pkg] = t
			pkgs = append(pkgs, b.pkg)
		}
		t.total += b.stmts
		if b.covered {
			t.covered += b.stmts
		}
	}
	out := make([]PackageCoverage, 0, len(pkgs))
	for _, p := range pkgs {
		t := per[p]
		pct := 100.0
		if t.total > 0 {
			pct = float64(t.covered) * 100 / float64(t.total)
		}
		out = append(out, PackageCoverage{Pkg: p, Percent: pct})
	}
	return out
}
