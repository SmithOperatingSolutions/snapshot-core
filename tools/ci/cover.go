package main

import (
	"regexp"
	"strconv"
	"strings"
)

// Coverage gate (Engine Spec: 90% on core packages, 80% on adapters). Go
// measures statement coverage, not branch coverage; statements are what the
// gate holds.

const modulePrefix = "github.com/SmithOperatingSolutions/snapshot-core/"

// PackageCoverage is one package's statement coverage from `go test -cover`.
type PackageCoverage struct {
	Pkg     string
	Percent float64
	NoTests bool // "[no test files]"
}

var (
	coverLine  = regexp.MustCompile(`^(?:ok\s+)?\s*(\S+)\s.*coverage: ([0-9.]+)% of statements`)
	noTestLine = regexp.MustCompile(`^\?\s+(\S+)\s+\[no test files\]`)
)

// ParseCover extracts per-package coverage from `go test -cover` output.
func ParseCover(out string) []PackageCoverage {
	var cs []PackageCoverage
	for _, line := range strings.Split(out, "\n") {
		if m := noTestLine.FindStringSubmatch(line); m != nil {
			cs = append(cs, PackageCoverage{Pkg: strings.TrimPrefix(m[1], modulePrefix), NoTests: true})
			continue
		}
		if strings.HasPrefix(line, "FAIL") || strings.HasPrefix(line, "---") {
			continue
		}
		if m := coverLine.FindStringSubmatch(line); m != nil {
			p, err := strconv.ParseFloat(m[2], 64)
			if err != nil {
				continue
			}
			cs = append(cs, PackageCoverage{Pkg: strings.TrimPrefix(m[1], modulePrefix), Percent: p})
		}
	}
	return cs
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

// GateCoverage returns the packages below their threshold. A core package with
// no tests at all fails: zero coverage is not "not applicable".
func GateCoverage(cs []PackageCoverage) []CoverFailure {
	var fs []CoverFailure
	for _, c := range cs {
		th := Threshold(c.Pkg)
		if th == 0 {
			continue
		}
		if c.NoTests || c.Percent < th {
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
	return nil // stub
}
