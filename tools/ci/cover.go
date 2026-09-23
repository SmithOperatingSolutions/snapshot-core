package main

// Coverage gate (Engine Spec: 90% on core packages, 80% on adapters). Go
// measures statement coverage, not branch coverage; statements are what the
// gate holds.

// PackageCoverage is one package's statement coverage from `go test -cover`.
type PackageCoverage struct {
	Pkg     string
	Percent float64
	NoTests bool // "[no test files]"
}

// ParseCover extracts per-package coverage from `go test -cover` output.
func ParseCover(out string) []PackageCoverage {
	return nil // stub
}

// Threshold is the minimum statement coverage a package must reach.
func Threshold(pkg string) float64 {
	return 0 // stub
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
	return nil // stub
}
