package main

import "strings"

// layout is what the runner knows about the module it runs in: the module
// path, the directories that are the product (gated for coverage, fuzzed,
// crash-tested, measured) and the product packages that are adapters over a
// third-party API (an 80% gate, Engine Spec). The storage core is the
// default; another module says its own with -product and -adapters.
type layout struct {
	module   string   // the module path with a trailing slash
	product  []string // directory prefixes, each with a trailing slash
	adapters []string // package paths relative to the module
}

// coreLayout is snapshot-core's own.
func coreLayout(module string) layout {
	return newLayout(module, "core/,model/", "core/dnx,core/blob/s3")
}

// newLayout builds a layout from the module path and the comma-separated
// -product and -adapters flags.
func newLayout(module, product, adapters string) layout {
	l := layout{module: strings.TrimSuffix(module, "/") + "/"}
	for _, p := range strings.Split(product, ",") {
		if p = strings.TrimSpace(p); p != "" {
			l.product = append(l.product, strings.TrimSuffix(p, "/")+"/")
		}
	}
	for _, a := range strings.Split(adapters, ",") {
		if a = strings.TrimSpace(a); a != "" {
			l.adapters = append(l.adapters, strings.TrimSuffix(a, "/"))
		}
	}
	return l
}

// rel is pkg relative to the module.
func (l layout) rel(pkg string) string { return strings.TrimPrefix(pkg, l.module) }

// isProduct says whether a package (relative to the module) is the product.
func (l layout) isProduct(rel string) bool {
	for _, p := range l.product {
		if rel == strings.TrimSuffix(p, "/") || strings.HasPrefix(rel, p) {
			return true
		}
	}
	return false
}

// isAdapter says whether a product package adapts a third-party API.
func (l layout) isAdapter(rel string) bool {
	for _, a := range l.adapters {
		if rel == a {
			return true
		}
	}
	return false
}
