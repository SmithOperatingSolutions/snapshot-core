// Package compat holds the pinned-behavior suite for core/dnx: the facts about
// disknexus-engine this repository depends on (Storage Core Spec, dependency
// rule 5). A disknexus upgrade that changes any of them fails here, before it
// can reach the core. The package has no code, only tests.
package compat
