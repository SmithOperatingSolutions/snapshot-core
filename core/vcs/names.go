package vcs

import (
	"fmt"
	"strings"
)

// validName checks a branch or tag name against the Engine Spec's allowlist
// ^[a-zA-Z0-9][a-zA-Z0-9._/-]{0,127}$, with no "..", no "//", and no trailing
// "/" or ".lock". Nothing is normalized.
func validName(n string) error {
	bad := n == "" || len(n) > 128 || strings.Contains(n, "..") || strings.Contains(n, "//") ||
		strings.HasSuffix(n, "/") || strings.HasSuffix(n, ".lock")
	for i := 0; i < len(n) && !bad; i++ {
		c := n[i]
		alnum := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
		bad = !alnum && (i == 0 || (c != '.' && c != '_' && c != '/' && c != '-'))
	}
	if bad {
		return fmt.Errorf("%w: %q", ErrInvalidName, n)
	}
	return nil
}
