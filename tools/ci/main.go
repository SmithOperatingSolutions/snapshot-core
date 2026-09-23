// Command ci is the local run-all: every check CI runs that this machine can.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "ci: not implemented")
	os.Exit(2)
}
