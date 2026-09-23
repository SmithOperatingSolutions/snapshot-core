package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
)

func main() { os.Exit(run()) }

func run() int {
	var o Options
	flag.StringVar(&o.Base, "base", "", "branch point: commits in base..head are checked (default: main, else the root commit)")
	flag.StringVar(&o.Head, "head", "HEAD", "last commit to check")
	flag.StringVar(&o.Dir, "C", ".", "repository root")
	flag.Parse()
	o.Log = os.Stderr

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	rep, err := Check(ctx, o)
	if err != nil {
		fmt.Fprintln(os.Stderr, "redcheck:", err)
		return 2
	}
	fmt.Printf("redcheck: %s..%s: %d commits, %d test: commits checked, %d added tests run\n",
		rep.Base, o.Head, rep.Commits, rep.Checked, rep.TestsRun)
	for _, v := range rep.Violations {
		if v.Test != "" {
			fmt.Printf("  BLOCKED %s %q: %s: %s\n", v.Commit, v.Subject, v.Test, v.Reason)
		} else {
			fmt.Printf("  BLOCKED %s %q: %s\n", v.Commit, v.Subject, v.Reason)
		}
	}
	if len(rep.Violations) > 0 {
		return 1
	}
	return 0
}
