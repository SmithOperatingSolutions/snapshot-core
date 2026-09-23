package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
)

func main() {
	var o Options
	flag.StringVar(&o.Base, "base", "main", "branch point: commits in base..head are checked")
	flag.StringVar(&o.Head, "head", "HEAD", "last commit to check")
	flag.StringVar(&o.Dir, "C", ".", "repository root")
	flag.Parse()
	o.Log = os.Stderr

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	rep, err := Check(ctx, o)
	if err != nil {
		fmt.Fprintln(os.Stderr, "redcheck:", err)
		os.Exit(2)
	}
	fmt.Printf("redcheck: %d commits, %d test: commits checked, %d added tests run\n",
		rep.Commits, rep.Checked, rep.TestsRun)
	for _, v := range rep.Violations {
		if v.Test != "" {
			fmt.Printf("  BLOCKED %s %q: %s: %s\n", v.Commit, v.Subject, v.Test, v.Reason)
		} else {
			fmt.Printf("  BLOCKED %s %q: %s\n", v.Commit, v.Subject, v.Reason)
		}
	}
	if len(rep.Violations) > 0 {
		os.Exit(1)
	}
}
