package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
)

func main() {
	file := flag.String("f", "tools/mutate/mutants.txt", "mutants file")
	only := flag.String("only", "", "comma-separated mutant IDs to run (default: all)")
	flag.Parse()

	f, err := os.Open(*file)
	if err != nil {
		fmt.Fprintln(os.Stderr, "mutate:", err)
		os.Exit(2)
	}
	ms, err := Parse(f)
	f.Close()
	if err != nil {
		fmt.Fprintln(os.Stderr, "mutate:", err)
		os.Exit(2)
	}
	if *only != "" {
		want := map[string]bool{}
		for _, id := range strings.Split(*only, ",") {
			want[strings.TrimSpace(id)] = true
		}
		var sel []Mutant
		for _, m := range ms {
			if want[m.ID] {
				sel = append(sel, m)
			}
		}
		ms = sel
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	outs, err := Run(ctx, Options{Root: ".", Log: os.Stderr}, ms)
	if err != nil {
		fmt.Fprintln(os.Stderr, "mutate:", err)
		os.Exit(2)
	}
	bad := 0
	for _, o := range outs {
		fmt.Printf("%-9s %-40s %s\n", o.Status, o.Mutant.ID, o.Detail)
		if o.Status != Killed {
			bad++
		}
	}
	fmt.Printf("mutate: %d mutants, %d killed, %d not killed\n", len(outs), len(outs)-bad, bad)
	if bad > 0 || len(outs) == 0 {
		os.Exit(1)
	}
}
