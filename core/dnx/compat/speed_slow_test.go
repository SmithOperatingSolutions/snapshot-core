//go:build slow

package compat_test

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/cdc"
	"github.com/SmithOperatingSolutions/snapshot-core/core/dnx"
)

// #10: disknexus's chunker reads a byte at a time and bounds a write at
// about 200 MB/s on one core. core/cdc cuts the same boundaries (the
// goldens and TestTheChunkerCutsWhereDisknexusCuts say so) at least twice
// as fast on the same machine in the same run, so the verdict does not
// depend on the machine.
func TestSlowTheChunkerOutrunsDisknexus(t *testing.T) {
	data := corpus(256 << 20)
	g := cdc.DefaultGeometry()
	t0 := time.Now()
	ref := dnx.NewChunker(bytes.NewReader(data), dnx.Geometry{Min: g.Min, Max: g.Max, Mask: g.Mask})
	refChunks := 0
	for {
		if _, err := ref.Next(); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		refChunks++
	}
	refTime := time.Since(t0)
	c, err := cdc.New(bytes.NewReader(data), g)
	if err != nil {
		t.Fatal(err)
	}
	t0 = time.Now()
	ours := 0
	for {
		if _, err := c.Next(); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		ours++
	}
	ourTime := time.Since(t0)
	t.Logf("256 MiB: disknexus %v (%.0f MB/s, %d chunks), core/cdc %v (%.0f MB/s, %d chunks)",
		refTime.Round(time.Millisecond), 256*1.048576e6/1e6/refTime.Seconds(), refChunks,
		ourTime.Round(time.Millisecond), 256*1.048576e6/1e6/ourTime.Seconds(), ours)
	if ours != refChunks {
		t.Fatalf("core/cdc cut %d chunks, disknexus %d", ours, refChunks)
	}
	if ourTime*2 > refTime {
		t.Fatalf("core/cdc took %v against disknexus's %v: under twice as fast", ourTime.Round(time.Millisecond), refTime.Round(time.Millisecond))
	}
}
