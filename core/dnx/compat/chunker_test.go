package compat_test

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"testing/iotest"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/cdc"
	"github.com/SmithOperatingSolutions/snapshot-core/core/dnx"
)

// cutAll returns every chunk a chunker cuts and the error it stops with.
func cutAll(next func() ([]byte, error)) (chunks [][]byte, err error) {
	for {
		data, err := next()
		if err != nil {
			return chunks, err
		}
		chunks = append(chunks, data)
	}
}

func dnxNext(c *dnx.Chunker) func() ([]byte, error) {
	return func() ([]byte, error) {
		ch, err := c.Next()
		return ch.Data, err
	}
}

// #10: core/cdc cuts where disknexus cuts, byte for byte: the same chunks
// in the same order over random, zero, repeating and tiny streams, at both
// geometries, however the reader hands the bytes over (one at a time, in
// halves, data together with EOF), and it stops with the same error at
// the same chunk when a read fails or makes no progress.
func TestTheChunkerCutsWhereDisknexusCuts(t *testing.T) {
	random := corpus(4 << 20)
	var zeros [1 << 20]byte
	repeating := bytes.Repeat([]byte("abcabcabd"), 200_000)
	streams := map[string][]byte{
		"random 4 MiB": random, "zeros 1 MiB": zeros[:], "repeating 1.8 MB": repeating,
		"empty": nil, "one byte": random[:1], "a window less one": random[:47], "a window": random[:48], "a window and one": random[:49],
		"exactly Min": random[:16<<10], "exactly Max": random[:512<<10],
	}
	readers := map[string]func([]byte) io.Reader{
		"whole":         func(b []byte) io.Reader { return bytes.NewReader(b) },
		"one byte":      func(b []byte) io.Reader { return iotest.OneByteReader(bytes.NewReader(b)) },
		"halves":        func(b []byte) io.Reader { return iotest.HalfReader(bytes.NewReader(b)) },
		"data with EOF": func(b []byte) io.Reader { return iotest.DataErrReader(bytes.NewReader(b)) },
	}
	for gName, g := range map[string]cdc.Geometry{"repo": cdc.DefaultGeometry(), "fine": {Min: 4 << 10, Max: 64 << 10, Mask: 0x1FFF}} {
		dg := dnx.Geometry{Min: g.Min, Max: g.Max, Mask: g.Mask}
		for sName, data := range streams {
			for rName, mk := range readers {
				if rName == "one byte" && len(data) > 1<<20 {
					continue // a byte a call over megabytes is minutes for nothing more
				}
				want, wantErr := cutAll(dnxNext(dnx.NewChunker(mk(data), dg)))
				c, err := cdc.New(mk(data), g)
				if err != nil {
					t.Fatal(err)
				}
				got, gotErr := cutAll(c.Next)
				if !errors.Is(gotErr, io.EOF) || !errors.Is(wantErr, io.EOF) {
					t.Fatalf("%s, %s, read %s: stopped with %v, disknexus with %v", gName, sName, rName, gotErr, wantErr)
				}
				if len(got) != len(want) {
					t.Fatalf("%s, %s, read %s: core/cdc cut %d chunks, disknexus %d", gName, sName, rName, len(got), len(want))
				}
				for i := range want {
					if !bytes.Equal(got[i], want[i]) {
						t.Fatalf("%s, %s, read %s: chunk %d is %d bytes, disknexus's %d, or differs", gName, sName, rName, i, len(got[i]), len(want[i]))
					}
				}
				if len(data) > 0 && len(got) == 0 {
					t.Fatalf("positive control: %s cut no chunks", sName)
				}
			}
		}
	}
	g := cdc.DefaultGeometry()
	// The byte that makes a chunk exactly Min long is judged by the easy
	// mask, not the hard one: a stream is drawn from the corpus in which
	// the easy mask hits at that byte while the hard one does not, so a
	// chunker that keeps the hard mask one byte too long cuts elsewhere.
	fine := cdc.Geometry{Min: 4 << 10, Max: 64 << 10, Mask: 0x1FFF}
	atMin := streamCuttingExactlyAtMin(corpus(32<<20), fine)
	if atMin == nil {
		t.Fatal("positive control: 32 MiB of corpus never cuts a chunk of exactly Min by the easy mask alone")
	}
	want, wantErr := cutAll(dnxNext(dnx.NewChunker(bytes.NewReader(atMin), dnx.Geometry{Min: fine.Min, Max: fine.Max, Mask: fine.Mask})))
	c, _ := cdc.New(bytes.NewReader(atMin), fine)
	got, gotErr := cutAll(c.Next)
	if !errors.Is(gotErr, io.EOF) || !errors.Is(wantErr, io.EOF) || len(got) != len(want) {
		t.Fatalf("a chunk of exactly Min: core/cdc cut %d chunks (%v), disknexus %d (%v)", len(got), gotErr, len(want), wantErr)
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("a chunk of exactly Min: chunk %d is %d bytes, disknexus's %d", i, len(got[i]), len(want[i]))
		}
	}
	// A read that returns bytes together with its error: the bytes are cut
	// first, then the error comes out, as from disknexus.
	withErr := func() io.Reader { return &bytesThenError{data: random[:256<<10]} } // the last 64 KiB read carries the error, and cuts in it
	want, wantErr = cutAll(dnxNext(dnx.NewChunker(withErr(), dnx.Geometry{Min: g.Min, Max: g.Max, Mask: g.Mask})))
	c, _ = cdc.New(withErr(), g)
	got, gotErr = cutAll(c.Next)
	if !errors.Is(gotErr, errBoom) || !errors.Is(wantErr, errBoom) || len(got) != len(want) || len(want) == 0 {
		t.Fatalf("bytes returned with an error: core/cdc cut %d chunks and stopped with %v, disknexus %d and %v; want the same, and some chunks", len(got), gotErr, len(want), wantErr)
	}
	// A read that fails after some bytes: the chunks cut before it come out,
	// the failure comes out where disknexus's does, and the chunk under way
	// when it struck does not.
	failing := func() io.Reader { return iotest.TimeoutReader(bytes.NewReader(random[:200<<10])) }
	want, wantErr = cutAll(dnxNext(dnx.NewChunker(failing(), dnx.Geometry{Min: g.Min, Max: g.Max, Mask: g.Mask})))
	c, _ = cdc.New(failing(), g)
	got, gotErr = cutAll(c.Next)
	if !errors.Is(gotErr, iotest.ErrTimeout) || !errors.Is(wantErr, iotest.ErrTimeout) {
		t.Fatalf("a failing read: core/cdc stopped with %v, disknexus with %v, want the read's error from both", gotErr, wantErr)
	}
	if len(got) != len(want) {
		t.Fatalf("a failing read: core/cdc cut %d chunks before it, disknexus %d", len(got), len(want))
	}
	// A reader that makes no progress is given up on, as bufio does.
	c, _ = cdc.New(noProgress{}, g)
	if err := nextWithin(t, c.Next, 5*time.Second); !errors.Is(err, io.ErrNoProgress) {
		t.Fatalf("a reader returning (0, nil) forever: %v, want io.ErrNoProgress", err)
	}
	// Chunks are the caller's: cutting on does not alter one already returned.
	c, _ = cdc.New(bytes.NewReader(random), g)
	first, err := c.Next()
	if err != nil {
		t.Fatal(err)
	}
	keep := append([]byte(nil), first...)
	if _, err := cutAll(c.Next); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if !bytes.Equal(first, keep) {
		t.Fatal("the first chunk's bytes changed while the chunker cut on: a chunk must be the caller's")
	}
}

type noProgress struct{}

func (noProgress) Read([]byte) (int, error) { return 0, nil }

var errBoom = errors.New("boom")

// bytesThenError hands all its bytes over with the error, in one read.
type bytesThenError struct {
	data []byte
	done bool
}

func (r *bytesThenError) Read(p []byte) (int, error) {
	if r.done {
		return 0, errBoom
	}
	r.done = true
	n := copy(p, r.data)
	if n < len(r.data) {
		r.data = r.data[n:]
		r.done = false
		return n, nil
	}
	return n, errBoom
}

// streamCuttingExactlyAtMin rolls disknexus's hash over data as the
// reference does and returns a prefix in which some chunk is exactly Min
// bytes long because the easy mask hit at its last byte while the hard
// mask did not; nil if data has no such chunk.
func streamCuttingExactlyAtMin(data []byte, g cdc.Geometry) []byte {
	lens := refLengths(data, dnx.Geometry{Min: g.Min, Max: g.Max, Mask: g.Mask})
	off := 0
	for _, n := range lens {
		off += n
		if n == g.Min {
			return data[:off+1000]
		}
	}
	return nil
}

// nextWithin calls next and returns its error, or fails the test if it has
// not returned within d: a chunker that never gives up on a reader must
// fail this test, not hang it.
func nextWithin(t *testing.T, next func() ([]byte, error), d time.Duration) error {
	t.Helper()
	errs := make(chan error, 1)
	go func() {
		_, err := next()
		errs <- err
	}()
	select {
	case err := <-errs:
		return err
	case <-time.After(d):
		t.Fatalf("Next did not return in %v on a reader that makes no progress", d)
		return nil
	}
}
