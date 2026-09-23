// Package contract is the suite every chunk.Store must pass, unchanged
// (Engine Spec L0 "First failing tests", boundary rule 4). It holds the
// Engine Spec's L0 tests verbatim in intent: identical bytes back, unknown is
// ErrNotFound, one copy of the same bytes, a flipped byte is ErrCorrupt, a
// stale CAS changes nothing, 100 racing CASes have one winner, a root must
// name a stored chunk, 1 MiB + 1 is ErrTooLarge.
package contract

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
)

// Subject is one store under test and the hooks the suite needs.
type Subject struct {
	Store chunk.Store
	// Tamper corrupts the stored bytes of h wherever the store keeps them.
	Tamper func(t *testing.T, h hash.Hash)
	// Reopen returns a fresh store on the same storage; nil for a store
	// that is not durable.
	Reopen func(t *testing.T) chunk.Store
}

// Factory returns a fresh, empty subject.
type Factory func(t *testing.T) Subject

// Options tunes the suite.
type Options struct {
	Racers int // concurrent CompareAndSetRoot callers per round (default 100)
	Rounds int // default 5
}

var ctx = context.Background()

// Run runs the contract.
func Run(t *testing.T, newSubject Factory, opts Options) {
	if opts.Racers == 0 {
		opts.Racers = 100
	}
	if opts.Rounds == 0 {
		opts.Rounds = 5
	}
	t.Run("PutGetIdentical", func(t *testing.T) { putGetIdentical(t, newSubject(t)) })
	t.Run("IdentityIsSHA256", func(t *testing.T) { identityIsSHA256(t, newSubject(t)) })
	t.Run("UnknownIsNotFound", func(t *testing.T) { unknownIsNotFound(t, newSubject(t)) })
	t.Run("SameBytesAreOneChunk", func(t *testing.T) { sameBytesAreOneChunk(t, newSubject(t)) })
	t.Run("FlippedByteIsCorrupt", func(t *testing.T) { flippedByteIsCorrupt(t, newSubject(t)) })
	t.Run("TooLarge", func(t *testing.T) { tooLarge(t, newSubject(t)) })
	t.Run("Has", func(t *testing.T) { has(t, newSubject(t)) })
	t.Run("RootLifecycle", func(t *testing.T) { rootLifecycle(t, newSubject(t)) })
	t.Run("StaleCASChangesNothing", func(t *testing.T) { staleCAS(t, newSubject(t)) })
	t.Run("RootMustBeStored", func(t *testing.T) { rootMustBeStored(t, newSubject(t)) })
	t.Run("RacingCASOneWinnerPerRound", func(t *testing.T) { racingCAS(t, newSubject(t), opts) })
	t.Run("ReopenKeepsRootAndChunks", func(t *testing.T) { reopen(t, newSubject(t)) })
	t.Run("ConcurrentPutsAndGets", func(t *testing.T) { concurrent(t, newSubject(t)) })
}

func payload(seed string, n int) []byte {
	out := make([]byte, 0, n+32)
	for i := 0; len(out) < n; i++ {
		h := sha256.Sum256([]byte(fmt.Sprintf("%s/%d", seed, i)))
		out = append(out, h[:]...)
	}
	return out[:n]
}

func put(t *testing.T, s chunk.Store, b []byte) hash.Hash {
	t.Helper()
	h, err := s.Put(ctx, b)
	if err != nil {
		t.Fatalf("Put(%d bytes): %v", len(b), err)
	}
	return h
}

func mustGet(t *testing.T, s chunk.Store, h hash.Hash) []byte {
	t.Helper()
	b, err := s.Get(ctx, h)
	if err != nil {
		t.Fatalf("Get(%s): %v", h.Short(), err)
	}
	return b
}

func putGetIdentical(t *testing.T, sub Subject) {
	for _, n := range []int{0, 1, 4096, chunk.MaxChunkSize} {
		want := payload(fmt.Sprintf("identical-%d", n), n)
		h := put(t, sub.Store, want)
		if got := mustGet(t, sub.Store, h); !bytes.Equal(got, want) {
			t.Fatalf("a %d-byte chunk read back as %d different bytes", n, len(got))
		}
	}
	// Compressible data too, which a packing store transforms on the way in.
	text := bytes.Repeat([]byte("the same line again\n"), 5000)
	if got := mustGet(t, sub.Store, put(t, sub.Store, text)); !bytes.Equal(got, text) {
		t.Fatal("a compressible chunk read back differently")
	}
}

func identityIsSHA256(t *testing.T, sub Subject) {
	b := payload("identity", 777)
	if got, want := put(t, sub.Store, b), hash.Hash(sha256.Sum256(b)); got != want {
		t.Fatalf("Put returned %s, want SHA-256 of the bytes %s", got.Short(), want.Short())
	}
}

func unknownIsNotFound(t *testing.T, sub Subject) {
	h := put(t, sub.Store, []byte("present"))
	if _, err := sub.Store.Get(ctx, h); err != nil {
		t.Fatalf("positive control: %v", err)
	}
	if _, err := sub.Store.Get(ctx, hash.Sum([]byte("absent"))); !errors.Is(err, chunk.ErrNotFound) {
		t.Fatalf("Get of an unknown hash = %v, want ErrNotFound", err)
	}
}

func sameBytesAreOneChunk(t *testing.T, sub Subject) {
	b := payload("once", 10000)
	h1 := put(t, sub.Store, b)
	before, err := sub.Store.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	h2 := put(t, sub.Store, bytes.Clone(b))
	after, err := sub.Store.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if h1 != h2 {
		t.Fatalf("the same bytes stored twice got two hashes %s and %s", h1.Short(), h2.Short())
	}
	if before.Chunks < 1 || after.Chunks != before.Chunks {
		t.Fatalf("storing the same bytes again took the store from %d to %d chunks: no dedup", before.Chunks, after.Chunks)
	}
}

func flippedByteIsCorrupt(t *testing.T, sub Subject) {
	victim := put(t, sub.Store, payload("victim", 50000))
	bystander := put(t, sub.Store, payload("bystander", 50000))
	if err := sub.Store.CompareAndSetRoot(ctx, hash.Hash{}, bystander); err != nil {
		t.Fatal(err) // publish, so a packing store has flushed both to storage
	}
	sub.Tamper(t, victim)
	if b, err := sub.Store.Get(ctx, victim); !errors.Is(err, chunk.ErrCorrupt) {
		t.Fatalf("Get of a chunk with a flipped byte = %d bytes, %v; want ErrCorrupt: tampered data was trusted", len(b), err)
	}
	if got := mustGet(t, sub.Store, bystander); !bytes.Equal(got, payload("bystander", 50000)) {
		t.Fatal("tampering one chunk damaged another")
	}
}

func tooLarge(t *testing.T, sub Subject) {
	if _, err := sub.Store.Put(ctx, make([]byte, chunk.MaxChunkSize+1)); !errors.Is(err, chunk.ErrTooLarge) {
		t.Fatalf("Put of 1 MiB + 1 byte = %v, want ErrTooLarge", err)
	}
	put(t, sub.Store, make([]byte, chunk.MaxChunkSize)) // positive control: exactly the limit
}

func has(t *testing.T, sub Subject) {
	a, b := put(t, sub.Store, []byte("a")), put(t, sub.Store, []byte("b"))
	missing := hash.Sum([]byte("missing"))
	got, err := sub.Store.Has(ctx, []hash.Hash{a, missing, b})
	if err != nil {
		t.Fatal(err)
	}
	if !got[a] || !got[b] || got[missing] || len(got) != 3 {
		t.Fatalf("Has = %v, want a and b true, missing false, one answer each", got)
	}
}

func rootLifecycle(t *testing.T, sub Subject) {
	s := sub.Store
	if r, err := s.Root(ctx); err != nil || !r.IsZero() {
		t.Fatalf("a new store's root = %s, %v; want the zero hash", r.Short(), err)
	}
	c1, c2 := put(t, s, []byte("root one")), put(t, s, []byte("root two"))
	if err := s.CompareAndSetRoot(ctx, hash.Hash{}, c1); err != nil {
		t.Fatalf("setting the first root: %v", err)
	}
	if r, _ := s.Root(ctx); r != c1 {
		t.Fatalf("root = %s, want %s", r.Short(), c1.Short())
	}
	if err := s.CompareAndSetRoot(ctx, hash.Hash{}, c2); !errors.Is(err, chunk.ErrRootConflict) {
		t.Fatalf("setting a first root when one exists = %v, want ErrRootConflict", err)
	}
	if err := s.CompareAndSetRoot(ctx, c1, c2); err != nil {
		t.Fatalf("CAS with the current root: %v", err)
	}
	if r, _ := s.Root(ctx); r != c2 {
		t.Fatalf("root = %s, want %s", r.Short(), c2.Short())
	}
}

func staleCAS(t *testing.T, sub Subject) {
	s := sub.Store
	c1, c2, c3 := put(t, s, []byte("1")), put(t, s, []byte("2")), put(t, s, []byte("3"))
	if err := s.CompareAndSetRoot(ctx, hash.Hash{}, c1); err != nil {
		t.Fatal(err)
	}
	if err := s.CompareAndSetRoot(ctx, c1, c2); err != nil {
		t.Fatal(err)
	}
	if err := s.CompareAndSetRoot(ctx, c1, c3); !errors.Is(err, chunk.ErrRootConflict) {
		t.Fatalf("CAS with a stale expected root = %v, want ErrRootConflict: a concurrent commit would be lost", err)
	}
	if r, _ := s.Root(ctx); r != c2 {
		t.Fatalf("after a refused CAS the root is %s, want %s", r.Short(), c2.Short())
	}
}

func rootMustBeStored(t *testing.T, sub Subject) {
	s := sub.Store
	if err := s.CompareAndSetRoot(ctx, hash.Hash{}, hash.Sum([]byte("never stored"))); !errors.Is(err, chunk.ErrRootMissing) {
		t.Fatalf("CAS to a chunk that was never stored = %v, want ErrRootMissing", err)
	}
	if err := s.CompareAndSetRoot(ctx, hash.Hash{}, hash.Hash{}); !errors.Is(err, chunk.ErrRootMissing) {
		t.Fatalf("CAS to the zero hash = %v, want ErrRootMissing", err)
	}
	if r, _ := s.Root(ctx); !r.IsZero() {
		t.Fatalf("a refused CAS set the root to %s", r.Short())
	}
}

// Engine Spec: "100 goroutines racing CompareAndSetRoot: exactly one wins per round".
func racingCAS(t *testing.T, sub Subject, opts Options) {
	s := sub.Store
	cur := put(t, s, []byte("round 0"))
	if err := s.CompareAndSetRoot(ctx, hash.Hash{}, cur); err != nil {
		t.Fatal(err)
	}
	for round := 1; round <= opts.Rounds; round++ {
		mine := make([]hash.Hash, opts.Racers)
		for i := range mine {
			mine[i] = put(t, s, []byte(fmt.Sprintf("round %d racer %d", round, i)))
		}
		var (
			wg      sync.WaitGroup
			mu      sync.Mutex
			winners []int
			others  []error
			start   = make(chan struct{})
		)
		for i := 0; i < opts.Racers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				err := s.CompareAndSetRoot(ctx, cur, mine[i])
				mu.Lock()
				defer mu.Unlock()
				switch {
				case err == nil:
					winners = append(winners, i)
				case !errors.Is(err, chunk.ErrRootConflict):
					others = append(others, err)
				}
			}(i)
		}
		close(start)
		wg.Wait()
		if len(others) > 0 {
			t.Fatalf("round %d: a loser failed with %v, want ErrRootConflict", round, others[0])
		}
		if len(winners) != 1 {
			t.Fatalf("round %d: %d of %d racers won the same CAS, want exactly 1", round, len(winners), opts.Racers)
		}
		r, err := s.Root(ctx)
		if err != nil || r != mine[winners[0]] {
			t.Fatalf("round %d: root is %s, the winner set %s (%v)", round, r.Short(), mine[winners[0]].Short(), err)
		}
		cur = r
	}
}

func reopen(t *testing.T, sub Subject) {
	if sub.Reopen == nil {
		return // not a durable store
	}
	data := payload("durable", 30000)
	h := put(t, sub.Store, data)
	if err := sub.Store.CompareAndSetRoot(ctx, hash.Hash{}, h); err != nil {
		t.Fatal(err)
	}
	re := sub.Reopen(t)
	if r, err := re.Root(ctx); err != nil || r != h {
		t.Fatalf("after reopening, root = %s (%v), want %s", r.Short(), err, h.Short())
	}
	if got := mustGet(t, re, h); !bytes.Equal(got, data) {
		t.Fatal("after reopening, the root chunk reads differently")
	}
}

func concurrent(t *testing.T, sub Subject) {
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				b := payload(fmt.Sprintf("g%d-%d", g, i%10), 2000+i)
				h, err := sub.Store.Put(ctx, b)
				if err != nil {
					errs <- err
					return
				}
				got, err := sub.Store.Get(ctx, h)
				if err != nil || !bytes.Equal(got, b) {
					errs <- fmt.Errorf("goroutine %d read back %d bytes (%w)", g, len(got), err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}
