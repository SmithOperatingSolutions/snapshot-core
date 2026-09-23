// Package split is a BlobStore whose objects live in one store and whose
// root lives in another (docs/DESIGN.md §4, #8): objects on an S3-compatible
// provider that ignores conditional writes (blob/s3 opened objects only),
// the root on a store that compare-and-swaps (blob/local). A copy of the
// root is kept on the objects store, in the mode the host picks, so losing
// the root store's disk loses nothing, or nothing but the swap in flight.
package split

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
)

// Errors.
var (
	ErrOptions     = errors.New("split: invalid options")
	ErrNoMirror    = errors.New("split: the objects store holds no copy of the root")
	ErrRootPresent = errors.New("split: the root store already holds a root")
)

// MirrorMode is how the root's copy on the objects store is kept.
type MirrorMode int

// Mirror modes.
const (
	MirrorWait       MirrorMode = iota // a swap returns once the copy has landed (the default)
	MirrorBackground                   // one writer uploads the newest root after each swap
	MirrorPeriodic                     // the newest root, if it changed, every Mirror.Every
	MirrorOff                          // no copy; the host backs up the root store
)

// Mirror is the root copy's mode and, for MirrorPeriodic, its period.
type Mirror struct {
	Mode  MirrorMode
	Every time.Duration
}

// Mirrorer keeps the root's copy: the unconditional replacement of one
// object outside the object namespace (blob/s3, blob/mem, blob/cache).
type Mirrorer interface {
	WriteMirror(ctx context.Context, value []byte) error
	ReadMirror(ctx context.Context) ([]byte, error)
}

// Options configures a split store.
type Options struct {
	Objects blob.BlobStore // holds the objects, and the root's copy unless Mirror is off
	Roots   blob.BlobStore // holds the root; it must compare-and-swap
	Mirror  Mirror
}

// Status is the root copy's state.
type Status struct {
	Mirrored  bool      // the newest root is on the objects store
	LastWrite time.Time // when a copy last landed
	LastError error     // what last went wrong writing one, until one lands
}

const (
	writeTimeout = 30 * time.Second // one copy
	waitAttempts = 3                // copies tried within a MirrorWait swap
	minBackoff   = 10 * time.Millisecond
	maxBackoff   = time.Second
)

// Store is a BlobStore over two.
type Store struct {
	o      Options
	mirror Mirrorer // nil when the copy is off

	mu        sync.Mutex
	pending   []byte // the newest root, until its copy lands
	mirrored  bool
	lastWrite time.Time
	lastErr   error

	wake  chan struct{} // MirrorBackground: a swap happened
	stop  chan struct{}
	done  chan struct{} // the writer has stopped
	close sync.Once
}

var _ blob.BlobStore = (*Store)(nil)

// New opens a split store.
func New(o Options) (*Store, error) {
	switch {
	case o.Objects == nil || o.Roots == nil:
		return nil, fmt.Errorf("%w: an objects store and a root store are required", ErrOptions)
	case o.Mirror.Mode < MirrorWait || o.Mirror.Mode > MirrorOff:
		return nil, fmt.Errorf("%w: mirror mode %d", ErrOptions, o.Mirror.Mode)
	case o.Mirror.Mode == MirrorPeriodic && o.Mirror.Every <= 0:
		return nil, fmt.Errorf("%w: a periodic mirror needs a period", ErrOptions)
	}
	s := &Store{o: o, mirrored: true, wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{})}
	if o.Mirror.Mode != MirrorOff {
		m, ok := o.Objects.(Mirrorer)
		if !ok {
			return nil, fmt.Errorf("%w: the objects store cannot hold a copy of the root", ErrOptions)
		}
		s.mirror = m
	}
	switch o.Mirror.Mode {
	case MirrorBackground:
		go s.writer(nil)
	case MirrorPeriodic:
		go s.writer(time.NewTicker(o.Mirror.Every))
	default:
		close(s.done)
	}
	return s, nil
}

// Put implements blob.BlobStore.
func (s *Store) Put(ctx context.Context, name string, r io.Reader, size int64) error {
	return s.o.Objects.Put(ctx, name, r, size)
}

// Get implements blob.BlobStore.
func (s *Store) Get(ctx context.Context, name string, off, n int64) (io.ReadCloser, error) {
	return s.o.Objects.Get(ctx, name, off, n)
}

// Stat implements blob.BlobStore.
func (s *Store) Stat(ctx context.Context, name string) (blob.Info, error) {
	return s.o.Objects.Stat(ctx, name)
}

// List implements blob.BlobStore.
func (s *Store) List(ctx context.Context, prefix, after string, limit int) ([]blob.Info, error) {
	return s.o.Objects.List(ctx, prefix, after, limit)
}

// Delete implements blob.BlobStore.
func (s *Store) Delete(ctx context.Context, name string) error { return s.o.Objects.Delete(ctx, name) }

// Root implements blob.BlobStore.
func (s *Store) Root(ctx context.Context) (blob.Root, error) { return s.o.Roots.Root(ctx) }

// SwapRoot implements blob.BlobStore: the swap on the root store, then the
// copy as the mode says. A swap that landed is reported as landed whatever
// became of its copy, since it cannot be undone; MirrorStatus says.
func (s *Store) SwapRoot(ctx context.Context, expected blob.Version, next []byte) (blob.Version, error) {
	v, err := s.o.Roots.SwapRoot(ctx, expected, next)
	if err != nil || s.mirror == nil {
		return v, err
	}
	s.mu.Lock()
	s.pending, s.mirrored = bytes.Clone(next), false
	s.mu.Unlock()
	switch s.o.Mirror.Mode {
	case MirrorWait:
		for attempt := 0; attempt < waitAttempts; attempt++ {
			if s.copy(ctx, next) == nil {
				break
			}
		}
	case MirrorBackground:
		select {
		case s.wake <- struct{}{}:
		default: // the writer is already awake and will see the newest root
		}
	}
	return v, nil
}

// copy writes one copy of value and records how it went. Callers hold no
// lock: a copy can be slow, and swaps must not wait on it unless they mean to.
func (s *Store) copy(ctx context.Context, value []byte) error {
	err := s.mirror.WriteMirror(ctx, value)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.lastErr = err
		return err
	}
	s.lastWrite, s.lastErr = time.Now(), nil
	if bytes.Equal(s.pending, value) {
		s.pending, s.mirrored = nil, true
	}
	return nil
}

// take returns the newest root whose copy has not landed, or nil.
func (s *Store) take() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pending
}

// writer is the MirrorBackground and MirrorPeriodic goroutine: it copies the
// newest root, skipping any it was superseded on, and retries a failed copy
// with backoff.
func (s *Store) writer(tick *time.Ticker) {
	defer close(s.done)
	if tick != nil {
		defer tick.Stop()
	}
	backoff := minBackoff
	for {
		var wait <-chan time.Time
		v := s.take()
		switch {
		case v == nil && tick != nil:
			wait = tick.C
		case v == nil:
			select {
			case <-s.stop:
				return
			case <-s.wake:
			}
			continue
		case tick != nil:
			// A period is due: copy now.
		default:
		}
		if wait != nil {
			select {
			case <-s.stop:
				return
			case <-wait:
			}
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
		err := s.copy(ctx, v)
		cancel()
		if err == nil {
			backoff = minBackoff
			continue
		}
		select {
		case <-s.stop:
			return
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// MirrorStatus is the root copy's state.
func (s *Store) MirrorStatus() Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Status{Mirrored: s.mirrored || s.mirror == nil, LastWrite: s.lastWrite, LastError: s.lastErr}
}

// Close stops the store's writer and flushes the root's copy: the newest
// root goes up if it has not, tried a few times. The two stores are the
// host's to close.
func (s *Store) Close() error {
	var err error
	s.close.Do(func() {
		close(s.stop)
		<-s.done
		if s.mirror == nil {
			return
		}
		for attempt := 0; attempt < waitAttempts; attempt++ {
			v := s.take()
			if v == nil {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
			err = s.copy(ctx, v)
			cancel()
			if err == nil {
				return
			}
			time.Sleep(minBackoff << attempt)
		}
	})
	return err
}

// Recover seeds a root store that has no root from the copy the objects
// store holds, after the disk holding the root is lost. A root store that
// holds a root is refused (ErrRootPresent): recover onto a fresh one.
func Recover(ctx context.Context, objects Mirrorer, roots blob.BlobStore) error {
	r, err := roots.Root(ctx)
	if err != nil {
		return err
	}
	if r.Version != blob.NoVersion {
		return ErrRootPresent
	}
	value, err := objects.ReadMirror(ctx)
	if err != nil {
		return err
	}
	if len(value) == 0 {
		return ErrNoMirror
	}
	_, err = roots.SwapRoot(ctx, blob.NoVersion, value)
	return err
}
