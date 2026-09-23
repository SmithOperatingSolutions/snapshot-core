// Package packstore is the chunk store over any BlobStore: chunks are packed
// (core/pack), located through sealed index objects (core/dedup), and
// published by swapping a sealed manifest that names the root chunk and the
// live index objects (docs/DESIGN.md §4-5).
//
// Writing: Put adds to an in-memory pack; a full pack is finished and
// uploaded, and stays readable from memory until the upload is confirmed.
// CompareAndSetRoot uploads what remains, writes one index object for the
// session's packs, and swaps the manifest. Every chunk the new root reaches
// is therefore durable before the manifest names it. A swap that loses to a
// manifest-only change (another writer's index objects, GC) is retried with
// jittered backoff, up to MaxSwapAttempts; a root that moved is the caller's
// conflict, returned at once.
//
// Reading: a chunk this store has not seen triggers one manifest refresh, so
// another writer's published chunks become visible without reopening. Every
// read is verified by SHA-256, from memory, cache or backend.
package packstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/dedup"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/pack"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
)

// ErrManifest is returned when the root object does not open: the wrong key
// or repository, or an altered manifest.
var ErrManifest = errors.New("packstore: manifest does not authenticate or decode")

// Defaults.
const (
	DefaultPackSize   = 32 << 20
	DefaultCacheBytes = 64 << 20
	// MaxSwapAttempts is the Engine Spec's bound on manifest retries.
	MaxSwapAttempts = 10
	defaultBackoff  = 20 * time.Millisecond
)

// Options configures a store.
type Options struct {
	Blobs      blob.BlobStore
	Keys       *seal.Keyring
	Repo       seal.RepoID
	PackSize   int // 0: DefaultPackSize
	CacheBytes int // 0: DefaultCacheBytes; negative: no cache
	backoff    time.Duration
}

// Store is a chunk.Store over a BlobStore.
type Store struct {
	o     Options
	codec *pack.Codec
	cache *cache

	commitMu sync.Mutex // serializes CompareAndSetRoot

	mu         sync.Mutex // guards everything below
	closed     bool
	index      *dedup.Index      // chunks of published packs and of this session's packs
	inflight   map[string][]byte // finished packs whose upload is not yet confirmed
	unuploaded []pack.Built      // finished packs whose upload failed; retried at CAS
	pending    *pack.Writer
	session    []pack.Info  // uploaded packs not yet in a published index object
	sessionIdx [][32]byte   // index objects written but not yet in a published manifest
	man        manifest     // the newest manifest seen
	ver        blob.Version // its version
	loaded     map[[32]byte]bool
	keys       map[seal.Salt]*pack.Keys
}

var _ chunk.Store = (*Store)(nil)

// Open loads the manifest and its index objects.
func Open(ctx context.Context, o Options) (*Store, error) {
	if o.Blobs == nil || o.Keys == nil {
		return nil, errors.New("packstore: Blobs and Keys are required")
	}
	if o.PackSize == 0 {
		o.PackSize = DefaultPackSize
	}
	if o.PackSize < pack.HeaderSize+pack.TrailerSize || o.PackSize > pack.MaxPackSize {
		return nil, fmt.Errorf("packstore: pack size %d outside %d..%d", o.PackSize, pack.HeaderSize+pack.TrailerSize, pack.MaxPackSize)
	}
	if o.backoff == 0 {
		o.backoff = defaultBackoff
	}
	codec, err := pack.NewCodec()
	if err != nil {
		return nil, err
	}
	s := &Store{o: o, codec: codec, index: dedup.New(), inflight: map[string][]byte{},
		loaded: map[[32]byte]bool{}, keys: map[seal.Salt]*pack.Keys{}}
	switch {
	case o.CacheBytes == 0:
		s.cache = newCache(DefaultCacheBytes)
	case o.CacheBytes > 0:
		s.cache = newCache(o.CacheBytes)
	}
	if err := s.refresh(ctx); err != nil {
		codec.Close()
		return nil, err
	}
	return s, nil
}

// refresh reads the current manifest and loads any index objects it lists
// that this store has not loaded.
func (s *Store) refresh(ctx context.Context) error {
	r, err := s.o.Blobs.Root(ctx)
	if err != nil {
		return err
	}
	var m manifest
	if r.Version != blob.NoVersion {
		if m, err = openManifest(r.Value, s.o.Keys, s.o.Repo); err != nil {
			return err
		}
	}
	s.mu.Lock()
	var toLoad [][32]byte
	for _, sum := range m.indexes {
		if !s.loaded[sum] {
			toLoad = append(toLoad, sum)
		}
	}
	s.mu.Unlock()
	loaded := make(map[[32]byte][]pack.Info, len(toLoad))
	for _, sum := range toLoad {
		infos, err := s.loadIndex(ctx, sum)
		if err != nil {
			return err
		}
		loaded[sum] = infos
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for sum, infos := range loaded {
		if s.loaded[sum] {
			continue
		}
		for _, info := range infos {
			s.index.Add(info)
		}
		s.loaded[sum] = true
	}
	// Manifests only move forward (every swap increments seq); a slower
	// refresh must not roll back what a faster one saw.
	if s.ver == blob.NoVersion || m.seq > s.man.seq {
		s.man, s.ver = m, r.Version
	}
	return nil
}

func (s *Store) loadIndex(ctx context.Context, sum [32]byte) ([]pack.Info, error) {
	name := indexName(sum)
	rc, err := s.o.Blobs.Get(ctx, name, 0, -1)
	if err != nil {
		return nil, fmt.Errorf("packstore: index object %s listed by the manifest: %w", name, err)
	}
	defer rc.Close()
	b, err := io.ReadAll(io.LimitReader(rc, dedup.MaxObjectSize+1))
	if err != nil {
		return nil, err
	}
	infos, err := dedup.DecodeObject(s.o.Keys, s.o.Repo, name, b)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", chunk.ErrCorrupt, err)
	}
	return infos, nil
}

// Location reports where a published or flushed chunk lives, for tools and tests.
func (s *Store) Location(h hash.Hash) (packName string, off, n int64, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	loc, ok := s.index.Lookup(h)
	if !ok {
		return "", 0, 0, false
	}
	return loc.Pack.Name, int64(loc.Entry.Offset), int64(loc.Entry.StoredLen), true
}

func (s *Store) newWriter() (*pack.Writer, error) {
	return pack.NewWriter(s.o.Keys, s.o.Repo, s.codec, s.o.PackSize)
}

// finishPendingLocked seals the pending pack, makes its chunks locatable, and
// keeps its bytes readable until its upload is confirmed. Callers hold s.mu.
func (s *Store) finishPendingLocked() (*pack.Built, error) {
	if s.pending == nil || s.pending.Count() == 0 {
		return nil, nil
	}
	b, err := s.pending.Finish()
	s.pending = nil
	if err != nil {
		return nil, err
	}
	s.index.Add(b.Info)
	s.inflight[b.Name] = b.Bytes
	return &b, nil
}

// upload stores packs; an object that already exists under a pack's name is
// the same bytes (packs are named by their hash).
func (s *Store) upload(ctx context.Context, packs []pack.Built) error {
	for _, b := range packs {
		err := s.o.Blobs.Put(ctx, b.Name, bytes.NewReader(b.Bytes), int64(len(b.Bytes)))
		s.mu.Lock()
		switch {
		case err == nil || errors.Is(err, blob.ErrExists):
			delete(s.inflight, b.Name)
			s.session = append(s.session, b.Info)
		default:
			s.unuploaded = append(s.unuploaded, b)
		}
		s.mu.Unlock()
		if err != nil && !errors.Is(err, blob.ErrExists) {
			return fmt.Errorf("packstore: uploading %s: %w", b.Name, err)
		}
	}
	return nil
}

// Put implements chunk.Store.
func (s *Store) Put(ctx context.Context, data []byte) (hash.Hash, error) {
	if len(data) > chunk.MaxChunkSize {
		return hash.Hash{}, fmt.Errorf("%w: %d bytes", chunk.ErrTooLarge, len(data))
	}
	h := hash.Sum(data)
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return hash.Hash{}, chunk.ErrClosed
	}
	if (s.pending != nil && s.pending.Has(h)) || s.index.Has(h) {
		s.mu.Unlock()
		return h, nil
	}
	if s.pending == nil {
		w, err := s.newWriter()
		if err != nil {
			s.mu.Unlock()
			return hash.Hash{}, err
		}
		s.pending = w
	}
	err := s.pending.Add(h, data)
	var full *pack.Built
	if errors.Is(err, pack.ErrFull) {
		if full, err = s.finishPendingLocked(); err == nil {
			if s.pending, err = s.newWriter(); err == nil {
				err = s.pending.Add(h, data)
			}
		}
	}
	s.mu.Unlock()
	if err != nil {
		return hash.Hash{}, err
	}
	if full != nil {
		// A failed upload is kept and retried at CAS; the chunk stays readable.
		_ = s.upload(ctx, []pack.Built{*full})
	}
	return h, nil
}

func (s *Store) keysLocked(salt seal.Salt) (*pack.Keys, error) {
	if k, ok := s.keys[salt]; ok {
		return k, nil
	}
	k, err := pack.DeriveKeys(s.o.Keys, s.o.Repo, salt)
	if err != nil {
		return nil, err
	}
	s.keys[salt] = k
	return k, nil
}

func verified(h hash.Hash, data []byte) ([]byte, error) {
	if hash.Sum(data) != h {
		return nil, fmt.Errorf("%w: %s", chunk.ErrCorrupt, h.Short())
	}
	return data, nil
}

// Get implements chunk.Store.
func (s *Store) Get(ctx context.Context, h hash.Hash) ([]byte, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, chunk.ErrClosed
	}
	if s.pending != nil {
		if data, ok, err := s.pending.Get(h); ok {
			s.mu.Unlock()
			if err != nil {
				return nil, fmt.Errorf("%w: %w", chunk.ErrCorrupt, err)
			}
			return data, nil
		}
	}
	s.mu.Unlock()
	if data, ok := s.cache.get(h); ok {
		return verified(h, bytes.Clone(data))
	}
	loc, local, keys, err := s.locate(ctx, h)
	if err != nil {
		return nil, err
	}
	var frame []byte
	if local != nil {
		frame = local[loc.Entry.Offset : loc.Entry.Offset+loc.Entry.StoredLen]
	} else {
		rc, err := s.o.Blobs.Get(ctx, loc.Pack.Name, int64(loc.Entry.Offset), int64(loc.Entry.StoredLen))
		if errors.Is(err, blob.ErrNotFound) {
			return nil, fmt.Errorf("%w: %s is in pack %s, which is missing", chunk.ErrCorrupt, h.Short(), loc.Pack.Name)
		}
		if err != nil {
			return nil, err
		}
		frame, err = io.ReadAll(rc)
		rc.Close()
		if err != nil {
			return nil, err
		}
	}
	data, err := pack.OpenFrame(keys, s.codec, loc.Entry, frame)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", chunk.ErrCorrupt, err)
	}
	s.cache.put(h, bytes.Clone(data))
	return data, nil
}

// locate finds a chunk, refreshing the manifest once if it is unknown.
func (s *Store) locate(ctx context.Context, h hash.Hash) (dedup.Location, []byte, *pack.Keys, error) {
	for attempt := 0; attempt < 2; attempt++ {
		s.mu.Lock()
		loc, ok := s.index.Lookup(h)
		if ok {
			keys, err := s.keysLocked(loc.Pack.Salt)
			local := s.inflight[loc.Pack.Name]
			s.mu.Unlock()
			return loc, local, keys, err
		}
		s.mu.Unlock()
		if attempt == 0 {
			if err := s.refresh(ctx); err != nil {
				return dedup.Location{}, nil, nil, err
			}
		}
	}
	return dedup.Location{}, nil, nil, chunk.ErrNotFound
}

// Has implements chunk.Store.
func (s *Store) Has(ctx context.Context, hs []hash.Hash) (map[hash.Hash]bool, error) {
	out := make(map[hash.Hash]bool, len(hs))
	check := func() (missing bool) {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, h := range hs {
			ok := s.index.Has(h) || (s.pending != nil && s.pending.Has(h))
			out[h] = ok
			missing = missing || !ok
		}
		return missing
	}
	if check() {
		if err := s.refresh(ctx); err != nil {
			return nil, err
		}
		check()
	}
	return out, nil
}

// Root implements chunk.Store.
func (s *Store) Root(ctx context.Context) (hash.Hash, error) {
	if err := s.refresh(ctx); err != nil {
		return hash.Hash{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.man.root, nil
}

// CompareAndSetRoot implements chunk.Store.
func (s *Store) CompareAndSetRoot(ctx context.Context, expected, next hash.Hash) error {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return chunk.ErrClosed
	}
	if next.IsZero() || !(s.index.Has(next) || (s.pending != nil && s.pending.Has(next))) {
		s.mu.Unlock()
		return fmt.Errorf("%w: %s", chunk.ErrRootMissing, next.Short())
	}
	built, err := s.finishPendingLocked()
	if err != nil {
		s.mu.Unlock()
		return err
	}
	toUpload := s.unuploaded
	s.unuploaded = nil
	if built != nil {
		toUpload = append(toUpload, *built)
	}
	s.mu.Unlock()

	// Every chunk the new root can reach is durable before the manifest names it.
	if err := s.upload(ctx, toUpload); err != nil {
		return err
	}
	if err := s.writeSessionIndex(ctx); err != nil {
		return err
	}

	for attempt := 0; attempt < MaxSwapAttempts; attempt++ {
		if attempt > 0 {
			if err := s.sleep(ctx, attempt-1); err != nil {
				return err
			}
			if err := s.refresh(ctx); err != nil {
				return err
			}
		}
		s.mu.Lock()
		m, v := s.man, s.ver
		pending := append([][32]byte(nil), s.sessionIdx...)
		s.mu.Unlock()
		if m.root != expected && attempt == 0 {
			// Our view may be stale; only a fresh read can say the root moved.
			if err := s.refresh(ctx); err != nil {
				return err
			}
			s.mu.Lock()
			m, v = s.man, s.ver
			s.mu.Unlock()
		}
		if m.root != expected {
			return chunk.ErrRootConflict // the root moved: the caller's conflict
		}
		upd := m
		upd.seq = m.seq + 1
		upd.root = next
		upd.indexes = mergeIndexes(m.indexes, pending)
		if len(upd.indexes) > maxIndexes {
			return fmt.Errorf("packstore: the manifest lists %d index objects, over %d: run GC to compact", len(upd.indexes), maxIndexes)
		}
		sealed, err := upd.seal(s.o.Keys, s.o.Repo)
		if err != nil {
			return err
		}
		nv, err := s.o.Blobs.SwapRoot(ctx, v, sealed)
		if err == nil {
			s.mu.Lock()
			s.man, s.ver = upd, nv
			s.sessionIdx = s.sessionIdx[len(pending):]
			s.mu.Unlock()
			return nil
		}
		if !errors.Is(err, blob.ErrRootConflict) {
			return err
		}
		// Someone else changed the manifest: refresh and re-apply.
	}
	return chunk.ErrRootConflict
}

// Stats implements chunk.Store.
func (s *Store) Stats(ctx context.Context) (chunk.Stats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := int64(s.index.Len())
	if s.pending != nil {
		n += int64(s.pending.Count())
	}
	return chunk.Stats{Chunks: n}, nil
}

// Close implements chunk.Store. Chunks never published are dropped.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		s.codec.Close()
	}
	return nil
}

func (s *Store) writeSessionIndex(ctx context.Context) error {
	s.mu.Lock()
	session := append([]pack.Info(nil), s.session...)
	s.mu.Unlock()
	if len(session) == 0 {
		return nil
	}
	name, blobBytes, err := dedup.EncodeObject(s.o.Keys, s.o.Repo, session)
	if err != nil {
		return err
	}
	if err := s.o.Blobs.Put(ctx, name, bytes.NewReader(blobBytes), int64(len(blobBytes))); err != nil && !errors.Is(err, blob.ErrExists) {
		return fmt.Errorf("packstore: writing index object: %w", err)
	}
	sum, err := indexSum(name)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.session = s.session[len(session):]
	s.sessionIdx = append(s.sessionIdx, sum)
	s.loaded[sum] = true
	s.mu.Unlock()
	return nil
}

func mergeIndexes(have, add [][32]byte) [][32]byte {
	seen := make(map[[32]byte]bool, len(have)+len(add))
	out := make([][32]byte, 0, len(have)+len(add))
	for _, l := range [][][32]byte{have, add} {
		for _, s := range l {
			if !seen[s] {
				seen[s] = true
				out = append(out, s)
			}
		}
	}
	return out
}

func (s *Store) sleep(ctx context.Context, attempt int) error {
	d := s.o.backoff << min(attempt, 5)
	d = d/2 + time.Duration(rand.Int64N(int64(d)+1))
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}
