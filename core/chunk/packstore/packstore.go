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
	"os"
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
	PackSize   int              // 0: DefaultPackSize
	CacheBytes int              // 0: DefaultCacheBytes; negative: no cache
	Clock      func() time.Time // dates this store's uploads; nil: time.Now
	// The index of published chunks lives in memory up to IndexInMemory
	// chunks (0: DefaultIndexInMemory) and on disk past that, as a table in
	// IndexDir ("": the system's temporary directory), so a store's memory
	// does not grow with the repository (#6, DESIGN §6).
	IndexDir      string
	IndexInMemory int
	backoff       time.Duration
}

// DefaultIndexInMemory is the published chunks a store indexes in memory
// before spilling the index to disk: about 60 MiB of index.
const DefaultIndexInMemory = 1 << 19

// Store is a chunk.Store over a BlobStore.
type Store struct {
	o     Options
	codec *pack.Codec
	cache *cache

	commitMu sync.Mutex // serializes CompareAndSetRoot

	mu         sync.Mutex // guards everything below
	closed     bool
	mem        *dedup.Index      // chunks of this session's packs, and of published packs up to o.IndexInMemory
	disk       *spilled          // chunks of published packs past that, on disk; nil until needed
	published  int               // chunks of published packs in mem
	inflight   map[string][]byte // finished packs whose upload is not yet confirmed
	unuploaded []pack.Built      // finished packs whose upload failed; retried at CAS
	finishing  []*pack.Writer    // full packs a finisher is naming and uploading; their chunks read from here meanwhile
	finishers  sync.WaitGroup    // one per pack finishing
	finishErr  error             // a finisher's failure to build its pack, surfaced at the next publish
	slots      chan struct{}     // a token per pack that may be finishing or uploading at once (#10)
	holdFinish func()            // tests: called by a finisher before it names its pack
	pending    *pack.Writer
	session    []pack.Info  // uploaded packs not yet in a published index object
	sessionIdx [][32]byte   // index objects written but not yet in a published manifest
	man        manifest     // the newest manifest seen
	ver        blob.Version // its version
	loaded     map[[32]byte]bool
	keys       map[seal.Salt]*pack.Keys

	// GC (docs/DESIGN.md §9).
	condemned   map[string]bool       // packs the newest manifest condemns
	unpublished map[string]pack.Info  // packs built here that no published index object lists
	inIndex     map[[32]byte][]string // session index objects written, and the packs each lists
	deduped     map[hash.Hash]bool    // chunks puts found stored, since the last publish
	sessGen     uint64                // the gcGen those puts began under
	uploaded    map[string]time.Time  // this store's unpublished packs and index objects, dated by o.Clock
	lost        error                 // chunk.ErrSessionLost once GC deleted unpublished work: writes refuse
}

// maxInFlight bounds the packs finishing or uploading at once (#10): a
// writer with another full pack waits for one to land, so a slow backend
// costs time, never memory.
const maxInFlight = 2

// recheckAfter is how old an unpublished upload is before a publish looks it
// up; GC keeps its record of a deleted orphan this much past the grace
// window, so every deletion is caught by one or the other.
const recheckAfter = time.Hour

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
	if o.Clock == nil {
		o.Clock = time.Now
	}
	if o.IndexInMemory == 0 {
		o.IndexInMemory = DefaultIndexInMemory
	}
	if o.IndexInMemory < 0 {
		return nil, fmt.Errorf("packstore: index in memory %d is negative", o.IndexInMemory)
	}
	if o.IndexDir == "" {
		o.IndexDir = os.TempDir()
	}
	if st, err := os.Stat(o.IndexDir); err != nil || !st.IsDir() {
		return nil, fmt.Errorf("packstore: index directory %s: %w", o.IndexDir, errNotADir(err))
	}
	codec, err := pack.NewCodec()
	if err != nil {
		return nil, err
	}
	s := &Store{o: o, codec: codec, mem: dedup.New(), inflight: map[string][]byte{}, slots: make(chan struct{}, maxInFlight),
		loaded: map[[32]byte]bool{}, keys: map[seal.Salt]*pack.Keys{}, condemned: map[string]bool{},
		unpublished: map[string]pack.Info{}, inIndex: map[[32]byte][]string{}, deduped: map[hash.Hash]bool{},
		uploaded: map[string]time.Time{}}
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
	s.sessGen = s.man.gcGen
	return s, nil
}

// refresh reads the current manifest and loads any index objects it lists
// that this store has not loaded. When GC has expired packs since the
// manifest this store knew (gcGen moved), the index is rebuilt from the
// manifest's index objects and the packs built here and not yet published,
// so it names nothing in a deleted pack. Published chunks past
// o.IndexInMemory are indexed on disk (#6): the table is built when they
// first pass the bound, and again at every rebuild.
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
	rebuild := s.ver != blob.NoVersion && m.seq > s.man.seq && m.gcGen != s.man.gcGen
	var toLoad [][32]byte
	for _, sum := range m.indexes {
		if rebuild || !s.loaded[sum] {
			toLoad = append(toLoad, sum)
		}
	}
	published := s.published
	s.mu.Unlock()
	// This refresh spills once the chunks about to sit in memory pass the
	// bound: the objects loaded so far are dropped then, and the build
	// streams every object through, so no more than the bound's worth of
	// decoded index is ever in memory. A rebuild counts from nothing: a
	// repository GC shrank under the bound returns to memory.
	spill := false
	loaded := map[[32]byte][]pack.Info{}
	total := published
	if rebuild {
		total = 0
	}
	for _, sum := range toLoad {
		infos, err := s.loadIndex(ctx, sum)
		if err != nil {
			return err
		}
		total += entries(infos)
		if total > s.o.IndexInMemory {
			spill, loaded = true, nil
			break
		}
		loaded[sum] = infos
	}
	cond := condemnedPacks(m)
	var sp *spilled
	if spill {
		if sp, err = s.buildSpilled(ctx, m, loaded, cond); err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Manifests only move forward (every swap increments seq); a slower
	// refresh must not roll back what a faster one saw.
	newer := s.ver == blob.NoVersion || m.seq > s.man.seq
	switch {
	case sp != nil && !newer:
		_ = sp.close()
	case sp != nil:
		if s.disk != nil {
			_ = s.disk.close()
		}
		s.disk, s.published = sp, 0
		s.mem = dedup.New()
		s.loaded = map[[32]byte]bool{}
		for _, sum := range m.indexes {
			s.loaded[sum] = true
		}
		for _, info := range s.unpublished {
			s.mem.Add(info)
		}
	case rebuild && newer:
		if s.disk != nil { // the repository shrank under the bound
			_ = s.disk.close()
			s.disk = nil
		}
		x := dedup.New()
		s.loaded = map[[32]byte]bool{}
		var infos []pack.Info
		for _, sum := range m.indexes {
			infos = append(infos, loaded[sum]...)
			s.loaded[sum] = true
		}
		addPacks(x, infos, cond)
		for _, info := range s.unpublished {
			x.Add(info)
		}
		s.mem, s.published = x, entries(infos)
	case !rebuild:
		var infos []pack.Info
		for sum, packs := range loaded {
			if s.loaded[sum] {
				continue
			}
			infos = append(infos, packs...)
			s.loaded[sum] = true
		}
		addPacks(s.mem, infos, cond)
		s.published += entries(infos)
	}
	if newer {
		s.man, s.ver = m, r.Version
		s.condemned = cond
	}
	return nil
}

// addPacks adds packs to an index, those still in service first, so a chunk
// a repacked pack also holds resolves to its new pack (DESIGN §9).
func addPacks(x *dedup.Index, infos []pack.Info, cond map[string]bool) {
	for pass := range 2 {
		for _, info := range infos {
			if cond[info.Name] == (pass == 1) {
				x.Add(info)
			}
		}
	}
}

// condemnedPacks is the packs m condemns or has repacked: none is
// deduplicated against, since each is on its way out.
func condemnedPacks(m manifest) map[string]bool {
	out := map[string]bool{}
	for _, c := range m.condemned {
		if c.kind == condemnedPack || c.kind == repackedPack {
			out[dedup.PackName(c.sum)] = true
		}
	}
	return out
}

// errNotADir is the error for an index directory that is not one.
func errNotADir(err error) error {
	if err != nil {
		return err
	}
	return errors.New("not a directory")
}

// lookupLocked locates a chunk: in the index on disk, then in memory.
// Callers hold s.mu.
func (s *Store) lookupLocked(h hash.Hash) (dedup.Location, bool, error) {
	if s.disk != nil {
		if loc, ok, err := s.disk.lookup(h); ok || err != nil {
			return loc, ok, err
		}
	}
	loc, ok := s.mem.Lookup(h)
	return loc, ok, nil
}

// hasLocked reports whether a chunk is indexed. Callers hold s.mu.
func (s *Store) hasLocked(h hash.Hash) (bool, error) {
	if s.mem.Has(h) {
		return true, nil
	}
	if s.disk != nil {
		return s.disk.has(h)
	}
	return false, nil
}

func (s *Store) loadIndex(ctx context.Context, sum [32]byte) ([]pack.Info, error) {
	return loadIndex(ctx, s.o, sum)
}

// loadIndex reads and opens one index object the manifest lists.
func loadIndex(ctx context.Context, o Options, sum [32]byte) ([]pack.Info, error) {
	name := indexName(sum)
	rc, err := o.Blobs.Get(ctx, name, 0, -1)
	if err != nil {
		return nil, fmt.Errorf("packstore: index object %s listed by the manifest: %w", name, err)
	}
	defer rc.Close()
	b, err := io.ReadAll(io.LimitReader(rc, dedup.MaxObjectSize+1))
	if err != nil {
		return nil, err
	}
	infos, err := dedup.DecodeObject(o.Keys, o.Repo, name, b)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", chunk.ErrCorrupt, err)
	}
	return infos, nil
}

// Location reports where a published or flushed chunk lives, for tools and tests.
func (s *Store) Location(h hash.Hash) (packName string, off, n int64, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	loc, ok, err := s.lookupLocked(h)
	if err != nil || !ok {
		return "", 0, 0, false
	}
	return loc.Pack.Name, int64(loc.Entry.Offset), int64(loc.Entry.StoredLen), true
}

func (s *Store) newWriter() (*pack.Writer, error) {
	return pack.NewWriter(s.o.Keys, s.o.Repo, s.codec, s.o.PackSize)
}

// finishPendingLocked names and builds the pending pack on the caller's
// goroutine, makes its chunks locatable, and keeps its bytes readable
// until its upload is confirmed. Callers hold s.mu.
func (s *Store) finishPendingLocked() (*pack.Built, error) {
	if s.pending == nil || s.pending.Count() == 0 {
		return nil, nil
	}
	b, err := s.pending.Finish()
	s.pending = nil
	if err != nil {
		return nil, err
	}
	s.mem.Add(b.Info)
	s.inflight[b.Name] = b.Bytes
	s.unpublished[b.Name] = b.Info
	return &b, nil
}

// takePendingLocked hands the pending pack, if it holds anything, to a
// finisher: its chunks read and deduplicate from the unfinished writer
// until the finisher has named it. Callers hold s.mu and then call
// startFinisher with what it returns, outside the lock.
func (s *Store) takePendingLocked() *pack.Writer {
	w := s.pending
	s.pending = nil
	if w == nil || w.Count() == 0 {
		return nil
	}
	s.finishing = append(s.finishing, w)
	s.finishers.Add(1)
	return w
}

// startFinisher takes a slot, waiting while maxInFlight packs are still
// finishing or uploading, then finishes and uploads w on its own goroutine
// (#10): the writer's goroutine is not the one hashing a pack's name,
// building it and writing it to the backend. The upload outlives the
// caller's cancellation: a pack half uploaded would be a chunk the next
// root reaches that nothing durable holds.
func (s *Store) startFinisher(ctx context.Context, w *pack.Writer) {
	if w == nil {
		return
	}
	s.slots <- struct{}{}
	go func() {
		defer s.finishers.Done()
		defer func() { <-s.slots }()
		s.finish(context.WithoutCancel(ctx), w)
	}()
}

// finish names and builds a full pack, makes its chunks locatable, keeps
// its bytes readable until its upload is confirmed, and uploads it.
func (s *Store) finish(ctx context.Context, w *pack.Writer) {
	if s.holdFinish != nil {
		s.holdFinish()
	}
	b, err := w.Finish()
	s.mu.Lock()
	for i, f := range s.finishing {
		if f == w {
			s.finishing = append(s.finishing[:i], s.finishing[i+1:]...)
			break
		}
	}
	if err != nil {
		if s.finishErr == nil {
			s.finishErr = err
		}
		s.mu.Unlock()
		return
	}
	s.mem.Add(b.Info)
	s.inflight[b.Name] = b.Bytes
	s.unpublished[b.Name] = b.Info
	s.mu.Unlock()
	// A failed upload is kept and retried at CAS; the chunk stays readable.
	_ = s.upload(ctx, []pack.Built{b})
}

// finishingGetLocked reads h from a pack being finished, if one holds it.
// Callers hold s.mu.
func (s *Store) finishingGetLocked(h hash.Hash) ([]byte, bool, error) {
	for _, w := range s.finishing {
		if data, ok, err := w.Get(h); ok {
			return data, true, err
		}
	}
	return nil, false, nil
}

func (s *Store) finishingHasLocked(h hash.Hash) bool {
	for _, w := range s.finishing {
		if w.Has(h) {
			return true
		}
	}
	return false
}

// upload stores packs; an object that already exists under a pack's name is
// the same bytes (packs are named by their hash). Every pack is attempted and
// every failure kept for the next CAS: a pack dropped here would be a chunk
// the next root reaches that nothing durable holds.
func (s *Store) upload(ctx context.Context, packs []pack.Built) error {
	return s.uploadPacks(ctx, packs, true)
}

// uploadPacks is upload; with toSession false the packs are not queued
// for the next publish's index objects, the publish under way writing
// them itself.
func (s *Store) uploadPacks(ctx context.Context, packs []pack.Built, toSession bool) error {
	var first error
	for _, b := range packs {
		err := s.o.Blobs.Put(ctx, b.Name, bytes.NewReader(b.Bytes), int64(len(b.Bytes)))
		if errors.Is(err, blob.ErrExists) {
			err = nil
		}
		s.mu.Lock()
		if err == nil {
			delete(s.inflight, b.Name)
			if toSession {
				s.session = append(s.session, b.Info)
			}
			s.uploaded[b.Name] = s.o.Clock()
		} else {
			s.unuploaded = append(s.unuploaded, b)
		}
		s.mu.Unlock()
		if err != nil && first == nil {
			first = fmt.Errorf("packstore: uploading %s: %w", b.Name, err)
		}
	}
	return first
}

// prepared is a chunk hashed, compressed and sealed for the pack that was
// pending when it was prepared, not yet stored.
type prepared struct {
	h       hash.Hash
	n       int    // the chunk's length
	payload []byte // compressed, or the chunk itself: sealed again if the pack moved on
	codec   uint8
	sealed  []byte    // the frame, under the keys of the pack salt names
	salt    seal.Salt // that pack
}

func (p *prepared) Hash() hash.Hash { return p.h }
func (p *prepared) Len() int        { return p.n }

var (
	_ chunk.Preparer = (*Store)(nil)
	_ chunk.Flusher  = (*Store)(nil)
)

// flushShare is the share of a pack the pending pack must hold for Flush
// to upload it: a pack put costs one fsync latency (one round trip, on
// S3) whatever its size, so flushing a small pack saves nothing at the
// publish and costs a pack, a fsync and an index entry.
const flushShare = 8

// Flush implements chunk.Flusher: the pending pack, if it holds at least
// a flushShare'th of a pack, goes to a finisher now, so its upload runs
// beside whatever the caller does next instead of inside the next
// publish; a smaller one waits for the publish and shares its pack with
// what comes next.
func (s *Store) Flush(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return chunk.ErrClosed
	}
	if s.pending == nil || s.pending.Size() < s.o.PackSize/flushShare {
		s.mu.Unlock()
		return nil
	}
	w := s.takePendingLocked()
	s.mu.Unlock()
	s.startFinisher(ctx, w)
	return nil
}

// Prepare implements chunk.Preparer: the hash, the compression and the
// seal, on the caller's goroutine, the lock taken only to learn which pack
// is pending (#10). The seal is under that pack's keys; if the pack has
// moved on by the time the chunk is stored, PutPrepared seals it again.
func (s *Store) Prepare(data []byte) (chunk.Prepared, error) {
	if len(data) > chunk.MaxChunkSize {
		return nil, fmt.Errorf("%w: %d bytes", chunk.ErrTooLarge, len(data))
	}
	payload, codec := s.codec.Compress(data)
	p := &prepared{h: hash.Sum(data), n: len(data), payload: payload, codec: codec}
	s.mu.Lock()
	if s.pending == nil && !s.closed {
		w, err := s.newWriter()
		if err != nil {
			s.mu.Unlock()
			return nil, err
		}
		s.pending = w
	}
	w := s.pending
	s.mu.Unlock()
	if w != nil {
		sealed, err := w.Seal(p.h, payload)
		if err != nil {
			return nil, err
		}
		p.sealed, p.salt = sealed, w.Salt()
	}
	return p, nil
}

// addPreparedLocked appends p to the pending pack: the frame as sealed if
// the pack is the one it was sealed for, sealed again otherwise. Callers
// hold s.mu.
func (s *Store) addPreparedLocked(p *prepared) error {
	if p.sealed != nil && s.pending.Salt() == p.salt {
		return s.pending.AddSealed(p.h, p.n, p.sealed, p.codec)
	}
	return s.pending.AddCompressed(p.h, p.n, p.payload, p.codec)
}

// Put implements chunk.Store.
func (s *Store) Put(ctx context.Context, data []byte) (hash.Hash, error) {
	p, err := s.Prepare(data)
	if err != nil {
		return hash.Hash{}, err
	}
	return s.PutPrepared(ctx, p)
}

// PutPrepared implements chunk.Preparer: the deduplication check, the seal
// into the pending pack and its upload when full, under the store's lock.
func (s *Store) PutPrepared(ctx context.Context, cp chunk.Prepared) (hash.Hash, error) {
	p, ok := cp.(*prepared)
	if !ok {
		return hash.Hash{}, errors.New("packstore: a chunk prepared by another store")
	}
	h := p.h
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return hash.Hash{}, chunk.ErrClosed
	}
	if s.lost != nil {
		s.mu.Unlock()
		return hash.Hash{}, s.lost
	}
	if (s.pending != nil && s.pending.Has(h)) || s.finishingHasLocked(h) {
		s.mu.Unlock()
		return h, nil
	}
	loc, ok, err := s.lookupLocked(h)
	if err != nil {
		s.mu.Unlock()
		return hash.Hash{}, err
	}
	if ok && !s.condemned[loc.Pack.Name] {
		// Counted on: the publish checks it survived any collection.
		s.deduped[h] = true
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
	err = s.addPreparedLocked(p)
	var full *pack.Writer
	if errors.Is(err, pack.ErrFull) {
		full = s.takePendingLocked()
		if s.pending, err = s.newWriter(); err == nil {
			err = s.addPreparedLocked(p) // a fresh pack: sealed again under its keys
		}
	}
	s.mu.Unlock()
	s.startFinisher(ctx, full)
	if err != nil {
		return hash.Hash{}, err
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
	if data, ok, err := s.finishingGetLocked(h); ok {
		s.mu.Unlock()
		if err != nil {
			return nil, fmt.Errorf("%w: %w", chunk.ErrCorrupt, err)
		}
		return data, nil
	}
	s.mu.Unlock()
	if data, ok := s.cache.get(h); ok {
		return verified(h, bytes.Clone(data))
	}
	loc, frame, keys, err := s.frame(ctx, h)
	if errors.Is(err, blob.ErrNotFound) {
		// GC may have expired the pack: a refresh rebuilds the index.
		if err := s.refresh(ctx); err != nil {
			return nil, err
		}
		loc, frame, keys, err = s.frame(ctx, h)
		if errors.Is(err, blob.ErrNotFound) {
			if lost := s.promised(h, loc.Pack.Name); lost != nil {
				return nil, lost
			}
			return nil, fmt.Errorf("%w: %s is in pack %s, which is missing", chunk.ErrCorrupt, h.Short(), loc.Pack.Name)
		}
	}
	if errors.Is(err, chunk.ErrNotFound) {
		if lost := s.promised(h, ""); lost != nil {
			return nil, lost
		}
	}
	if err != nil {
		return nil, err
	}
	data, err := pack.OpenFrame(keys, s.codec, loc.Entry, frame)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", chunk.ErrCorrupt, err)
	}
	s.cache.put(h, bytes.Clone(data))
	return data, nil
}

// promised ends the session when a chunk that cannot be read is one this
// store told a put was stored: one it counted on, or one in a pack of its
// own it has not published (packName, when the pack is known). Otherwise
// it is nil.
func (s *Store) promised(h hash.Hash, packName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, own := s.unpublished[packName]; own || s.deduped[h] {
		return s.lostLocked(h.Short() + " was promised to a put and is gone")
	}
	return nil
}

// frame locates a chunk and reads its sealed frame, from memory or the
// backend; a pack the backend does not have is blob.ErrNotFound.
func (s *Store) frame(ctx context.Context, h hash.Hash) (dedup.Location, []byte, *pack.Keys, error) {
	loc, local, keys, err := s.locate(ctx, h)
	if err != nil {
		return dedup.Location{}, nil, nil, err
	}
	if local != nil {
		return loc, local[loc.Entry.Offset : loc.Entry.Offset+loc.Entry.StoredLen], keys, nil
	}
	rc, err := s.o.Blobs.Get(ctx, loc.Pack.Name, int64(loc.Entry.Offset), int64(loc.Entry.StoredLen))
	if err != nil {
		return loc, nil, nil, err
	}
	frame, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		return loc, nil, nil, err
	}
	return loc, frame, keys, nil
}

// locate finds a chunk, refreshing the manifest once if it is unknown.
func (s *Store) locate(ctx context.Context, h hash.Hash) (dedup.Location, []byte, *pack.Keys, error) {
	for attempt := 0; attempt < 2; attempt++ {
		s.mu.Lock()
		loc, ok, err := s.lookupLocked(h)
		if err != nil {
			s.mu.Unlock()
			return dedup.Location{}, nil, nil, err
		}
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
	if s.isClosed() {
		return nil, chunk.ErrClosed
	}
	out := make(map[hash.Hash]bool, len(hs))
	check := func() (missing bool, err error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, h := range hs {
			ok := s.pending != nil && s.pending.Has(h)
			if !ok {
				if ok, err = s.hasLocked(h); err != nil {
					return false, err
				}
			}
			out[h] = ok
			missing = missing || !ok
		}
		return missing, nil
	}
	missing, err := check()
	if err != nil {
		return nil, err
	}
	if missing {
		if err := s.refresh(ctx); err != nil {
			return nil, err
		}
		if _, err := check(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// Root implements chunk.Store.
func (s *Store) Root(ctx context.Context) (hash.Hash, error) {
	if s.isClosed() {
		return hash.Hash{}, chunk.ErrClosed
	}
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
	if s.lost != nil {
		s.mu.Unlock()
		return s.lost
	}
	stored := !next.IsZero() && ((s.pending != nil && s.pending.Has(next)) || s.finishingHasLocked(next))
	if !next.IsZero() && !stored {
		var err error
		if stored, err = s.hasLocked(next); err != nil {
			s.mu.Unlock()
			return err
		}
	}
	if !stored {
		s.mu.Unlock()
		return fmt.Errorf("%w: %s", chunk.ErrRootMissing, next.Short())
	}
	s.mu.Unlock()
	// Every finisher has landed its pack or left it to retry here.
	s.finishers.Wait()
	s.mu.Lock()
	if s.finishErr != nil {
		err := s.finishErr
		s.mu.Unlock()
		return err
	}
	// The pending pack is finished and uploaded here, on this goroutine: a
	// failure to store it is this publish's error, not a retry's.
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

	// Every chunk the new root can reach is durable before the manifest
	// names it. The packs and the index objects listing them are written
	// together, each waiting on the backend and neither on the other (#10);
	// a pack that fails leaves its index objects orphans, which GC deletes.
	uploaded := make(chan error, 1)
	go func() { uploaded <- s.uploadPacks(ctx, toUpload, false) }()
	written, err := s.writeSessionIndex(ctx, toUpload)
	if uerr := <-uploaded; uerr != nil {
		return uerr
	}
	if err != nil {
		return err
	}
	s.recordSessionIndex(written)
	if err := s.stillStored(ctx); err != nil {
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
		if err := s.survived(m); err != nil {
			return err
		}
		if err := s.unrecorded(m, pending); err != nil {
			return err
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
			for _, sum := range pending {
				for _, name := range s.inIndex[sum] {
					delete(s.unpublished, name)
					delete(s.uploaded, name)
				}
				delete(s.inIndex, sum)
				delete(s.uploaded, indexName(sum))
			}
			s.sessionIdx = s.sessionIdx[len(pending):]
			s.deduped, s.sessGen = map[hash.Hash]bool{}, upd.gcGen
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
	if s.closed {
		return chunk.Stats{}, chunk.ErrClosed
	}
	n := int64(s.mem.Len())
	if s.disk != nil {
		n += s.disk.len() // a chunk in both is counted twice: about, not exactly
	}
	if s.pending != nil {
		n += int64(s.pending.Count())
	}
	for _, w := range s.finishing {
		n += int64(w.Count())
	}
	return chunk.Stats{Chunks: n}, nil
}

func (s *Store) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// Close implements chunk.Store. Chunks never published are dropped; a
// pack still uploading is waited for, so nothing writes after Close.
func (s *Store) Close() error {
	s.finishers.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	var err error
	if !s.closed {
		s.closed = true
		s.codec.Close()
		if s.disk != nil {
			err = s.disk.close()
			s.disk = nil
		}
	}
	return err
}

// writeSessionIndex writes the index objects for the session's uploaded
// packs and the packs being uploaded by this publish, in objects of at most
// maxIndexObject each (an object is decoded whole by every store that opens
// it), and returns them for recordSessionIndex once every pack has landed.
func (s *Store) writeSessionIndex(ctx context.Context, uploading []pack.Built) (*indexWriter, error) {
	s.mu.Lock()
	session := append([]pack.Info(nil), s.session...)
	s.mu.Unlock()
	w := &indexWriter{ctx: ctx, o: s.o, session: len(session)}
	for _, p := range session {
		if err := w.add(p); err != nil {
			return nil, err
		}
	}
	for _, b := range uploading {
		if err := w.add(b.Info); err != nil {
			return nil, err
		}
	}
	if len(w.batch) == 0 && len(w.written) == 0 {
		return w, nil
	}
	if _, err := w.finish(); err != nil {
		return nil, err
	}
	return w, nil
}

// recordSessionIndex takes the index objects a publish wrote as this
// session's, pending in the manifest.
func (s *Store) recordSessionIndex(w *indexWriter) {
	s.mu.Lock()
	s.session = s.session[w.session:]
	for _, obj := range w.written {
		s.sessionIdx = append(s.sessionIdx, obj.sum)
		s.loaded[obj.sum] = true
		s.inIndex[obj.sum] = obj.packs
		s.uploaded[indexName(obj.sum)] = s.o.Clock()
	}
	s.mu.Unlock()
}

// lostLocked ends the store's session: GC deleted what it had written and
// not published, or a chunk it counted on, and which roots in flight reach
// that the store cannot know, so every later write is refused (DESIGN §9).
// Callers hold s.mu.
func (s *Store) lostLocked(what string) error {
	s.lost = fmt.Errorf("%w: %s", chunk.ErrSessionLost, what)
	return s.lost
}

// unpublishedNames lists the index objects written and not yet in a
// published manifest, and the packs each lists. Callers hold s.mu.
func (s *Store) unpublishedNames() []string {
	var names []string
	for _, sum := range s.sessionIdx {
		names = append(append(names, indexName(sum)), s.inIndex[sum]...)
	}
	return names
}

// stillStored looks up each unpublished upload over recheckAfter old by this
// store's clock, and ends the session if one is gone: GC deleted it as an
// orphan longer ago than its record lasts. Fresh uploads are not looked up.
func (s *Store) stillStored(ctx context.Context) error {
	s.mu.Lock()
	now := s.o.Clock()
	var old []string
	for _, name := range s.unpublishedNames() {
		if at, ok := s.uploaded[name]; ok && now.Sub(at) > recheckAfter {
			old = append(old, name)
		}
	}
	s.mu.Unlock()
	for _, name := range old {
		_, err := s.o.Blobs.Stat(ctx, name)
		if errors.Is(err, blob.ErrNotFound) {
			s.mu.Lock()
			defer s.mu.Unlock()
			return s.lostLocked(name + " is gone")
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// unrecorded ends the session when m, the manifest about to be replaced,
// records as a deleted orphan an upload this publish would name. GC swaps
// its record in before it deletes, so a publish racing the deletion swaps
// against the record, or loses to it and reads it on the next attempt.
func (s *Store) unrecorded(m manifest, pending [][32]byte) error {
	deleted := map[string]bool{}
	for _, c := range m.condemned {
		switch c.kind {
		case deletedPack:
			deleted[dedup.PackName(c.sum)] = true
		case deletedIndex:
			deleted[indexName(c.sum)] = true
		}
	}
	if len(deleted) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sum := range pending {
		for _, name := range append([]string{indexName(sum)}, s.inIndex[sum]...) {
			if deleted[name] {
				return s.lostLocked(name + " was deleted as an orphan")
			}
		}
	}
	return nil
}

// survived ends the session when GC expired packs since this store's writes
// began (m, the manifest about to be replaced, is newer in gcGen) and a
// chunk a put counted on is no longer stored: publishing would name a chunk
// that is gone. The index was rebuilt when the store saw m.
func (s *Store) survived(m manifest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if m.gcGen == s.sessGen {
		return nil
	}
	for h := range s.deduped {
		if (s.pending != nil && s.pending.Has(h)) || s.finishingHasLocked(h) {
			continue
		}
		ok, err := s.hasLocked(h)
		if err != nil {
			return err
		}
		if !ok {
			return s.lostLocked("counted on " + h.Short() + ", which expired")
		}
	}
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
	d = d/2 + time.Duration(rand.Int64N(int64(d)+1)) //nolint:gosec // G404: backoff jitter, not a secret
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}
