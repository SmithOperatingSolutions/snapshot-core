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

	// GC (docs/DESIGN.md §9).
	condemned   map[string]bool       // packs the newest manifest condemns
	unpublished map[string]pack.Info  // packs built here that no published index object lists
	inIndex     map[[32]byte][]string // session index objects written, and the packs each lists
	deduped     map[hash.Hash]bool    // chunks puts found stored, since the last publish
	sessGen     uint64                // the gcGen those puts began under
	uploaded    map[string]time.Time  // this store's unpublished packs and index objects, dated by o.Clock
	lost        error                 // chunk.ErrSessionLost once GC deleted unpublished work: writes refuse
}

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
	codec, err := pack.NewCodec()
	if err != nil {
		return nil, err
	}
	s := &Store{o: o, codec: codec, index: dedup.New(), inflight: map[string][]byte{},
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
// so it names nothing in a deleted pack.
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
	// Manifests only move forward (every swap increments seq); a slower
	// refresh must not roll back what a faster one saw.
	newer := s.ver == blob.NoVersion || m.seq > s.man.seq
	cond := condemnedPacks(m)
	if rebuild && newer {
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
		s.index = x
	} else if !rebuild {
		var infos []pack.Info
		for sum, packs := range loaded {
			if s.loaded[sum] {
				continue
			}
			infos = append(infos, packs...)
			s.loaded[sum] = true
		}
		addPacks(s.index, infos, cond)
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
	s.unpublished[b.Name] = b.Info
	return &b, nil
}

// upload stores packs; an object that already exists under a pack's name is
// the same bytes (packs are named by their hash). Every pack is attempted and
// every failure kept for the next CAS: a pack dropped here would be a chunk
// the next root reaches that nothing durable holds.
func (s *Store) upload(ctx context.Context, packs []pack.Built) error {
	var first error
	for _, b := range packs {
		err := s.o.Blobs.Put(ctx, b.Name, bytes.NewReader(b.Bytes), int64(len(b.Bytes)))
		if errors.Is(err, blob.ErrExists) {
			err = nil
		}
		s.mu.Lock()
		if err == nil {
			delete(s.inflight, b.Name)
			s.session = append(s.session, b.Info)
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
	if s.lost != nil {
		s.mu.Unlock()
		return hash.Hash{}, s.lost
	}
	if s.pending != nil && s.pending.Has(h) {
		s.mu.Unlock()
		return h, nil
	}
	if loc, ok := s.index.Lookup(h); ok && !s.condemned[loc.Pack.Name] {
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
	if s.isClosed() {
		return nil, chunk.ErrClosed
	}
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
	if next.IsZero() || (!s.index.Has(next) && (s.pending == nil || !s.pending.Has(next))) {
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
	n := int64(s.index.Len())
	if s.pending != nil {
		n += int64(s.pending.Count())
	}
	return chunk.Stats{Chunks: n}, nil
}

func (s *Store) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
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
	names := make([]string, len(session))
	for i, p := range session {
		names[i] = p.Name
	}
	s.mu.Lock()
	s.session = s.session[len(session):]
	s.sessionIdx = append(s.sessionIdx, sum)
	s.loaded[sum] = true
	s.inIndex[sum] = names
	s.uploaded[name] = s.o.Clock()
	s.mu.Unlock()
	return nil
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
		if !s.index.Has(h) && (s.pending == nil || !s.pending.Has(h)) {
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
