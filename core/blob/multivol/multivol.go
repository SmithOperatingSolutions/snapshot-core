// Package multivol spreads one store across several local mount points
// (Storage Core Spec "core/blob/multivol"; Engine Spec "multistore" rules).
//
// Each volume is a blob/local store in <path>/vol, so every volume passes the
// local-filesystem allowlist, carries its own marker (a random UUID), and is
// crash-safe on its own. The primary volume also holds the volume map (volume
// UUID -> path) and the root. An object lives on the volume rendezvous hashing
// of its name picks among the volumes taking new objects; reads try volumes in
// that order, so objects placed before a volume was added are still found
// where they are. A volume that is missing, remounted or swapped is detected
// at open and on every operation, and the store goes read-only
// (ErrVolumeMissing) rather than guessing.
package multivol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/internal/fsutil"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/local"
	"github.com/SmithOperatingSolutions/snapshot-core/core/internal/wire"
)

// Errors.
var (
	ErrVolumeMissing  = fmt.Errorf("multivol: a volume is missing or remounted: %w", blob.ErrReadOnly)
	ErrVolumeMismatch = errors.New("multivol: a volume's marker does not match the volume map")
	ErrTooManyVolumes = errors.New("multivol: more than 64 volumes")
	ErrNoSpace        = errors.New("multivol: every volume is below its free-space floor")
	ErrNotAStore      = errors.New("multivol: not a multivol store")
	ErrCorrupt        = errors.New("multivol: volume map is corrupt")
)

// MaxVolumes is the Engine Spec's limit.
const MaxVolumes = 64

// DefaultFloor is the free-space fraction below which a volume stops taking
// new objects.
const DefaultFloor = 0.05

// Options configures a store.
type Options struct {
	Floor     float64                                           // 0 means DefaultFloor
	freeSpace func(path string) (free, total uint64, err error) // test seam; nil means statfs
}

const (
	mapName  = "multivol.map"
	lockName = "multivol.lock"
	volDir   = "vol"
	mapMagic = "SCMV"
	mapV1    = 1
	maxPath  = 4096
)

type volume struct {
	id       string
	path     string
	st       *local.Store // nil once missing
	dev, ino uint64       // identity of <path>/vol when opened
}

// Store is a multi-volume BlobStore.
type Store struct {
	primary string
	opts    Options

	mu       sync.RWMutex
	vols     []*volume
	readOnly bool
}

var _ blob.BlobStore = (*Store)(nil)

// Create makes a store over a primary path and any number of secondaries.
func Create(primary string, secondaries []string, opts Options) (*Store, error) {
	if 1+len(secondaries) > MaxVolumes {
		return nil, fmt.Errorf("%w: %d", ErrTooManyVolumes, 1+len(secondaries))
	}
	paths := append([]string{primary}, secondaries...)
	for i, p := range paths {
		abs, err := filepath.Abs(p)
		if err != nil {
			return nil, err
		}
		paths[i] = abs
	}
	if _, err := os.Stat(filepath.Join(paths[0], mapName)); err == nil {
		return nil, fmt.Errorf("%w: %s already holds a store", local.ErrNotEmpty, paths[0])
	}
	s := &Store{primary: paths[0], opts: opts}
	for _, p := range paths {
		v, err := createVolume(p)
		if err != nil {
			return nil, err
		}
		s.vols = append(s.vols, v)
	}
	if err := s.writeMap(); err != nil {
		return nil, err
	}
	return s, nil
}

func createVolume(path string) (*volume, error) {
	if err := os.MkdirAll(path, fsutil.DirPerm); err != nil {
		return nil, err
	}
	if err := os.Chmod(path, fsutil.DirPerm); err != nil {
		return nil, err
	}
	st, err := local.Create(filepath.Join(path, volDir), local.Options{})
	if err != nil {
		return nil, err
	}
	dev, ino, err := fsutil.Identity(filepath.Join(path, volDir))
	if err != nil {
		return nil, err
	}
	return &volume{id: st.ID(), path: path, st: st, dev: dev, ino: ino}, nil
}

// Open opens the store whose primary volume is at primary.
func Open(primary string, opts Options) (*Store, error) {
	abs, err := filepath.Abs(primary)
	if err != nil {
		return nil, err
	}
	b, err := os.ReadFile(filepath.Join(abs, mapName)) //nolint:gosec // G304: the store's own volume map
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%w: no volume map at %s (is the primary volume mounted?)", ErrNotAStore, abs)
	}
	if err != nil {
		return nil, err
	}
	entries, err := decodeMap(b)
	if err != nil {
		return nil, err
	}
	s := &Store{primary: abs, opts: opts}
	for i, e := range entries {
		v := &volume{id: e.id, path: e.path}
		st, err := local.Open(filepath.Join(e.path, volDir), local.Options{})
		switch {
		case errors.Is(err, local.ErrNotAStore) && i == 0:
			return nil, fmt.Errorf("%w: primary volume: %w", ErrNotAStore, err)
		case errors.Is(err, local.ErrNotAStore):
			s.readOnly = true // missing: open read-only rather than guess
		case err != nil:
			return nil, err
		case st.ID() != e.id:
			return nil, fmt.Errorf("%w: %s holds volume %s, the map says %s", ErrVolumeMismatch, e.path, st.ID(), e.id)
		default:
			dev, ino, err := fsutil.Identity(filepath.Join(e.path, volDir))
			if err != nil {
				return nil, err
			}
			v.st, v.dev, v.ino = st, dev, ino
		}
		s.vols = append(s.vols, v)
	}
	return s, nil
}

type mapEntry struct{ id, path string }

// Volume map: magic "SCMV" | version u16 | count u16 | count x (id [16] |
// path len-prefixed) | SHA-256 of everything before it.
func (s *Store) writeMap() error {
	var w wire.Writer
	w.Raw([]byte(mapMagic))
	w.U16(mapV1)
	w.U16(uint16(len(s.vols)))
	for _, v := range s.vols {
		id, err := hex.DecodeString(v.id)
		if err != nil || len(id) != 16 {
			return fmt.Errorf("multivol: volume id %q", v.id)
		}
		w.Raw(id)
		w.LenBytes([]byte(v.path))
	}
	sum := sha256.Sum256(w.Bytes())
	w.Raw(sum[:])
	return fsutil.WriteFileAtomic(filepath.Join(s.primary, mapName), w.Bytes())
}

func decodeMap(b []byte) ([]mapEntry, error) {
	if len(b) < sha256.Size {
		return nil, ErrCorrupt
	}
	body, sum := b[:len(b)-sha256.Size], b[len(b)-sha256.Size:]
	if want := sha256.Sum256(body); !bytes.Equal(sum, want[:]) {
		return nil, fmt.Errorf("%w: checksum", ErrCorrupt)
	}
	r := wire.NewReader(body)
	magic := r.Fixed(4)
	version := r.U16()
	n := int(r.U16())
	if string(magic) != mapMagic || version != mapV1 || n == 0 || n > MaxVolumes {
		return nil, ErrCorrupt
	}
	entries := make([]mapEntry, 0, n)
	for i := 0; i < n; i++ {
		id := r.Fixed(16)
		path := r.LenBytes(maxPath)
		entries = append(entries, mapEntry{id: hex.EncodeToString(id), path: string(path)})
	}
	if err := r.Done(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrCorrupt, err)
	}
	return entries, nil
}

// AddVolume adds a volume online. New objects start landing on it; nothing
// already stored moves.
func (s *Store) AddVolume(path string) error {
	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	unlock, err := fsutil.Lock(filepath.Join(s.primary, lockName))
	if err != nil {
		return err
	}
	defer unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.verifyAllLocked(); err != nil {
		return err
	}
	if len(s.vols) >= MaxVolumes {
		return fmt.Errorf("%w: already %d", ErrTooManyVolumes, len(s.vols))
	}
	v, err := createVolume(abs)
	if err != nil {
		return err
	}
	s.vols = append(s.vols, v)
	if err := s.writeMap(); err != nil {
		s.vols = s.vols[:len(s.vols)-1]
		return err
	}
	return nil
}

// ReadOnly reports whether a volume has gone missing.
func (s *Store) ReadOnly() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.readOnly
}

// VolumeIDs lists the volumes' UUIDs in map order (primary first).
func (s *Store) VolumeIDs() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make([]string, len(s.vols))
	for i, v := range s.vols {
		ids[i] = v.id
	}
	return ids
}

func score(id, name string) uint64 {
	h := sha256.Sum256([]byte(id + "\x00" + name))
	return binary.BigEndian.Uint64(h[:8])
}

// rank orders volume indices by rendezvous score for name, best first. It is
// the only placement rule: Put, reads and Place all use it.
func rank(ids []string, name string) []int {
	idx := make([]int, len(ids))
	scores := make([]uint64, len(ids))
	for i, id := range ids {
		idx[i], scores[i] = i, score(id, name)
	}
	sort.SliceStable(idx, func(a, b int) bool { return scores[idx[a]] > scores[idx[b]] })
	return idx
}

// Place returns, for each name, the index into volumeIDs of the volume
// rendezvous hashing assigns it (all volumes taking objects).
func Place(names []string, volumeIDs []string) []int {
	out := make([]int, len(names))
	for i, n := range names {
		out[i] = rank(volumeIDs, n)[0]
	}
	return out
}

// ranked returns the volumes in rendezvous order for name. Callers hold s.mu.
func (s *Store) ranked(name string) []*volume {
	ids := make([]string, len(s.vols))
	for i, v := range s.vols {
		ids[i] = v.id
	}
	vs := make([]*volume, 0, len(s.vols))
	for _, i := range rank(ids, name) {
		vs = append(vs, s.vols[i])
	}
	return vs
}

// verifyLocked checks one volume is still the one opened: present, same
// directory identity. A failure marks it missing and the store read-only.
// Callers hold s.mu for writing, or read with verify().
func (s *Store) verifyLocked(v *volume) error {
	if v.st == nil {
		return ErrVolumeMissing
	}
	dev, ino, err := fsutil.Identity(filepath.Join(v.path, volDir))
	if err != nil || dev != v.dev || ino != v.ino {
		v.st = nil
		s.readOnly = true
		return fmt.Errorf("%w: %s", ErrVolumeMissing, v.path)
	}
	return nil
}

func (s *Store) verifyAllLocked() error {
	var first error
	for _, v := range s.vols {
		if err := s.verifyLocked(v); err != nil && first == nil {
			first = err
		}
	}
	if first == nil && s.readOnly {
		first = ErrVolumeMissing
	}
	return first
}

// snapshot verifies every volume and returns them, and whether any is missing.
func (s *Store) snapshot() ([]*volume, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.verifyAllLocked()
	return append([]*volume(nil), s.vols...), err
}

func (s *Store) floor() float64 {
	if s.opts.Floor > 0 {
		return s.opts.Floor
	}
	return DefaultFloor
}

func (s *Store) hasRoom(v *volume) bool {
	probe := s.opts.freeSpace
	if probe == nil {
		probe = fsutil.FreeSpace
	}
	free, total, err := probe(filepath.Join(v.path, volDir))
	if err != nil || total == 0 {
		return false
	}
	return float64(free)/float64(total) >= s.floor()
}

// Put implements blob.BlobStore.
func (s *Store) Put(ctx context.Context, name string, r io.Reader, size int64) error {
	if err := blob.CheckPut(name, size); err != nil {
		return err
	}
	if _, err := s.snapshot(); err != nil {
		return err
	}
	s.mu.RLock()
	ranked := s.ranked(name)
	s.mu.RUnlock()
	// Put-if-absent across volumes: an object placed before a volume was
	// added lives where it was put, not where it would be put today.
	for _, v := range ranked {
		if _, err := v.st.Stat(ctx, name); err == nil {
			return blob.ErrExists
		} else if !errors.Is(err, blob.ErrNotFound) {
			return err
		}
	}
	for _, v := range ranked {
		if s.hasRoom(v) {
			return v.st.Put(ctx, name, r, size)
		}
	}
	return ErrNoSpace
}

// find locates name, trying volumes in rendezvous order. If it is on no
// present volume and a volume is missing, the answer is ErrVolumeMissing: the
// object may be there.
func (s *Store) find(ctx context.Context, name string, f func(*local.Store) error) error {
	if err := blob.ValidName(name); err != nil {
		return err
	}
	s.mu.Lock()
	ranked := s.ranked(name)
	missing := false
	var present []*local.Store
	for _, v := range ranked {
		if err := s.verifyLocked(v); err != nil {
			missing = true
			continue
		}
		present = append(present, v.st)
	}
	s.mu.Unlock()
	for _, st := range present {
		err := f(st)
		if errors.Is(err, blob.ErrNotFound) {
			continue
		}
		return err
	}
	if missing {
		return fmt.Errorf("%w: %s may be on it", ErrVolumeMissing, name)
	}
	return blob.ErrNotFound
}

// Get implements blob.BlobStore.
func (s *Store) Get(ctx context.Context, name string, off, n int64) (io.ReadCloser, error) {
	var rc io.ReadCloser
	err := s.find(ctx, name, func(st *local.Store) error {
		var err error
		rc, err = st.Get(ctx, name, off, n)
		return err
	})
	return rc, err
}

// Stat implements blob.BlobStore.
func (s *Store) Stat(ctx context.Context, name string) (blob.Info, error) {
	var info blob.Info
	err := s.find(ctx, name, func(st *local.Store) error {
		var err error
		info, err = st.Stat(ctx, name)
		return err
	})
	return info, err
}

// List implements blob.BlobStore. It refuses when a volume is missing: a
// partial listing is how a garbage collector concludes packs are gone.
func (s *Store) List(ctx context.Context, prefix, after string, limit int) ([]blob.Info, error) {
	if err := blob.CheckList(prefix, limit); err != nil {
		return nil, err
	}
	vols, err := s.snapshot()
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var all []blob.Info
	for _, v := range vols {
		infos, err := v.st.List(ctx, prefix, after, limit)
		if err != nil {
			return nil, err
		}
		for _, in := range infos {
			if !seen[in.Name] {
				seen[in.Name] = true
				all = append(all, in)
			}
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })
	if len(all) > limit {
		all = all[:limit]
	}
	return all, nil
}

// Delete implements blob.BlobStore.
func (s *Store) Delete(ctx context.Context, name string) error {
	if err := blob.ValidName(name); err != nil {
		return err
	}
	vols, err := s.snapshot()
	if err != nil {
		return err
	}
	for _, v := range vols {
		if err := v.st.Delete(ctx, name); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) primaryStore() (*local.Store, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.verifyLocked(s.vols[0]); err != nil {
		return nil, err
	}
	return s.vols[0].st, nil
}

// Root implements blob.BlobStore.
func (s *Store) Root(ctx context.Context) (blob.Root, error) {
	p, err := s.primaryStore()
	if err != nil {
		return blob.Root{}, err
	}
	return p.Root(ctx)
}

// SwapRoot implements blob.BlobStore. A read-only store swaps nothing.
func (s *Store) SwapRoot(ctx context.Context, expected blob.Version, next []byte) (blob.Version, error) {
	if err := blob.CheckRootValue(next); err != nil {
		return blob.NoVersion, err
	}
	vols, err := s.snapshot()
	if err != nil {
		return blob.NoVersion, err
	}
	return vols[0].st.SwapRoot(ctx, expected, next)
}
