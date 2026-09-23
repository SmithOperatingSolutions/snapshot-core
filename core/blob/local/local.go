// Package local is the single-path disk backend (Storage Core Spec
// "core/blob/local"; Engine Spec "filestore" rules). A store is a directory:
//
//	.snapshot-core   marker: format and store id (an unmounted mount point has none)
//	objects/...      one file per object, named <segment>~ so a name and a
//	                 longer name under it ("a/b", "a/b/c") never collide
//	tmp/             bodies being written; linked into objects/ when complete
//	root             the root pointer: version, value, SHA-256 of both
//	root.lock        flock'd around every root swap
//
// Put-if-absent is write-temp, fsync, link(2): link fails if the name exists,
// and an object is visible only complete, even across a crash (O_CREAT|O_EXCL
// on the final name would expose, and after a crash keep, a torn object).
// The root swap is lock, write-temp, fsync, rename, fsync-dir. Directories are
// 0700 and files 0600 regardless of umask. The filesystem must be local and
// on the allowlist (ext4, xfs, btrfs, zfs, apfs); anything else fails closed.
package local

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/internal/wire"
)

// Errors.
var (
	ErrUnsupportedFilesystem = errors.New("local: filesystem is not on the allowlist (ext4, xfs, btrfs, zfs, apfs)")
	ErrNotAStore             = errors.New("local: not a snapshot-core store (no marker: an unmounted volume?)")
	ErrNotEmpty              = errors.New("local: directory is not empty")
	ErrPermissions           = errors.New("local: store directory is accessible to other users")
	ErrCorrupt               = errors.New("local: store metadata is corrupt")
)

// AllowedFilesystems is the allowlist.
var AllowedFilesystems = []string{"ext4", "xfs", "btrfs", "zfs", "apfs"}

// Options configures a store.
type Options struct {
	fsType func(path string) (string, error) // test seam; nil means detect
}

func (o Options) checkFS(path string) error {
	detect := o.fsType
	if detect == nil {
		detect = detectFS
	}
	name, err := detect(path)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUnsupportedFilesystem, err)
	}
	if !slices.Contains(AllowedFilesystems, name) {
		return fmt.Errorf("%w: %s is %q", ErrUnsupportedFilesystem, path, name)
	}
	return nil
}

const (
	markerName  = ".snapshot-core"
	markerMagic = "SCLS"
	markerV1    = 1
	objectsDir  = "objects"
	tmpDir      = "tmp"
	rootName    = "root"
	lockName    = "root.lock"
	fileSuffix  = "~"
	dirPerm     = 0o700
	filePerm    = 0o600
	staleTemp   = time.Hour
)

// Store is a local-disk BlobStore.
type Store struct {
	dir  string
	id   string
	dirs sync.Map // object directories known to exist and be durable
}

var _ blob.BlobStore = (*Store)(nil)

// Create makes a new store in dir, which must be absent or empty.
func Create(dir string, opts Options) (*Store, error) {
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return nil, err
	}
	if err := os.Chmod(dir, dirPerm); err != nil {
		return nil, err
	}
	if err := opts.checkFS(dir); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	if len(entries) > 0 {
		return nil, fmt.Errorf("%w: %s", ErrNotEmpty, dir)
	}
	for _, d := range []string{objectsDir, tmpDir} {
		if err := os.Mkdir(filepath.Join(dir, d), dirPerm); err != nil {
			return nil, err
		}
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, err
	}
	var w wire.Writer
	w.Raw([]byte(markerMagic))
	w.U16(markerV1)
	w.Raw(id[:])
	if err := writeFileSync(filepath.Join(dir, markerName), w.Bytes()); err != nil {
		return nil, err
	}
	if err := syncDir(dir); err != nil {
		return nil, err
	}
	return &Store{dir: dir, id: hex.EncodeToString(id[:])}, nil
}

// Open opens an existing store.
func Open(dir string, opts Options) (*Store, error) {
	info, err := os.Stat(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s does not exist", ErrNotAStore, dir)
	}
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%w: %s is not a directory", ErrNotAStore, dir)
	}
	b, err := os.ReadFile(filepath.Join(dir, markerName))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s", ErrNotAStore, dir)
	}
	if err != nil {
		return nil, err
	}
	id, err := parseMarker(b)
	if err != nil {
		return nil, err
	}
	if err := opts.checkFS(dir); err != nil {
		return nil, err
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("%w: %s has mode %v", ErrPermissions, dir, info.Mode().Perm())
	}
	s := &Store{dir: dir, id: id}
	s.sweepTemp()
	return s, nil
}

func parseMarker(b []byte) (string, error) {
	r := wire.NewReader(b)
	magic := r.Fixed(4)
	version := r.U16()
	id := r.Fixed(16)
	if err := r.Done(); err != nil || string(magic) != markerMagic || version != markerV1 {
		return "", fmt.Errorf("%w: marker", ErrCorrupt)
	}
	return hex.EncodeToString(id), nil
}

// sweepTemp removes bodies abandoned by a crashed writer. Only old ones: a
// temp file younger than staleTemp may belong to a live Put in another process.
func (s *Store) sweepTemp() {
	entries, err := os.ReadDir(filepath.Join(s.dir, tmpDir))
	if err != nil {
		return
	}
	for _, e := range entries {
		if info, err := e.Info(); err == nil && time.Since(info.ModTime()) > staleTemp {
			_ = os.Remove(filepath.Join(s.dir, tmpDir, e.Name()))
		}
	}
}

// ID is the store's identity from its marker.
func (s *Store) ID() string { return s.id }

// objectPath maps a (validated) name to its file.
func (s *Store) objectPath(name string) string {
	return filepath.Join(s.dir, objectsDir, filepath.FromSlash(name)+fileSuffix)
}

// Put implements blob.BlobStore.
func (s *Store) Put(ctx context.Context, name string, r io.Reader, size int64) error {
	if err := blob.CheckPut(name, size); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Join(s.dir, tmpDir), "put-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // after a successful link it is just a second name
	if err := tmp.Chmod(filePerm); err != nil {
		tmp.Close()
		return err
	}
	if err := blob.CopyExact(tmp, r, size); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	final := s.objectPath(name)
	if err := s.ensureDir(filepath.Dir(final)); err != nil {
		return err
	}
	if err := os.Link(tmpName, final); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return blob.ErrExists
		}
		return err
	}
	return syncDir(filepath.Dir(final))
}

// ensureDir creates an object directory and makes every new entry on the way
// durable, so an object the root will reference cannot vanish in a crash.
func (s *Store) ensureDir(dir string) error {
	if _, ok := s.dirs.Load(dir); ok {
		return nil
	}
	objects := filepath.Join(s.dir, objectsDir)
	var missing []string
	for d := dir; d != objects; d = filepath.Dir(d) {
		if _, err := os.Stat(d); err == nil {
			break
		}
		missing = append(missing, d)
	}
	for i := len(missing) - 1; i >= 0; i-- {
		if err := os.Mkdir(missing[i], dirPerm); err != nil && !errors.Is(err, fs.ErrExist) {
			return err
		}
		if err := syncDir(filepath.Dir(missing[i])); err != nil {
			return err
		}
	}
	s.dirs.Store(dir, true)
	return nil
}

// fileReader serves a byte range of an open file and closes it.
type fileReader struct {
	io.Reader
	f *os.File
}

func (r fileReader) Close() error { return r.f.Close() }

// Get implements blob.BlobStore.
func (s *Store) Get(ctx context.Context, name string, off, n int64) (io.ReadCloser, error) {
	if err := blob.ValidName(name); err != nil {
		return nil, err
	}
	f, err := os.Open(s.objectPath(name))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, blob.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	start, length, err := blob.Range(off, n, info.Size())
	if err != nil {
		f.Close()
		return nil, err
	}
	return fileReader{Reader: io.NewSectionReader(f, start, length), f: f}, nil
}

// Stat implements blob.BlobStore.
func (s *Store) Stat(ctx context.Context, name string) (blob.Info, error) {
	if err := blob.ValidName(name); err != nil {
		return blob.Info{}, err
	}
	info, err := os.Stat(s.objectPath(name))
	if errors.Is(err, fs.ErrNotExist) {
		return blob.Info{}, blob.ErrNotFound
	}
	if err != nil {
		return blob.Info{}, err
	}
	return blob.Info{Name: name, Size: info.Size(), ModTime: info.ModTime()}, nil
}

// List implements blob.BlobStore. It walks objects/ in byte order of the
// object names (a directory "a" sorts as "a/", after "a-b" and before "a0"),
// descending only into directories that can hold a name after `after`.
func (s *Store) List(ctx context.Context, prefix, after string, limit int) ([]blob.Info, error) {
	if err := blob.CheckList(prefix, limit); err != nil {
		return nil, err
	}
	// Start in the deepest directory the prefix names completely.
	base := ""
	if i := strings.LastIndex(prefix, "/"); i >= 0 {
		base = prefix[:i+1]
	}
	var out []blob.Info
	err := s.walk(base, prefix, after, limit, &out)
	if errors.Is(err, errLimit) {
		err = nil
	}
	return out, err
}

var errLimit = errors.New("limit reached")

type listEntry struct {
	key   string // the object name, or the directory's name + "/"
	isDir bool
	info  fs.FileInfo
}

func (s *Store) walk(base, prefix, after string, limit int, out *[]blob.Info) error {
	dir := filepath.Join(s.dir, objectsDir, filepath.FromSlash(base))
	des, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	entries := make([]listEntry, 0, len(des))
	for _, de := range des {
		switch {
		case de.IsDir():
			entries = append(entries, listEntry{key: base + de.Name() + "/", isDir: true})
		case strings.HasSuffix(de.Name(), fileSuffix):
			info, err := de.Info()
			if errors.Is(err, fs.ErrNotExist) {
				continue // deleted while listing
			}
			if err != nil {
				return err
			}
			entries = append(entries, listEntry{key: base + strings.TrimSuffix(de.Name(), fileSuffix), info: info})
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].key < entries[j].key })
	for _, e := range entries {
		// Every name under a directory starts with e.key; the prefix must be
		// compatible with that, and something under it must sort after after.
		if !strings.HasPrefix(e.key, prefix) && !strings.HasPrefix(prefix, e.key) {
			continue
		}
		if e.isDir {
			if after >= e.key && !strings.HasPrefix(after, e.key) {
				continue // every name in here is <= after
			}
			if err := s.walk(e.key, prefix, after, limit, out); err != nil {
				return err
			}
			continue
		}
		if e.key <= after || !strings.HasPrefix(e.key, prefix) {
			continue
		}
		*out = append(*out, blob.Info{Name: e.key, Size: e.info.Size(), ModTime: e.info.ModTime()})
		if len(*out) == limit {
			return errLimit
		}
	}
	return nil
}

// Delete implements blob.BlobStore.
func (s *Store) Delete(ctx context.Context, name string) error {
	if err := blob.ValidName(name); err != nil {
		return err
	}
	p := s.objectPath(name)
	if err := os.Remove(p); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	return syncDir(filepath.Dir(p))
}

// Root file: magic "SCRF" | version (len-prefixed) | value (len-prefixed) |
// SHA-256 of everything before it. rename(2) makes the file old or new; the
// checksum turns anything else (a flipped bit, a lying disk) into ErrCorrupt.
const rootMagic = "SCRF"

func encodeRoot(v blob.Version, value []byte) []byte {
	var w wire.Writer
	w.Raw([]byte(rootMagic))
	w.LenBytes([]byte(v))
	w.LenBytes(value)
	sum := sha256.Sum256(w.Bytes())
	w.Raw(sum[:])
	return w.Bytes()
}

func decodeRoot(b []byte) (blob.Root, error) {
	if len(b) < sha256.Size {
		return blob.Root{}, fmt.Errorf("%w: root file is %d bytes", ErrCorrupt, len(b))
	}
	body, sum := b[:len(b)-sha256.Size], b[len(b)-sha256.Size:]
	if want := sha256.Sum256(body); !bytes.Equal(sum, want[:]) {
		return blob.Root{}, fmt.Errorf("%w: root file checksum mismatch", ErrCorrupt)
	}
	r := wire.NewReader(body)
	magic := r.Fixed(4)
	v := r.LenBytes(64)
	value := r.LenBytes(blob.MaxRootSize)
	if err := r.Done(); err != nil || string(magic) != rootMagic || len(v) == 0 {
		return blob.Root{}, fmt.Errorf("%w: root file", ErrCorrupt)
	}
	return blob.Root{Value: bytes.Clone(value), Version: blob.Version(v)}, nil
}

func (s *Store) readRoot() (blob.Root, error) {
	b, err := os.ReadFile(filepath.Join(s.dir, rootName))
	if errors.Is(err, fs.ErrNotExist) {
		return blob.Root{}, nil
	}
	if err != nil {
		return blob.Root{}, err
	}
	return decodeRoot(b)
}

// Root implements blob.BlobStore.
func (s *Store) Root(ctx context.Context) (blob.Root, error) { return s.readRoot() }

// SwapRoot implements blob.BlobStore.
func (s *Store) SwapRoot(ctx context.Context, expected blob.Version, next []byte) (blob.Version, error) {
	if err := blob.CheckRootValue(next); err != nil {
		return blob.NoVersion, err
	}
	unlock, err := lockFile(filepath.Join(s.dir, lockName))
	if err != nil {
		return blob.NoVersion, err
	}
	defer unlock()
	cur, err := s.readRoot()
	if err != nil {
		return blob.NoVersion, err
	}
	if cur.Version != expected {
		return blob.NoVersion, blob.ErrRootConflict
	}
	v, err := blob.NewVersion()
	if err != nil {
		return blob.NoVersion, err
	}
	tmp, err := os.CreateTemp(filepath.Join(s.dir, tmpDir), "root-*")
	if err != nil {
		return blob.NoVersion, err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after the rename
	_, werr := tmp.Write(encodeRoot(v, next))
	if werr == nil {
		werr = tmp.Chmod(filePerm)
	}
	if werr == nil {
		werr = tmp.Sync()
	}
	if cerr := tmp.Close(); werr == nil {
		werr = cerr
	}
	if werr != nil {
		return blob.NoVersion, werr
	}
	if err := os.Rename(tmpName, filepath.Join(s.dir, rootName)); err != nil {
		return blob.NoVersion, err
	}
	if err := syncDir(s.dir); err != nil {
		return blob.NoVersion, err
	}
	return v, nil
}

func writeFileSync(path string, b []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, filePerm)
	if err != nil {
		return err
	}
	_, err = f.Write(b)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

// fsName maps a Linux statfs magic number to a filesystem name.
func fsName(magic int64) string {
	switch magic {
	case 0xEF53:
		return "ext4" // ext2/3/4 share the magic; all are local journaled-or-not ext
	case 0x58465342:
		return "xfs"
	case 0x9123683E:
		return "btrfs"
	case 0x2FC12FC1:
		return "zfs"
	case 0x6969:
		return "nfs"
	case 0xFF534D42:
		return "cifs"
	case 0xFE534D42:
		return "smb2"
	case 0x517B:
		return "smb"
	case 0x65735546:
		return "fuse"
	case 0x01021994:
		return "tmpfs"
	case 0x794C7630:
		return "overlay"
	default:
		return fmt.Sprintf("unknown(%#x)", magic)
	}
}
