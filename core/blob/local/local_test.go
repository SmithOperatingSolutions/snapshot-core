package local_test

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/contract"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/internal/crashtest"
	jcontract "github.com/SmithOperatingSolutions/snapshot-core/core/blob/journal/contract"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/local"
)

var ctx = context.Background()

func newStore(t *testing.T) (*local.Store, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "store")
	s, err := local.Create(dir, local.Options{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	return s, dir
}

func TestContract(t *testing.T) {
	contract.Run(t, func(t *testing.T) blob.BlobStore { s, _ := newStore(t); return s }, contract.Options{})
}

func TestReopenKeepsObjectsAndRoot(t *testing.T) {
	s, dir := newStore(t)
	if err := s.Put(ctx, "packs/aa/one", strings.NewReader("pack bytes"), 10); err != nil {
		t.Fatal(err)
	}
	v, err := s.SwapRoot(ctx, blob.NoVersion, []byte("manifest"))
	if err != nil {
		t.Fatal(err)
	}
	re, err := local.Open(dir, local.Options{})
	if err != nil {
		t.Fatalf("reopening the store: %v", err)
	}
	if re.ID() == "" || re.ID() != s.ID() {
		t.Fatalf("store id %q after reopen, want %q", re.ID(), s.ID())
	}
	r, err := re.Root(ctx)
	if err != nil || string(r.Value) != "manifest" || r.Version != v {
		t.Fatalf("root after reopen = %q@%q, %v; want manifest@%q", r.Value, r.Version, err, v)
	}
	rc, err := re.Get(ctx, "packs/aa/one", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	var b bytes.Buffer
	if _, err := b.ReadFrom(rc); err != nil || b.String() != "pack bytes" {
		t.Fatalf("object after reopen = %q, %v", b.String(), err)
	}
}

// Storage Core Spec: "core/blob/local creates every dir 0700 and file 0600,
// and a test fails on any wider mode". Run with umask 0 so nothing is saved
// by the process's umask.
func TestNothingIsWiderThanOwnerOnly(t *testing.T) {
	restore := setUmask(0)
	defer restore()
	s, dir := newStore(t)
	for _, n := range []string{"packs/ab/x", "index/y", "deep/a/b/c/d"} {
		if err := s.Put(ctx, n, strings.NewReader("data"), 4); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.SwapRoot(ctx, blob.NoVersion, []byte("root")); err != nil {
		t.Fatal(err)
	}
	checked := 0
	err := filepath.WalkDir(filepath.Dir(dir), func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p == filepath.Dir(dir) {
			return nil // the test's own temp dir
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		checked++
		if perm := info.Mode().Perm(); perm&0o077 != 0 {
			t.Errorf("%s has mode %v: other users can read repository metadata", p, perm)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if checked < 8 {
		t.Fatalf("checked only %d paths; the walk did not see the store", checked)
	}
}

func TestOnlyAllowlistedFilesystems(t *testing.T) {
	for _, fs := range local.AllowedFilesystems {
		dir := filepath.Join(t.TempDir(), "s")
		if _, err := local.Create(dir, local.WithFSType(func(string) (string, error) { return fs, nil })); err != nil {
			t.Errorf("positive control: %s refused: %v", fs, err)
		}
	}
	for _, fs := range []string{"nfs", "cifs", "smb2", "fuse", "tmpfs", "overlay", "unknown(0x1234)", ""} {
		dir := filepath.Join(t.TempDir(), "s")
		if _, err := local.Create(dir, local.WithFSType(func(string) (string, error) { return fs, nil })); !errors.Is(err, local.ErrUnsupportedFilesystem) {
			t.Errorf("Create on %q = %v, want ErrUnsupportedFilesystem: a repository on a network or volatile "+
				"filesystem loses fsync and rename guarantees", fs, err)
		}
		// An existing store is refused too if its filesystem is wrong at open.
		_, good := newStore(t)
		if _, err := local.Open(good, local.WithFSType(func(string) (string, error) { return fs, nil })); !errors.Is(err, local.ErrUnsupportedFilesystem) {
			t.Errorf("Open on %q = %v, want ErrUnsupportedFilesystem", fs, err)
		}
	}
}

func TestFilesystemMagicNumbers(t *testing.T) {
	for magic, want := range map[int64]string{
		0xEF53: "ext4", 0x58465342: "xfs", 0x9123683E: "btrfs", 0x2FC12FC1: "zfs",
		0x6969: "nfs", 0xFF534D42: "cifs", 0xFE534D42: "smb2", 0x517B: "smb", 0x65735546: "fuse",
		0x01021994: "tmpfs", 0x794C7630: "overlay", 0x1234: "unknown(0x1234)",
	} {
		if got := local.FSName(magic); got != want {
			t.Errorf("FSName(%#x) = %q, want %q", magic, got, want)
		}
	}
	name, err := local.DetectFS(t.TempDir())
	if err != nil || name == "" {
		t.Fatalf("detecting this machine's temp filesystem: %q, %v", name, err)
	}
}

// An unmounted mount point is an empty directory on the parent filesystem.
// Opening it must fail, not look like an empty repository.
func TestOpenRefusesADirectoryThatIsNotAStore(t *testing.T) {
	empty := t.TempDir()
	if _, err := local.Open(empty, local.Options{}); !errors.Is(err, local.ErrNotAStore) {
		t.Fatalf("Open(empty directory) = %v, want ErrNotAStore", err)
	}
	if _, err := local.Open(filepath.Join(empty, "missing"), local.Options{}); !errors.Is(err, local.ErrNotAStore) {
		t.Fatalf("Open(missing directory) = %v, want ErrNotAStore", err)
	}
	_, dir := newStore(t)
	if err := os.WriteFile(filepath.Join(dir, ".snapshot-core"), []byte("not a marker"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := local.Open(dir, local.Options{}); !errors.Is(err, local.ErrCorrupt) {
		t.Fatalf("Open with a garbled marker = %v, want ErrCorrupt", err)
	}
}

func TestCreateRefusesANonEmptyDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "someone-elses-file"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := local.Create(dir, local.Options{}); !errors.Is(err, local.ErrNotEmpty) {
		t.Fatalf("Create in a non-empty directory = %v, want ErrNotEmpty", err)
	}
	_, existing := newStore(t)
	if _, err := local.Create(existing, local.Options{}); !errors.Is(err, local.ErrNotEmpty) {
		t.Fatalf("Create over an existing store = %v, want ErrNotEmpty", err)
	}
}

func TestOpenRefusesAStoreOthersCanRead(t *testing.T) {
	_, dir := newStore(t)
	if _, err := local.Open(dir, local.Options{}); err != nil {
		t.Fatalf("positive control: %v", err)
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := local.Open(dir, local.Options{}); !errors.Is(err, local.ErrPermissions) {
		t.Fatalf("Open of a 0755 store = %v, want ErrPermissions", err)
	}
}

func TestRootFileCorruptionIsDetected(t *testing.T) {
	s, dir := newStore(t)
	v, err := s.SwapRoot(ctx, blob.NoVersion, []byte("a manifest worth keeping"))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "root")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)/2] ^= 0x01
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	if r, err := s.Root(ctx); !errors.Is(err, local.ErrCorrupt) {
		t.Fatalf("Root of a root file with a flipped byte = %q, %v; want ErrCorrupt", r.Value, err)
	}
	if _, err := s.SwapRoot(ctx, v, []byte("next")); !errors.Is(err, local.ErrCorrupt) {
		t.Fatalf("SwapRoot over a corrupt root = %v, want ErrCorrupt (never swap blind)", err)
	}
}

func TestObjectNamesNeverReachOutsideObjects(t *testing.T) {
	s, dir := newStore(t)
	if err := s.Put(ctx, "a/b", strings.NewReader("file"), 4); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, "a/b/c", strings.NewReader("nested"), 6); err != nil {
		t.Fatalf("an object under another object's name: %v", err)
	}
	var top []string
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		top = append(top, e.Name())
	}
	for _, n := range top {
		if !slices.Contains([]string{".snapshot-core", "objects", "tmp", "root", "root.lock"}, n) {
			t.Errorf("unexpected entry %q at the store's top level", n)
		}
	}
}

func openStore(dir string) (blob.BlobStore, error) { return local.Open(dir, local.Options{}) }

// TestMain doubles as the crash harness's child process.
func TestMain(m *testing.M) { crashtest.Main(m, openStore) }

func TestCrashDuringSwapRootLeavesOldOrNew(t *testing.T) {
	_, dir := newStore(t)
	crashtest.SwapRoot(t, dir, openStore)
}

func TestCrashDuringPutLeavesNothingPartial(t *testing.T) {
	_, dir := newStore(t)
	crashtest.Put(t, dir, openStore)
}

// The journal's contract (#34), with the store opened again from disk.
func TestJournalContract(t *testing.T) {
	jcontract.Run(t, func(t *testing.T) (blob.Journaler, func(*testing.T) blob.Journaler) {
		s, dir := newStore(t)
		return s, func(t *testing.T) blob.Journaler {
			re, err := local.Open(dir, local.Options{})
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			return re
		}
	})
}

// kill -9 mid-append, over and over: every append that returned is in the
// journal whole, and what the one in flight left is a prefix of it.
func TestCrashDuringJournalAppendKeepsWhatReturned(t *testing.T) {
	_, dir := newStore(t)
	crashtest.Append(t, dir, openStore)
}
