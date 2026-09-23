//go:build unix

package fsutil_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/internal/fsutil"
)

func requireUnprivileged(t *testing.T) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Fatal("fault-injection tests deny access with chmod, which root ignores: run the suite as an ordinary user")
	}
}

func TestWriteFileAtomicReplacesAndLeavesNoTemp(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "map")
	for _, body := range []string{"first version", "second"} {
		if err := fsutil.WriteFileAtomic(p, []byte(body)); err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(p)
		if err != nil || string(got) != body {
			t.Fatalf("after WriteFileAtomic(%q) the file reads %q (%v)", body, got, err)
		}
	}
	info, err := os.Stat(p)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != fsutil.FilePerm {
		t.Errorf("mode %v, want %v", info.Mode().Perm(), os.FileMode(fsutil.FilePerm))
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Errorf("the directory holds %d entries after two atomic writes; temp files were left behind", len(entries))
	}
}

func TestWriteFileAtomicFailureLeavesTheOldFile(t *testing.T) {
	requireUnprivileged(t)
	dir := t.TempDir()
	p := filepath.Join(dir, "map")
	if err := fsutil.WriteFileAtomic(p, []byte("keep me")); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(dir, 0o700)
	if err := fsutil.WriteFileAtomic(p, []byte("replacement")); err == nil {
		t.Fatal("WriteFileAtomic into an unwritable directory reported success")
	}
	if got, _ := os.ReadFile(p); string(got) != "keep me" {
		t.Fatalf("a failed atomic write changed the file to %q", got)
	}
	if err := fsutil.WriteFileAtomic(filepath.Join(dir, "missing", "x"), []byte("x")); err == nil {
		t.Fatal("WriteFileAtomic into a missing directory reported success")
	}
}

// A failure after the temp file exists (here the rename: the target is a
// non-empty directory) must not leave the temp file behind.
func TestWriteFileAtomicFailureAfterTempCleansUp(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	if err := os.MkdirAll(filepath.Join(target, "occupied"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := fsutil.WriteFileAtomic(target, []byte("x")); err == nil {
		t.Fatal("renaming over a non-empty directory reported success")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("after the failed write the directory holds %v: the temp file leaked", names)
	}
}

// The lock must be exclusive: while it is held, another open file description
// cannot take even a shared lock. Checked with LOCK_NB, so nothing races.
func TestLockIsExclusiveUntilReleased(t *testing.T) {
	p := filepath.Join(t.TempDir(), "lock")
	unlock, err := fsutil.Lock(p)
	if err != nil {
		t.Fatal(err)
	}
	probe := func() error {
		f, err := os.Open(p)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		err = unix.Flock(int(f.Fd()), unix.LOCK_SH|unix.LOCK_NB)
		if err == nil {
			_ = unix.Flock(int(f.Fd()), unix.LOCK_UN)
		}
		return err
	}
	if err := probe(); !errors.Is(err, unix.EWOULDBLOCK) {
		t.Fatalf("while the lock is held a probe got %v, want EWOULDBLOCK: two swaps could interleave", err)
	}
	unlock()
	if err := probe(); err != nil {
		t.Fatalf("after unlock the lock is still held: %v", err)
	}
	if _, err := fsutil.Lock(filepath.Join(t.TempDir(), "missing", "lock")); err == nil {
		t.Fatal("Lock in a missing directory reported success")
	}
}

func TestIdentityTellsDirectoriesApart(t *testing.T) {
	a, b := t.TempDir(), t.TempDir()
	da, ia, err := fsutil.Identity(a)
	if err != nil {
		t.Fatal(err)
	}
	da2, ia2, err := fsutil.Identity(a)
	if err != nil || da2 != da || ia2 != ia {
		t.Fatalf("the same directory has two identities (%d/%d vs %d/%d, %v)", da, ia, da2, ia2, err)
	}
	db, ib, err := fsutil.Identity(b)
	if err != nil {
		t.Fatal(err)
	}
	if da == db && ia == ib {
		t.Fatal("two different directories share an identity: a swapped volume would go unnoticed")
	}
	if _, _, err := fsutil.Identity(filepath.Join(a, "missing")); err == nil {
		t.Fatal("Identity of a missing path reported success")
	}
}

func TestFreeSpaceAndSyncDir(t *testing.T) {
	dir := t.TempDir()
	free, total, err := fsutil.FreeSpace(dir)
	if err != nil || total == 0 || free > total {
		t.Fatalf("FreeSpace = %d of %d, %v", free, total, err)
	}
	if _, _, err := fsutil.FreeSpace(filepath.Join(dir, "missing")); err == nil {
		t.Fatal("FreeSpace of a missing path reported success")
	}
	if err := fsutil.SyncDir(dir); err != nil {
		t.Fatalf("positive control: %v", err)
	}
	if err := fsutil.SyncDir(filepath.Join(dir, "missing")); err == nil {
		t.Fatal("SyncDir of a missing directory reported success")
	}
}
