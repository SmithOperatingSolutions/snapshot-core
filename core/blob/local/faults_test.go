//go:build unix

package local_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/local"
)

// Fault injection: on any error, nothing partial reaches the store (Engine
// Spec security table). Access is denied with chmod, which root ignores.

func requireUnprivileged(t *testing.T) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Fatal("fault-injection tests deny access with chmod, which root ignores: run the suite as an ordinary user")
	}
}

func deny(t *testing.T, path string) {
	t.Helper()
	if err := os.Chmod(path, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0o700) })
}

func allow(t *testing.T, path string) {
	t.Helper()
	if err := os.Chmod(path, 0o700); err != nil {
		t.Fatal(err)
	}
}

func entries(t *testing.T, dir string) []string {
	t.Helper()
	des, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, d := range des {
		names = append(names, d.Name())
	}
	return names
}

func TestPutFailureLeavesNoTraces(t *testing.T) {
	requireUnprivileged(t)
	s, dir := newStore(t)
	deny(t, filepath.Join(dir, "tmp"))
	if err := s.Put(ctx, "packs/x", strings.NewReader("body"), 4); err == nil {
		t.Fatal("Put with no room for its temp file reported success")
	}
	if _, err := s.Stat(ctx, "packs/x"); !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("after a failed Put the object is visible (Stat err=%v)", err)
	}
	allow(t, filepath.Join(dir, "tmp"))
	if err := s.Put(ctx, "packs/x", strings.NewReader("body"), 4); err != nil {
		t.Fatalf("positive control: the retried Put failed: %v", err)
	}
}

// A failure after the body is written (here: the object's directory cannot
// be created) must not strand the body in tmp/.
func TestPutFailureAfterTheBodyCleansItsTemp(t *testing.T) {
	requireUnprivileged(t)
	s, dir := newStore(t)
	deny(t, filepath.Join(dir, "objects"))
	if err := s.Put(ctx, "newdir/x", strings.NewReader("body"), 4); err == nil {
		t.Fatal("Put into a directory that cannot be created reported success")
	}
	if left := entries(t, filepath.Join(dir, "tmp")); len(left) != 0 {
		t.Fatalf("a failed Put left %v in tmp/: every failed write would leak its body", left)
	}
}

func TestSwapRootFailureLeavesTheRootUnchanged(t *testing.T) {
	requireUnprivileged(t)
	s, dir := newStore(t)
	v, err := s.SwapRoot(ctx, blob.NoVersion, []byte("before"))
	if err != nil {
		t.Fatal(err)
	}
	deny(t, filepath.Join(dir, "tmp"))
	if _, err := s.SwapRoot(ctx, v, []byte("after")); err == nil {
		t.Fatal("SwapRoot with no room for its temp file reported success")
	}
	r, err := s.Root(ctx)
	if err != nil || string(r.Value) != "before" || r.Version != v {
		t.Fatalf("after a failed swap the root is %q@%q (%v), want before@%q", r.Value, r.Version, err, v)
	}
	allow(t, filepath.Join(dir, "tmp"))
	if _, err := s.SwapRoot(ctx, v, []byte("after")); err != nil {
		t.Fatalf("positive control: a swap with the unchanged version failed: %v", err)
	}
}

// An object that exists but cannot be read is an error, never "not found":
// dedup would re-store it and GC would think it gone.
func TestUnreadableObjectIsAnErrorNotNotFound(t *testing.T) {
	requireUnprivileged(t)
	s, dir := newStore(t)
	if err := s.Put(ctx, "packs/y", strings.NewReader("body"), 4); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, "objects", "packs", "y~")
	if err := os.Chmod(p, 0o000); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(p, 0o600) }()
	rc, err := s.Get(ctx, "packs/y", 0, -1)
	if err == nil {
		rc.Close()
		t.Fatal("Get of an unreadable object reported success")
	}
	if errors.Is(err, blob.ErrNotFound) {
		t.Fatal("Get of an unreadable object reported ErrNotFound: the object exists")
	}
}

func TestDeleteReportsFailures(t *testing.T) {
	requireUnprivileged(t)
	s, dir := newStore(t)
	if err := s.Put(ctx, "packs/z", strings.NewReader("body"), 4); err != nil {
		t.Fatal(err)
	}
	deny(t, filepath.Join(dir, "objects", "packs"))
	if err := s.Delete(ctx, "packs/z"); err == nil {
		t.Fatal("Delete that could not remove the file reported success: GC would believe the pack gone")
	}
	allow(t, filepath.Join(dir, "objects", "packs"))
	if _, err := s.Stat(ctx, "packs/z"); err != nil {
		t.Fatalf("the object vanished after a failed Delete: %v", err)
	}
}

// Bodies abandoned by a crashed writer are swept at Open, but only old ones:
// a young temp file may belong to a live Put in another process.
func TestStaleTempFilesAreSweptAtOpen(t *testing.T) {
	_, dir := newStore(t)
	old := filepath.Join(dir, "tmp", "put-old")
	young := filepath.Join(dir, "tmp", "put-young")
	for _, p := range []string{old, young} {
		if err := os.WriteFile(p, []byte("abandoned"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	past := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(old, past, past); err != nil {
		t.Fatal(err)
	}
	if _, err := local.Open(dir, local.Options{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Errorf("a two-hour-old temp file survived Open (err=%v): crashed writers leak disk forever", err)
	}
	if _, err := os.Stat(young); err != nil {
		t.Errorf("a fresh temp file was swept at Open (err=%v): a concurrent writer's Put would fail", err)
	}
}

// rootFile builds a root file in the documented format with a valid checksum
// over whatever body it is given, to prove the decoder checks more than the sum.
func rootFile(magic, version, value string) []byte {
	var b []byte
	b = append(b, magic...)
	b = binary.AppendUvarint(b, uint64(len(version)))
	b = append(b, version...)
	b = binary.AppendUvarint(b, uint64(len(value)))
	b = append(b, value...)
	sum := sha256.Sum256(b)
	return append(b, sum[:]...)
}

func TestRootFileForgeriesAreCorrupt(t *testing.T) {
	s, dir := newStore(t)
	path := filepath.Join(dir, "root")
	write := func(b []byte) {
		t.Helper()
		if err := os.WriteFile(path, b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(rootFile("SCRF", "v1", "a manifest"))
	if r, err := s.Root(ctx); err != nil || string(r.Value) != "a manifest" || r.Version != "v1" {
		t.Fatalf("positive control: a well-formed root file read as %q@%q, %v", r.Value, r.Version, err)
	}
	for name, b := range map[string][]byte{
		"shorter than a checksum": []byte("short"),
		"wrong magic":             rootFile("XXXX", "v1", "a manifest"),
		"empty version":           rootFile("SCRF", "", "a manifest"),
		"trailing bytes":          append(rootFile("SCRF", "v1", "a manifest")[:0:0], rootFileWithTrailer()...),
	} {
		write(b)
		if _, err := s.Root(ctx); !errors.Is(err, local.ErrCorrupt) {
			t.Errorf("%s: Root = %v, want ErrCorrupt", name, err)
		}
	}
}

func rootFileWithTrailer() []byte {
	var b []byte
	b = append(b, "SCRF"...)
	b = binary.AppendUvarint(b, 2)
	b = append(b, "v1"...)
	b = binary.AppendUvarint(b, 1)
	b = append(b, 'x', 'y') // one byte more than the length says
	sum := sha256.Sum256(b)
	return append(b, sum[:]...)
}

func TestOpenRefusesAFileAndCreateRefusesTheUnwritable(t *testing.T) {
	requireUnprivileged(t)
	f := filepath.Join(t.TempDir(), "plain-file")
	if err := os.WriteFile(f, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := local.Open(f, local.Options{}); !errors.Is(err, local.ErrNotAStore) {
		t.Fatalf("Open(a regular file) = %v, want ErrNotAStore", err)
	}
	parent := t.TempDir()
	deny(t, parent)
	if _, err := local.Create(filepath.Join(parent, "store"), local.Options{}); err == nil {
		t.Fatal("Create under an unwritable parent reported success")
	}
}

func FuzzDecodeRoot(f *testing.F) {
	f.Add(local.EncodeRoot("0123456789abcdef", []byte("manifest")))
	f.Add([]byte("SCRF"))
	f.Fuzz(func(t *testing.T, b []byte) {
		value, version, err := local.DecodeRoot(b)
		if err != nil {
			return
		}
		if !bytes.Equal(local.EncodeRoot(version, value), b) {
			t.Fatalf("decoded a root file that does not re-encode to itself: the format is not canonical")
		}
	})
}

func FuzzParseMarker(f *testing.F) {
	f.Add(append([]byte("SCLS\x01\x00"), make([]byte, 16)...))
	f.Fuzz(func(t *testing.T, b []byte) {
		id, err := local.ParseMarker(b)
		if err == nil && len(id) != 32 {
			t.Fatalf("parsed a marker with a %d-character id", len(id))
		}
	})
}

// An I/O error must never be read as a benign state. Each of these is a
// misclassification that would silently lose data one layer up.

// An unreadable root is not "no root yet": if it were, SwapRoot(NoVersion)
// would create a fresh root over the real one.
func TestUnreadableRootIsAnErrorNeverNoRoot(t *testing.T) {
	requireUnprivileged(t)
	s, dir := newStore(t)
	if _, err := s.SwapRoot(ctx, blob.NoVersion, []byte("the real root")); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, "root")
	if err := os.Chmod(p, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(p, 0o600) })
	if r, err := s.Root(ctx); err == nil {
		t.Fatalf("Root of an unreadable root file = %q with no error", r.Value)
	}
	if _, err := s.SwapRoot(ctx, blob.NoVersion, []byte("an impostor")); err == nil {
		t.Fatal("SwapRoot(NoVersion) succeeded over an unreadable root: the real root was overwritten")
	}
	_ = os.Chmod(p, 0o600)
	if r, err := s.Root(ctx); err != nil || string(r.Value) != "the real root" {
		t.Fatalf("the real root is gone: %q, %v", r.Value, err)
	}
}

// An unreadable directory is not an empty one: a listing that skipped it
// would tell GC every pack in it is gone.
func TestUnreadableDirectoryFailsTheListing(t *testing.T) {
	requireUnprivileged(t)
	s, dir := newStore(t)
	for _, n := range []string{"packs/aa/1", "packs/bb/2"} {
		if err := s.Put(ctx, n, strings.NewReader("x"), 1); err != nil {
			t.Fatal(err)
		}
	}
	shard := filepath.Join(dir, "objects", "packs", "bb")
	if err := os.Chmod(shard, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(shard, 0o700) })
	if infos, err := s.List(ctx, "packs/", "", 100); err == nil {
		t.Fatalf("List with an unreadable shard returned %d objects and no error", len(infos))
	}
}

// A permission error on link is not "already exists": a content-addressed
// caller would take ErrExists to mean the bytes are safely stored.
func TestUnwritableDirectoryIsNeverErrExists(t *testing.T) {
	requireUnprivileged(t)
	s, dir := newStore(t)
	if err := s.Put(ctx, "packs/first", strings.NewReader("x"), 1); err != nil {
		t.Fatal(err)
	}
	deny(t, filepath.Join(dir, "objects", "packs"))
	err := s.Put(ctx, "packs/second", strings.NewReader("y"), 1)
	if err == nil {
		t.Fatal("Put into an unwritable directory reported success")
	}
	if errors.Is(err, blob.ErrExists) {
		t.Fatal("Put into an unwritable directory reported ErrExists: the caller would believe it stored")
	}
	if left := entries(t, filepath.Join(dir, "tmp")); len(left) != 0 {
		t.Fatalf("the failed Put left %v in tmp/", left)
	}
}

// A Stat that cannot look is not "not found".
func TestStatPermissionErrorIsNotNotFound(t *testing.T) {
	requireUnprivileged(t)
	s, dir := newStore(t)
	if err := s.Put(ctx, "packs/x", strings.NewReader("x"), 1); err != nil {
		t.Fatal(err)
	}
	packs := filepath.Join(dir, "objects", "packs")
	if err := os.Chmod(packs, 0o600); err != nil { // no search permission
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(packs, 0o700) })
	_, err := s.Stat(ctx, "packs/x")
	if err == nil || errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("Stat without search permission = %v; want an error that is not ErrNotFound", err)
	}
}

// The swap lock is not optional: if it cannot be taken, nothing is swapped.
func TestSwapRootRefusesWithoutItsLock(t *testing.T) {
	requireUnprivileged(t)
	s, dir := newStore(t)
	v, err := s.SwapRoot(ctx, blob.NoVersion, []byte("before"))
	if err != nil {
		t.Fatal(err)
	}
	lock := filepath.Join(dir, "root.lock")
	if err := os.Chmod(lock, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(lock, 0o600) })
	if _, err := s.SwapRoot(ctx, v, []byte("after")); err == nil {
		t.Fatal("SwapRoot proceeded without its lock: two processes could swap at once")
	}
	if r, err := s.Root(ctx); err != nil || string(r.Value) != "before" {
		t.Fatalf("root after the refused swap: %q, %v", r.Value, err)
	}
}

func TestOpenReportsUnreadableMarkersAndDetectorFailures(t *testing.T) {
	requireUnprivileged(t)
	_, dir := newStore(t)
	marker := filepath.Join(dir, ".snapshot-core")
	if err := os.Chmod(marker, 0o000); err != nil {
		t.Fatal(err)
	}
	if _, err := local.Open(dir, local.Options{}); err == nil || errors.Is(err, local.ErrNotAStore) {
		t.Fatalf("Open with an unreadable marker = %v; want an error, and not \"not a store\"", err)
	}
	_ = os.Chmod(marker, 0o600)
	failing := local.WithFSType(func(string) (string, error) { return "", errors.New("statfs failed") })
	if _, err := local.Create(filepath.Join(t.TempDir(), "s"), failing); !errors.Is(err, local.ErrUnsupportedFilesystem) {
		t.Fatalf("Create when the filesystem cannot be identified = %v, want ErrUnsupportedFilesystem (fail closed)", err)
	}
	// A temp directory that cannot be read does not stop the store opening:
	// the sweep is best-effort.
	tmp := filepath.Join(dir, "tmp")
	if err := os.Chmod(tmp, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(tmp, 0o700) })
	if _, err := local.Open(dir, local.Options{}); err != nil {
		t.Fatalf("Open failed because the temp sweep could not read tmp/: %v", err)
	}
}

// A journal the filesystem will not open (here a directory where the file
// belongs) is that error, never ErrJournalBusy: a writer would wait for a
// holder that does not exist, and a publish would refuse for one (#34).
func TestAJournalThatWillNotOpenIsItsError(t *testing.T) {
	s, _ := newStore(t)
	if j, err := s.OpenJournal(ctx); err != nil {
		t.Fatalf("positive control: OpenJournal: %v", err)
	} else {
		_ = j.Close()
	}
	s2, dir2 := newStore(t)
	if err := os.Mkdir(filepath.Join(dir2, "journal"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := s2.OpenJournal(ctx); err == nil || errors.Is(err, blob.ErrJournalBusy) {
		t.Fatalf("OpenJournal where the journal is a directory = %v, want the filesystem's error", err)
	}
	if _, _, err := s2.HoldJournal(ctx); err == nil || errors.Is(err, blob.ErrJournalBusy) {
		t.Fatalf("HoldJournal where the journal is a directory = %v, want the filesystem's error", err)
	}
}
