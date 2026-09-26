package multivol_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/contract"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/internal/crashtest"
	jcontract "github.com/SmithOperatingSolutions/snapshot-core/core/blob/journal/contract"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/multivol"
)

var ctx = context.Background()

type fixture struct {
	primary string
	vols    []string // all volume paths, primary first
}

func newFixture(t *testing.T, n int) (*multivol.Store, fixture) {
	t.Helper()
	root := t.TempDir()
	var f fixture
	for i := 0; i < n; i++ {
		f.vols = append(f.vols, filepath.Join(root, fmt.Sprintf("vol%d", i)))
	}
	f.primary = f.vols[0]
	s, err := multivol.Create(f.primary, f.vols[1:], multivol.Options{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	return s, f
}

func put(t *testing.T, s blob.BlobStore, name string, b []byte) {
	t.Helper()
	if err := s.Put(ctx, name, bytes.NewReader(b), int64(len(b))); err != nil {
		t.Fatalf("Put(%q): %v", name, err)
	}
}

func read(t *testing.T, s blob.BlobStore, name string) ([]byte, error) {
	t.Helper()
	rc, err := s.Get(ctx, name, 0, -1)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

// holder returns which volume (index) physically holds an object.
func (f fixture) holder(t *testing.T, name string) int {
	t.Helper()
	found := -1
	for i, v := range f.vols {
		p := filepath.Join(v, "vol", "objects", filepath.FromSlash(name)+"~")
		if _, err := os.Stat(p); err == nil {
			if found >= 0 {
				t.Fatalf("%s is stored on volumes %d and %d", name, found, i)
			}
			found = i
		}
	}
	return found
}

func packName(i int) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("pack-%d", i)))
	return "packs/" + hex.EncodeToString(h[:])
}

func TestContract(t *testing.T) {
	contract.Run(t, func(t *testing.T) blob.BlobStore { s, _ := newFixture(t, 3); return s }, contract.Options{})
}

func volumeIDs(n int) []string {
	var ids []string
	for i := 0; i < n; i++ {
		h := sha256.Sum256([]byte(fmt.Sprintf("volume-%d", i)))
		ids = append(ids, hex.EncodeToString(h[:16]))
	}
	return ids
}

// Engine Spec: "with 4 volumes and 10,000 table files, each volume holds 25% ± 3%".
func TestPlacementIsEvenAcrossFourVolumes(t *testing.T) {
	var names []string
	for i := 0; i < 10000; i++ {
		names = append(names, packName(i))
	}
	counts := make([]int, 4)
	for _, v := range multivol.Place(names, volumeIDs(4)) {
		if v < 0 || v >= 4 {
			t.Fatalf("Place returned volume %d of 4", v)
		}
		counts[v]++
	}
	for i, c := range counts {
		if share := float64(c) / 100; math.Abs(share-25) > 3 {
			t.Errorf("volume %d holds %.1f%% of 10,000 objects, want 25%% ± 3%%: %v", i, share, counts)
		}
	}
}

// Engine Spec: "adding a 5th volume moves no existing files and sends about
// 20% of new files to it".
func TestAddingAVolumeMovesNothingAndTakesAFifth(t *testing.T) {
	var names []string
	for i := 0; i < 10000; i++ {
		names = append(names, packName(i))
	}
	four := multivol.Place(names, volumeIDs(4))
	five := multivol.Place(names, volumeIDs(5))
	toNew, moved := 0, 0
	for i := range names {
		switch {
		case five[i] == 4:
			toNew++
		case five[i] != four[i]:
			moved++
		}
	}
	if share := float64(toNew) / 100; math.Abs(share-20) > 3 {
		t.Errorf("the 5th volume takes %.1f%% of new placements, want about 20%%", share)
	}
	if moved != 0 {
		t.Errorf("%d names changed volume between two OLD volumes when a 5th was added: rendezvous hashing "+
			"must only ever move names to the new volume", moved)
	}

	// End to end: stored objects stay where they are and stay readable.
	s, f := newFixture(t, 4)
	where := map[string]int{}
	for i := 0; i < 120; i++ {
		n := packName(100000 + i)
		put(t, s, n, []byte(n))
		where[n] = f.holder(t, n)
	}
	fifth := filepath.Join(filepath.Dir(f.primary), "vol4")
	if err := s.AddVolume(fifth); err != nil {
		t.Fatalf("AddVolume: %v", err)
	}
	f.vols = append(f.vols, fifth)
	for n, v := range where {
		if got := f.holder(t, n); got != v {
			t.Fatalf("%s moved from volume %d to %d when a volume was added", n, v, got)
		}
		if b, err := read(t, s, n); err != nil || string(b) != n {
			t.Fatalf("%s unreadable after adding a volume: %v", n, err)
		}
	}
	onNew := 0
	for i := 0; i < 200; i++ {
		n := packName(200000 + i)
		put(t, s, n, []byte(n))
		if f.holder(t, n) == 4 {
			onNew++
		}
	}
	if onNew < 20 || onNew > 60 {
		t.Errorf("%d of 200 new objects landed on the added volume, want about 40", onNew)
	}
	// Reopen: the map records the new volume.
	re, err := multivol.Open(f.primary, multivol.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(re.VolumeIDs()) != 5 {
		t.Fatalf("after reopen the store has %d volumes, want 5", len(re.VolumeIDs()))
	}
}

// A name already stored on an old volume must not be stored again on the new
// volume that now ranks first for it.
func TestPutRefusesANameStoredOnAnotherVolume(t *testing.T) {
	s, f := newFixture(t, 2)
	var names []string
	for i := 0; i < 60; i++ {
		n := packName(300000 + i)
		put(t, s, n, []byte("original"))
		names = append(names, n)
	}
	third := filepath.Join(filepath.Dir(f.primary), "vol2")
	if err := s.AddVolume(third); err != nil {
		t.Fatal(err)
	}
	ids := s.VolumeIDs()
	ranked := multivol.Place(names, ids)
	tested := 0
	for i, n := range names {
		if ranked[i] != 2 {
			continue
		}
		tested++
		if err := s.Put(ctx, n, strings.NewReader("replacement"), 11); !errors.Is(err, blob.ErrExists) {
			t.Fatalf("Put of %s (stored on an old volume, now ranked on the new one) = %v, want ErrExists", n, err)
		}
		if b, _ := read(t, s, n); string(b) != "original" {
			t.Fatalf("%s reads %q after the refused Put", n, b)
		}
	}
	if tested == 0 {
		t.Fatal("no stored name ranked on the new volume; the fixture tests nothing")
	}
}

// snapshot lists every file under the volumes, for "no writes land".
func snapshot(t *testing.T, f fixture) string {
	t.Helper()
	var lines []string
	for _, v := range f.vols {
		_ = filepath.WalkDir(v, func(p string, d fs.DirEntry, err error) error {
			if err == nil && !d.IsDir() {
				info, _ := d.Info()
				lines = append(lines, fmt.Sprintf("%s %d", p, info.Size()))
			}
			return nil
		})
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

// unmount simulates an unmounted volume: the mount point is an empty
// directory on the parent filesystem.
func unmount(t *testing.T, path string) {
	t.Helper()
	if err := os.Rename(path, path+".unmounted"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
}

// Engine Spec: "unmounting a secondary volume makes the store read-only with
// ErrVolumeMissing; no writes land".
func TestMissingSecondaryMakesTheStoreReadOnly(t *testing.T) {
	s, f := newFixture(t, 3)
	byVol := map[int]string{}
	for i := 0; len(byVol) < 3 && i < 200; i++ {
		n := packName(400000 + i)
		put(t, s, n, []byte(n))
		byVol[f.holder(t, n)] = n
	}
	if len(byVol) < 3 {
		t.Fatal("could not place an object on every volume")
	}
	v, err := s.SwapRoot(ctx, blob.NoVersion, []byte("root before"))
	if err != nil {
		t.Fatal(err)
	}

	unmount(t, f.vols[2])
	check := func(label string, st *multivol.Store) {
		t.Helper()
		before := snapshot(t, f)
		if err := st.Put(ctx, packName(999999), strings.NewReader("x"), 1); !errors.Is(err, multivol.ErrVolumeMissing) || !errors.Is(err, blob.ErrReadOnly) {
			t.Errorf("%s: Put = %v, want ErrVolumeMissing (a read-only error)", label, err)
		}
		if _, err := st.SwapRoot(ctx, v, []byte("root after")); !errors.Is(err, multivol.ErrVolumeMissing) {
			t.Errorf("%s: SwapRoot = %v, want ErrVolumeMissing", label, err)
		}
		if err := st.Delete(ctx, byVol[0]); !errors.Is(err, multivol.ErrVolumeMissing) {
			t.Errorf("%s: Delete = %v, want ErrVolumeMissing", label, err)
		}
		if got := snapshot(t, f); got != before {
			t.Errorf("%s: writes landed on a read-only store", label)
		}
		if b, err := read(t, st, byVol[0]); err != nil || string(b) != byVol[0] {
			t.Errorf("%s: an object on a present volume is unreadable: %v", label, err)
		}
		if _, err := read(t, st, byVol[2]); !errors.Is(err, multivol.ErrVolumeMissing) {
			t.Errorf("%s: an object on the missing volume reads as %v, want ErrVolumeMissing (not \"not found\")", label, err)
		}
		if _, err := st.List(ctx, "", "", blob.MaxListPage); !errors.Is(err, multivol.ErrVolumeMissing) {
			t.Errorf("%s: List = %v, want ErrVolumeMissing: an incomplete listing would let GC think packs are gone", label, err)
		}
		r, err := st.Root(ctx)
		if err != nil || string(r.Value) != "root before" {
			t.Errorf("%s: Root = %q, %v", label, r.Value, err)
		}
		if !st.ReadOnly() {
			t.Errorf("%s: ReadOnly() = false", label)
		}
	}
	check("detected on the next operation", s)
	re, err := multivol.Open(f.primary, multivol.Options{})
	if err != nil {
		t.Fatalf("Open with a secondary missing must open read-only, got %v", err)
	}
	check("detected at open", re)
}

// Engine Spec: "a volume whose marker UUID doesn't match is refused".
func TestSwappedVolumeIsRefused(t *testing.T) {
	_, a := newFixture(t, 2)
	_, b := newFixture(t, 2)
	if _, err := multivol.Open(a.primary, multivol.Options{}); err != nil {
		t.Fatalf("positive control: %v", err)
	}
	if err := os.Rename(a.vols[1], a.vols[1]+".real"); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(b.vols[1], a.vols[1]); err != nil {
		t.Fatal(err)
	}
	if _, err := multivol.Open(a.primary, multivol.Options{}); !errors.Is(err, multivol.ErrVolumeMismatch) {
		t.Fatalf("Open with another store's volume mounted in place = %v, want ErrVolumeMismatch", err)
	}
}

func TestPrimaryMissingRefusesOpen(t *testing.T) {
	_, f := newFixture(t, 2)
	unmount(t, f.primary)
	if _, err := multivol.Open(f.primary, multivol.Options{}); !errors.Is(err, multivol.ErrNotAStore) {
		t.Fatalf("Open with the primary unmounted = %v, want ErrNotAStore", err)
	}
}

func TestFreeSpaceFloor(t *testing.T) {
	_, f := newFixture(t, 3)
	full := map[string]bool{f.vols[1]: true}
	probe := func(path string) (uint64, uint64, error) {
		for v := range full {
			if strings.HasPrefix(path, v) {
				return 4, 100, nil // 4% free: under the 5% floor
			}
		}
		return 50, 100, nil
	}
	s, err := multivol.Open(f.primary, multivol.WithFreeSpace(multivol.Options{}, probe))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 90; i++ {
		n := packName(500000 + i)
		put(t, s, n, []byte(n))
		if f.holder(t, n) == 1 {
			t.Fatalf("%s landed on a volume below its free-space floor", n)
		}
	}
	full[f.vols[0]], full[f.vols[2]] = true, true
	if err := s.Put(ctx, packName(600000), strings.NewReader("x"), 1); !errors.Is(err, multivol.ErrNoSpace) {
		t.Fatalf("Put with every volume under the floor = %v, want ErrNoSpace", err)
	}
	if b, err := read(t, s, packName(500000)); err != nil || string(b) != packName(500000) {
		t.Fatalf("reads must keep working when volumes are full: %v", err)
	}
}

func TestVolumeLimit(t *testing.T) {
	root := t.TempDir()
	var secs []string
	for i := 0; i < multivol.MaxVolumes; i++ { // primary + 64 = 65
		secs = append(secs, filepath.Join(root, fmt.Sprintf("s%d", i)))
	}
	if _, err := multivol.Create(filepath.Join(root, "p"), secs, multivol.Options{}); !errors.Is(err, multivol.ErrTooManyVolumes) {
		t.Fatalf("Create with 65 volumes = %v, want ErrTooManyVolumes", err)
	}
	s, err := multivol.Create(filepath.Join(root, "q"), secs[:multivol.MaxVolumes-1], multivol.Options{})
	if err != nil {
		t.Fatalf("positive control: 64 volumes: %v", err)
	}
	if err := s.AddVolume(filepath.Join(root, "one-too-many")); !errors.Is(err, multivol.ErrTooManyVolumes) {
		t.Fatalf("AddVolume past 64 = %v, want ErrTooManyVolumes", err)
	}
}

func openStore(dir string) (blob.BlobStore, error) { return multivol.Open(dir, multivol.Options{}) }

// TestMain doubles as the crash harness's child process.
func TestMain(m *testing.M) { crashtest.Main(m, openStore) }

// Engine Spec: "kill -9 during commit; reopened store is at the old or new root".
func TestCrashDuringSwapRootLeavesOldOrNew(t *testing.T) {
	_, f := newFixture(t, 3)
	crashtest.SwapRoot(t, f.primary, openStore)
}

func TestCrashDuringPutLeavesNothingPartial(t *testing.T) {
	_, f := newFixture(t, 3)
	crashtest.Put(t, f.primary, openStore)
}

// A volume swapped for another store's while the store is open has a vol/
// directory, so only its identity gives it away. The store must go read-only
// rather than serve the other store's objects or report ours as not found.
func TestVolumeSwappedWhileOpenGoesReadOnly(t *testing.T) {
	s, a := newFixture(t, 2)
	_, b := newFixture(t, 2)
	var onSecondary string
	for i := 0; i < 200 && onSecondary == ""; i++ {
		n := packName(700000 + i)
		put(t, s, n, []byte(n))
		if a.holder(t, n) == 1 {
			onSecondary = n
		}
	}
	if onSecondary == "" {
		t.Fatal("no object landed on the secondary")
	}
	if got, err := read(t, s, onSecondary); err != nil || string(got) != onSecondary {
		t.Fatalf("positive control: %v", err)
	}
	if err := os.Rename(a.vols[1], a.vols[1]+".real"); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(b.vols[1], a.vols[1]); err != nil {
		t.Fatal(err)
	}
	if _, err := read(t, s, onSecondary); !errors.Is(err, multivol.ErrVolumeMissing) {
		t.Fatalf("after the secondary was swapped, reading its object = %v, want ErrVolumeMissing", err)
	}
	if err := s.Put(ctx, packName(800000), strings.NewReader("x"), 1); !errors.Is(err, multivol.ErrVolumeMissing) {
		t.Fatalf("Put after a swap = %v, want ErrVolumeMissing", err)
	}
}

// The journal's contract (#34): it lives on the primary volume, beside the
// root.
func TestJournalContract(t *testing.T) {
	jcontract.Run(t, func(t *testing.T) (blob.Journaler, func(*testing.T) blob.Journaler) {
		s, f := newFixture(t, 3)
		return s, func(t *testing.T) blob.Journaler {
			re, err := multivol.Open(f.primary, multivol.Options{})
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			return re
		}
	})
}

func TestCrashDuringJournalAppendKeepsWhatReturned(t *testing.T) {
	_, f := newFixture(t, 3)
	crashtest.Append(t, f.primary, openStore)
}

// A store with a volume missing is read-only, and a journal it opened
// would take commits no publish could land (#34): it opens none.
func TestMissingSecondaryOpensNoJournal(t *testing.T) {
	s, f := newFixture(t, 3)
	j, err := s.OpenJournal(ctx)
	if err != nil {
		t.Fatalf("positive control: OpenJournal with every volume present: %v", err)
	}
	_ = j.Close()
	unmount(t, f.vols[2])
	if j, err := s.OpenJournal(ctx); !errors.Is(err, multivol.ErrVolumeMissing) {
		if err == nil {
			_ = j.Close()
		}
		t.Fatalf("OpenJournal with a volume missing = %v, want ErrVolumeMissing: commits would be journaled that cannot be published", err)
	}
}
