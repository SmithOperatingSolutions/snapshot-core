//go:build unix

package multivol_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/multivol"
)

func requireUnprivileged(t *testing.T) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Fatal("fault-injection tests deny access with chmod, which root ignores: run the suite as an ordinary user")
	}
}

// mapFile builds a volume map in the documented format with a valid checksum
// over whatever it is given, so a forgery is refused on content, not the sum.
func mapFile(magic string, version, count uint16, ids [][16]byte, paths []string) []byte {
	var b []byte
	b = append(b, magic...)
	b = binary.LittleEndian.AppendUint16(b, version)
	b = binary.LittleEndian.AppendUint16(b, count)
	for i := range ids {
		b = append(b, ids[i][:]...)
		b = binary.AppendUvarint(b, uint64(len(paths[i])))
		b = append(b, paths[i]...)
	}
	sum := sha256.Sum256(b)
	return append(b, sum[:]...)
}

func TestCorruptOrForgedVolumeMapIsRefused(t *testing.T) {
	_, f := newFixture(t, 2)
	path := filepath.Join(f.primary, "multivol.map")
	good, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	write := func(b []byte) {
		t.Helper()
		if err := os.WriteFile(path, b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	flipped := bytes.Clone(good)
	flipped[len(flipped)/2] ^= 1
	var id [16]byte
	for name, b := range map[string][]byte{
		"flipped byte":  flipped,
		"truncated":     good[:len(good)-1],
		"tiny":          []byte("x"),
		"wrong magic":   mapFile("XXXX", 1, 1, [][16]byte{id}, []string{"/v"}),
		"wrong version": mapFile("SCMV", 2, 1, [][16]byte{id}, []string{"/v"}),
		"zero volumes":  mapFile("SCMV", 1, 0, nil, nil),
		"65 volumes":    mapFile("SCMV", 1, 65, nil, nil),
		"count lies":    mapFile("SCMV", 1, 2, [][16]byte{id}, []string{"/v"}),
	} {
		write(b)
		if _, err := multivol.Open(f.primary, multivol.Options{}); !errors.Is(err, multivol.ErrCorrupt) {
			t.Errorf("%s: Open = %v, want ErrCorrupt", name, err)
		}
	}
	write(good)
	if _, err := multivol.Open(f.primary, multivol.Options{}); err != nil {
		t.Fatalf("positive control: the original map does not open: %v", err)
	}
}

// Create must never overwrite an existing volume map, even when the primary's
// vol/ is gone (a half-deleted store): the map is how its other volumes are
// found.
func TestCreateNeverOverwritesAVolumeMap(t *testing.T) {
	_, f := newFixture(t, 2)
	mapPath := filepath.Join(f.primary, "multivol.map")
	before, err := os.ReadFile(mapPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(f.primary, "vol")); err != nil {
		t.Fatal(err)
	}
	if _, err := multivol.Create(f.primary, nil, multivol.Options{}); err == nil {
		t.Fatal("Create over a primary that still holds a volume map reported success")
	}
	if after, _ := os.ReadFile(mapPath); !bytes.Equal(after, before) {
		t.Fatal("a refused Create rewrote the volume map")
	}
}

func TestAddVolumeWhenReadOnlyOrFailingChangesNothing(t *testing.T) {
	requireUnprivileged(t)
	s, f := newFixture(t, 2)
	parent := filepath.Dir(f.primary)
	locked := filepath.Join(parent, "locked")
	if err := os.Mkdir(locked, 0o500); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(locked, 0o700)
	if err := s.AddVolume(filepath.Join(locked, "vol")); err == nil {
		t.Fatal("AddVolume on an unwritable path reported success")
	}
	if n := len(s.VolumeIDs()); n != 2 {
		t.Fatalf("after a failed AddVolume the store has %d volumes, want 2", n)
	}
	if re, err := multivol.Open(f.primary, multivol.Options{}); err != nil || len(re.VolumeIDs()) != 2 {
		t.Fatalf("after a failed AddVolume the map changed (err=%v)", err)
	}
	// The new volume is created, but the map cannot be written (the primary
	// is read-only): the volume must not stay in the store's list.
	if err := s.AddVolume(filepath.Join(parent, "vol7")); err != nil {
		t.Fatalf("positive control: %v", err)
	}
	if err := os.Chmod(f.primary, 0o500); err != nil {
		t.Fatal(err)
	}
	if err := s.AddVolume(filepath.Join(parent, "vol8")); err == nil {
		t.Fatal("AddVolume whose map write failed reported success")
	}
	if err := os.Chmod(f.primary, 0o700); err != nil {
		t.Fatal(err)
	}
	if n := len(s.VolumeIDs()); n != 3 {
		t.Fatalf("after a failed map write the store lists %d volumes, want 3: objects would be placed on a "+
			"volume no reopen can find", n)
	}
	unmount(t, f.vols[1])
	if err := s.AddVolume(filepath.Join(parent, "vol9")); !errors.Is(err, multivol.ErrVolumeMissing) {
		t.Fatalf("AddVolume on a read-only store = %v, want ErrVolumeMissing", err)
	}
}

func TestCustomFloorIsHonored(t *testing.T) {
	_, f := newFixture(t, 2)
	half := func(string) (uint64, uint64, error) { return 50, 100, nil }
	strict, err := multivol.Open(f.primary, multivol.WithFreeSpace(multivol.Options{Floor: 0.6}, half))
	if err != nil {
		t.Fatal(err)
	}
	if err := strict.Put(ctx, packName(1), strings.NewReader("x"), 1); !errors.Is(err, multivol.ErrNoSpace) {
		t.Fatalf("with a 60%% floor and 50%% free, Put = %v, want ErrNoSpace", err)
	}
	lax, err := multivol.Open(f.primary, multivol.WithFreeSpace(multivol.Options{Floor: 0.4}, half))
	if err != nil {
		t.Fatal(err)
	}
	if err := lax.Put(ctx, packName(1), strings.NewReader("x"), 1); err != nil {
		t.Fatalf("with a 40%% floor and 50%% free, Put = %v", err)
	}
}

func TestPrimaryVanishingWhileOpen(t *testing.T) {
	s, f := newFixture(t, 2)
	if _, err := s.SwapRoot(ctx, blob.NoVersion, []byte("root")); err != nil {
		t.Fatal(err)
	}
	unmount(t, f.primary)
	if r, err := s.Root(ctx); !errors.Is(err, multivol.ErrVolumeMissing) {
		t.Fatalf("Root with the primary gone = %q, %v; want ErrVolumeMissing", r.Value, err)
	}
}

func FuzzDecodeMap(f *testing.F) {
	good, err := multivol.EncodeMap([][2]string{{"00112233445566778899aabbccddeeff", "/mnt/a"}})
	if err == nil {
		f.Add(good)
	}
	f.Add([]byte("SCMV"))
	f.Fuzz(func(t *testing.T, b []byte) {
		entries, err := multivol.DecodeMap(b)
		if err != nil {
			return
		}
		again, err := multivol.EncodeMap(entries)
		if err != nil || !bytes.Equal(again, b) {
			t.Fatalf("decoded a volume map that does not re-encode to itself (%v)", err)
		}
	})
}
