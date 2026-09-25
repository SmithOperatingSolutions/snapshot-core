package packstore_test

import (
	"testing"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob/mem"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk/packstore"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
)

// Every read of the root refreshes, and a refresh that opened the manifest
// each time decrypted and parsed all of it, which grows with every publish:
// on memory, 70% of a sustained load's CPU at 70,000 commits. A refresh
// whose root has not moved since the store last opened it answers from
// what the store holds; one that finds it moved opens the new manifest.
func TestARefreshOfAnUnmovedRootOpensNoManifest(t *testing.T) {
	bs := mem.New()
	kr := keyring(t)
	writer, reader := open(t, bs, kr), open(t, bs, kr)
	h, err := writer.Put(ctx, []byte("first"))
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.CompareAndSetRoot(ctx, hash.Hash{}, h); err != nil {
		t.Fatal(err)
	}
	if r, err := reader.Root(ctx); err != nil || r != h {
		t.Fatalf("the reader's root = %s (%v), want %s", r.Short(), err, h.Short())
	}
	for _, s := range []struct {
		who   string
		store *packstore.Store
	}{{"the reader", reader}, {"the writer, after its own publish", writer}} {
		before := packstore.ManifestOpens(s.store)
		for range 100 {
			if r, err := s.store.Root(ctx); err != nil || r != h {
				t.Fatalf("%s reads root %s (%v), want %s", s.who, r.Short(), err, h.Short())
			}
		}
		if n := packstore.ManifestOpens(s.store) - before; n != 0 {
			t.Fatalf("100 reads of a root nobody moved: %s opened the manifest %d times, want 0: every read decrypts and parses a manifest that grows with each publish", s.who, n)
		}
	}
	// Positive control: a root that moved is read, from the new manifest.
	h2, err := writer.Put(ctx, []byte("second"))
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.CompareAndSetRoot(ctx, h, h2); err != nil {
		t.Fatal(err)
	}
	before := packstore.ManifestOpens(reader)
	if r, err := reader.Root(ctx); err != nil || r != h2 {
		t.Fatalf("after the writer's second publish the reader's root = %s (%v), want %s", r.Short(), err, h2.Short())
	}
	if n := packstore.ManifestOpens(reader) - before; n != 1 {
		t.Fatalf("reading a root that moved opened %d manifests, want 1", n)
	}
	if got, err := reader.Get(ctx, h2); err != nil || string(got) != "second" {
		t.Fatalf("the reader cannot read the chunk the moved root names: %q, %v", got, err)
	}
}
