package packstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/dedup"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/pack"
)

// ErrMoved is Apply's answer when the manifest changed after Begin: a writer
// published, the round's mark is stale, and a new round must start.
var ErrMoved = errors.New("packstore: the manifest moved during the GC round")

// Round is one GC decision taken against one manifest (docs/DESIGN.md §9):
// Begin reads it, GC marks from its root, and Apply condemns, reprieves and
// expires against it, losing to any writer that published in between.
type Round struct {
	o       Options
	man     manifest
	ver     blob.Version
	packs   []pack.Info // every pack the manifest's index objects list
	orphans []string
}

// Orphans hands the round the packs and index objects GC found older than
// the grace window by the backend's clock. Apply records each its manifest
// does not name as deleted and returns them in Outcome.Orphans, for the GC
// role to delete once the swap has landed (DESIGN §9).
func (r *Round) Orphans(names []string) { r.orphans = names }

// Begin reads the manifest and every index object it lists.
func Begin(ctx context.Context, o Options) (*Round, error) {
	if o.Blobs == nil || o.Keys == nil {
		return nil, errors.New("packstore: Blobs and Keys are required")
	}
	root, err := o.Blobs.Root(ctx)
	if err != nil {
		return nil, err
	}
	r := &Round{o: o, ver: root.Version}
	if root.Version == blob.NoVersion {
		return r, nil
	}
	if r.man, err = openManifest(root.Value, o.Keys, o.Repo); err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for _, sum := range r.man.indexes {
		infos, err := loadIndex(ctx, o, sum)
		if err != nil {
			return nil, err
		}
		for _, p := range infos {
			if !seen[p.Name] {
				seen[p.Name] = true
				r.packs = append(r.packs, p)
			}
		}
	}
	return r, nil
}

// Root is the refs root GC marks from.
func (r *Round) Root() hash.Hash { return r.man.root }

// Outcome is what a round did.
type Outcome struct {
	Condemned, Reprieved int             // packs
	Expired              []string        // packs and index objects the manifest no longer names: the GC role deletes them
	Named                map[string]bool // every pack and index object the manifest now names, live or condemned
	Orphans              []string        // packs and index objects no manifest names, recorded as deleted: the GC role deletes them
}

// Apply decides against the round's manifest and swaps it: a pack none of
// whose chunks is live is condemned at now; a condemned pack with a live
// chunk is reprieved; one condemned at least grace before now and still
// unmarked expires, as does an index object condemned that long ago.
// Expired packs leave the index objects, which are rewritten (and the
// replaced ones condemned), and gcGen moves. A live set without the root is
// refused: marking went wrong, and nothing is decided on it.
func (r *Round) Apply(ctx context.Context, live func(hash.Hash) bool, now time.Time, grace time.Duration) (Outcome, error) {
	out := Outcome{Named: map[string]bool{}}
	if r.ver == blob.NoVersion {
		// No manifest: nothing to record in, and every object is an orphan.
		out.Orphans = append(out.Orphans, r.orphans...)
		return out, nil
	}
	if !live(r.man.root) {
		return Outcome{}, fmt.Errorf("packstore: the root %s is not in the live set", r.man.root.Short())
	}
	at := now.UnixNano()
	expired := func(c condemned) bool { return at-c.at >= int64(grace) }
	packs := map[string]condemned{}
	var next []condemned
	recorded := map[string]bool{} // orphans already recorded as deleted
	lapsed := 0
	for _, c := range r.man.condemned {
		switch {
		case c.kind == condemnedPack:
			packs[dedup.PackName(c.sum)] = c
		case c.kind == deletedPack || c.kind == deletedIndex:
			if at-c.at >= int64(grace+recheckAfter) { // every writer has looked its uploads up by now
				lapsed++
				continue
			}
			next = append(next, c)
			recorded[orphanName(c)] = true
		case expired(c):
			out.Expired = append(out.Expired, indexName(c.sum))
		default:
			next = append(next, c)
		}
	}
	var keep []pack.Info
	gone := 0
	for _, p := range r.packs {
		c, was := packs[p.Name]
		switch {
		case isLive(p, live):
			keep = append(keep, p)
			if was {
				out.Reprieved++
			}
		case was && expired(c):
			out.Expired = append(out.Expired, p.Name)
			gone++
		case was:
			keep = append(keep, p)
			next = append(next, c)
		case len(next) < maxCondemned/2: // the rest wait for a later round
			sum, err := dedup.PackSum(p.Name)
			if err != nil {
				return Outcome{}, err
			}
			keep = append(keep, p)
			next = append(next, condemned{kind: condemnedPack, sum: sum, at: at})
			out.Condemned++
		default:
			keep = append(keep, p)
		}
	}
	upd := r.man
	upd.seq++
	if gone > 0 {
		sums, err := writeIndexes(ctx, r.o, keep)
		if err != nil {
			return Outcome{}, err
		}
		for _, old := range r.man.indexes {
			if !containsSum(sums, old) {
				next = append(next, condemned{kind: condemnedIndex, sum: old, at: at})
			}
		}
		upd.indexes = sums
		upd.gcGen++
	}
	for _, p := range keep {
		out.Named[p.Name] = true
	}
	for _, s := range upd.indexes {
		out.Named[indexName(s)] = true
	}
	for _, c := range next {
		if c.kind == condemnedIndex {
			out.Named[indexName(c.sum)] = true
		}
	}
	expiring := map[string]bool{}
	for _, name := range out.Expired {
		expiring[name] = true
	}
	added := 0
	for _, name := range r.orphans {
		if out.Named[name] || expiring[name] {
			continue
		}
		// A name no pack or index object has cannot be a writer's upload:
		// it goes unrecorded.
		if c, ok := deletion(name, at); ok && !recorded[name] {
			if len(next) >= maxCondemned { // the rest wait for a later round
				break
			}
			next = append(next, c)
			recorded[name] = true
			added++
		}
		out.Orphans = append(out.Orphans, name)
	}
	upd.condemned = next
	if out.Condemned == 0 && out.Reprieved == 0 && len(out.Expired) == 0 && added == 0 && lapsed == 0 {
		return out, nil
	}
	sealed, err := upd.seal(r.o.Keys, r.o.Repo)
	if err != nil {
		return Outcome{}, err
	}
	if _, err := r.o.Blobs.SwapRoot(ctx, r.ver, sealed); err != nil {
		if errors.Is(err, blob.ErrRootConflict) {
			return Outcome{}, ErrMoved
		}
		return Outcome{}, err
	}
	return out, nil
}

// deletion is the record of deleting name, a pack or an index object, as an
// orphan at at; ok is false for any other name.
func deletion(name string, at int64) (condemned, bool) {
	if sum, err := dedup.PackSum(name); err == nil {
		return condemned{kind: deletedPack, sum: sum, at: at}, true
	}
	if sum, err := indexSum(name); err == nil {
		return condemned{kind: deletedIndex, sum: sum, at: at}, true
	}
	return condemned{}, false
}

// orphanName is the object a deletion record names.
func orphanName(c condemned) string {
	if c.kind == deletedPack {
		return dedup.PackName(c.sum)
	}
	return indexName(c.sum)
}

func isLive(p pack.Info, live func(hash.Hash) bool) bool {
	for _, e := range p.Entries {
		if live(e.Hash) {
			return true
		}
	}
	return false
}

func containsSum(sums [][32]byte, s [32]byte) bool {
	for _, x := range sums {
		if x == s {
			return true
		}
	}
	return false
}

// writeIndexes stores packs as index objects, each within dedup's limits,
// and returns their sums in order.
func writeIndexes(ctx context.Context, o Options, packs []pack.Info) ([][32]byte, error) {
	var sums [][32]byte
	for len(packs) > 0 {
		n, size := 0, 0
		for n < len(packs) && n < dedup.MaxPacksPerObject && (n == 0 || size+estimate(packs[n]) <= dedup.MaxObjectSize/2) {
			size += estimate(packs[n])
			n++
		}
		name, b, err := dedup.EncodeObject(o.Keys, o.Repo, packs[:n])
		if err != nil {
			return nil, err
		}
		if err := o.Blobs.Put(ctx, name, bytes.NewReader(b), int64(len(b))); err != nil && !errors.Is(err, blob.ErrExists) {
			return nil, fmt.Errorf("packstore: writing index object: %w", err)
		}
		sum, err := indexSum(name)
		if err != nil {
			return nil, err
		}
		sums = append(sums, sum)
		packs = packs[n:]
	}
	sort.Slice(sums, func(i, j int) bool { return bytes.Compare(sums[i][:], sums[j][:]) < 0 })
	return sums, nil
}

// estimate bounds a pack's share of an index object.
func estimate(p pack.Info) int { return 128 + 64*len(p.Entries) }
