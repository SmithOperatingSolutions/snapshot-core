package packstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/SmithOperatingSolutions/snapshot-core/core/blob"
	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/dedup"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/pack"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
)

// ErrMoved is Apply's answer when the manifest changed after Begin: a writer
// published, the round's mark is stale, and a new round must start.
var ErrMoved = errors.New("packstore: the manifest moved during the GC round")

// Repack is a round's policy for rewriting packs that are mostly dead
// (docs/DESIGN.md §9): a kept pack whose live bytes are under MaxLive of
// its size is a candidate, and candidates are repacked emptiest first until
// Budget bytes have been copied. The zero value is the default policy: half,
// and a GiB per round.
type Repack struct {
	MaxLive float64 // 0: 0.5
	Budget  int64   // bytes copied per round; 0: 1 GiB
	Off     bool
}

// Repack defaults.
const (
	DefaultMaxLive = 0.5
	DefaultBudget  = 1 << 30
)

// Live is the set of chunks GC marked, which a round decides against. It
// is sorted, so a round joins it against its own index in one pass.
type Live interface {
	Has(h hash.Hash) (bool, error)
	Len() int64
	Each(func(h hash.Hash) error) error // in ascending order
}

// Round is one GC decision taken against one manifest (docs/DESIGN.md §9):
// Begin reads it, GC marks from its root, and Apply condemns, reprieves and
// expires against it, losing to any writer that published in between. Its
// memory is a record per pack, none per chunk (#6): the chunks are in a
// table under Options.IndexDir, the packs in service first.
type Round struct {
	o       Options
	man     manifest
	ver     blob.Version
	packs   []packSummary // every pack the manifest's index objects list
	index   *spilled      // chunk hash to location; nil when no pack is listed
	orphans []string
	repack  Repack
}

// packSummary is what a round keeps of a pack.
type packSummary struct {
	name    string
	salt    seal.Salt
	size    int64
	entries int
	object  [32]byte // the index object listing it
}

// Repack sets the round's repacking policy (the zero value is the default).
func (r *Round) Repack(p Repack) { r.repack = p }

// Orphans hands the round the packs and index objects GC found older than
// the grace window by the backend's clock. Apply records each its manifest
// does not name as deleted and returns them in Outcome.Orphans, for the GC
// role to delete once the swap has landed (DESIGN §9).
func (r *Round) Orphans(names []string) { r.orphans = names }

// Begin reads the manifest and every index object it lists, indexing every
// pack's chunks on disk.
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
	if err := r.load(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

// load indexes every pack the manifest lists: those in service in a first
// pass over the index objects, the condemned and repacked in a second over
// the objects that list them, so a chunk two packs hold counts for the one
// staying.
func (r *Round) load(ctx context.Context) error {
	b, err := dedup.NewBuilder(r.o.IndexDir, locLen)
	if err != nil {
		return err
	}
	cond := condemnedPacks(r.man)
	sp := &spilled{}
	seen := map[string]bool{}
	add := func(info pack.Info, object [32]byte) error {
		sum, err := dedup.PackSum(info.Name)
		if err != nil {
			return err
		}
		seen[info.Name] = true
		pi := uint32(len(r.packs))
		r.packs = append(r.packs, packSummary{name: info.Name, salt: info.Salt, size: info.Size, entries: len(info.Entries), object: object})
		sp.packs = append(sp.packs, packRef{sum: sum, salt: info.Salt, size: info.Size})
		for _, e := range info.Entries {
			if err := b.Add(e.Hash, encodeLoc(pi, e)); err != nil {
				return err
			}
		}
		return nil
	}
	var later [][32]byte
	for _, sum := range r.man.indexes {
		infos, err := loadIndex(ctx, r.o, sum)
		if err != nil {
			b.Abort()
			return err
		}
		listsCondemned := false
		for _, info := range infos {
			switch {
			case seen[info.Name]:
			case cond[info.Name]:
				listsCondemned = true
			default:
				if err := add(info, sum); err != nil {
					b.Abort()
					return err
				}
			}
		}
		if listsCondemned {
			later = append(later, sum)
		}
	}
	for _, sum := range later {
		infos, err := loadIndex(ctx, r.o, sum)
		if err != nil {
			b.Abort()
			return err
		}
		for _, info := range infos {
			if !seen[info.Name] {
				if err := add(info, sum); err != nil {
					b.Abort()
					return err
				}
			}
		}
	}
	if sp.table, err = b.Finish(); err != nil {
		return fmt.Errorf("packstore: indexing the round in %s: %w", r.o.IndexDir, err)
	}
	r.index = sp
	return nil
}

// Root is the refs root GC marks from.
func (r *Round) Root() hash.Hash { return r.man.root }

// Close removes the round's index. Apply closes the round itself.
func (r *Round) Close() error {
	if r.index == nil {
		return nil
	}
	err := r.index.close()
	r.index = nil
	return err
}

// Outcome is what a round did.
type Outcome struct {
	Condemned, Reprieved int             // packs
	Expired              []string        // packs and index objects the manifest no longer names: the GC role deletes them
	Named                map[string]bool // every pack and index object the manifest now names, live or condemned
	Orphans              []string        // packs and index objects no manifest names, recorded as deleted: the GC role deletes them
	Repacked             int             // packs whose live chunks were copied into new packs
	Copied               int64           // bytes of frames copied by repacking
}

// Apply decides against the round's manifest and swaps it: a pack none of
// whose chunks is live is condemned at now; a condemned pack with a live
// chunk is reprieved; one condemned at least grace before now and still
// unmarked expires, as does an index object condemned that long ago.
// Expired packs leave the index objects, which are rewritten (and the
// replaced ones condemned), and gcGen moves. A live set without the root is
// refused: marking went wrong, and nothing is decided on it. The round is
// closed either way.
func (r *Round) Apply(ctx context.Context, live Live, now time.Time, grace time.Duration) (Outcome, error) {
	defer r.Close()
	out := Outcome{Named: map[string]bool{}}
	if r.ver == blob.NoVersion {
		// No manifest: nothing to record in, and every object is an orphan.
		out.Orphans = append(out.Orphans, r.orphans...)
		return out, nil
	}
	if ok, err := live.Has(r.man.root); err != nil {
		return Outcome{}, err
	} else if !ok {
		return Outcome{}, fmt.Errorf("packstore: the root %s is not in the live set", r.man.root.Short())
	}
	at := now.UnixNano()
	expired := func(c condemned) bool { return at-c.at >= int64(grace) }
	policy, err := r.repack.resolved()
	if err != nil {
		return Outcome{}, err
	}
	liveCount, liveBytes, err := r.join(live)
	if err != nil {
		return Outcome{}, err
	}
	packs := map[string]condemned{}
	repacked := map[string]condemned{} // packs whose live chunks are already in new packs
	var next []condemned
	recorded := map[string]bool{} // orphans already recorded as deleted
	lapsed := 0
	for _, c := range r.man.condemned {
		switch {
		case c.kind == condemnedPack:
			packs[dedup.PackName(c.sum)] = c
		case c.kind == repackedPack:
			repacked[dedup.PackName(c.sum)] = c
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
	keep := map[string]bool{}
	var candidates []candidate
	gone := 0
	for i, p := range r.packs {
		if c, ok := repacked[p.name]; ok {
			// Never reprieved: the new packs hold its live chunks. It stays
			// listed until it expires, for readers that located a chunk in it.
			if expired(c) {
				out.Expired = append(out.Expired, p.name)
				gone++
			} else {
				keep[p.name] = true
				next = append(next, c)
			}
			continue
		}
		c, was := packs[p.name]
		switch {
		case liveCount[i] > 0:
			if !policy.Off { // a reprieved pack too: mostly dead, it is repacked at once
				// A pack whose every chunk is live has nothing to reclaim,
				// whatever its overhead.
				if share := float64(liveBytes[i]) / float64(p.size); liveCount[i] < p.entries && share < policy.MaxLive {
					candidates = append(candidates, candidate{i: i, share: share, live: liveBytes[i]})
				}
			}
			keep[p.name] = true
			if was {
				out.Reprieved++
			}
		case was && expired(c):
			out.Expired = append(out.Expired, p.name)
			gone++
		case was:
			keep[p.name] = true
			next = append(next, c)
		case len(next) < maxCondemned/2: // the rest wait for a later round
			sum, err := dedup.PackSum(p.name)
			if err != nil {
				return Outcome{}, err
			}
			keep[p.name] = true
			next = append(next, condemned{kind: condemnedPack, sum: sum, at: at})
			out.Condemned++
		default:
			keep[p.name] = true
		}
	}
	fresh, err := r.copyLive(ctx, candidates, live, policy.Budget, &out)
	if err != nil {
		return Outcome{}, err
	}
	for _, c := range candidates[:out.Repacked] {
		sum, err := dedup.PackSum(r.packs[c.i].name)
		if err != nil {
			return Outcome{}, err
		}
		next = append(next, condemned{kind: repackedPack, sum: sum, at: at})
	}
	upd := r.man
	upd.seq++
	if gone > 0 || out.Repacked > 0 {
		sums, err := r.writeIndexes(ctx, keep, fresh)
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
	for name := range keep {
		out.Named[name] = true
	}
	for _, p := range fresh {
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
	if out.Condemned == 0 && out.Reprieved == 0 && len(out.Expired) == 0 && added == 0 && lapsed == 0 && out.Repacked == 0 {
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

// join walks the round's index and the live set together, both in hash
// order, and counts each pack's live chunks and their frame bytes. A chunk
// two packs hold counts for the first, the one in service if either is.
func (r *Round) join(live Live) (count []int, frames []int64, err error) {
	count, frames = make([]int, len(r.packs)), make([]int64, len(r.packs))
	if r.index == nil {
		return count, frames, nil
	}
	cur := r.index.table.Cursor()
	h, v, ok, err := cur.Next()
	if err != nil {
		return nil, nil, err
	}
	err = live.Each(func(l hash.Hash) error {
		for ok && h.Compare(l) < 0 {
			if h, v, ok, err = cur.Next(); err != nil {
				return err
			}
		}
		if ok && h == l {
			if len(v) != locLen {
				return fmt.Errorf("%w: round index record of %d bytes", chunk.ErrCorrupt, len(v))
			}
			pi := binary.LittleEndian.Uint32(v[0:])
			if pi >= uint32(len(r.packs)) {
				return fmt.Errorf("%w: round index names pack %d of %d", chunk.ErrCorrupt, pi, len(r.packs))
			}
			count[pi]++
			frames[pi] += int64(binary.LittleEndian.Uint32(v[8:]))
			if h, v, ok, err = cur.Next(); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return count, frames, nil
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

// resolved is the policy with its defaults filled in.
func (p Repack) resolved() (Repack, error) {
	if p.MaxLive == 0 {
		p.MaxLive = DefaultMaxLive
	}
	if p.Budget == 0 {
		p.Budget = DefaultBudget
	}
	if p.MaxLive < 0 || p.MaxLive > 1 || p.Budget < 0 {
		return Repack{}, fmt.Errorf("packstore: repack policy: live share %v must be within 0..1 and the budget %d non-negative", p.MaxLive, p.Budget)
	}
	return p, nil
}

// candidate is a kept pack that is mostly dead.
type candidate struct {
	i     int     // into Round.packs
	share float64 // its live bytes over its size
	live  int64   // its live frames' bytes: what a repack copies
}

// packInfo reads a pack's entries again from the index object listing it.
func (r *Round) packInfo(ctx context.Context, p packSummary) (pack.Info, error) {
	infos, err := loadIndex(ctx, r.o, p.object)
	if err != nil {
		return pack.Info{}, err
	}
	for _, info := range infos {
		if info.Name == p.name {
			return info, nil
		}
	}
	return pack.Info{}, fmt.Errorf("%w: index object %s no longer lists pack %s", chunk.ErrCorrupt, indexName(p.object), p.name)
}

// copyLive repacks candidates emptiest first until budget bytes of frames
// have been copied, reading each in one GET, opening its live frames and
// sealing them into new packs, which it uploads and returns. It sorts
// candidates so that the first out.Repacked of them are the ones repacked.
func (r *Round) copyLive(ctx context.Context, candidates []candidate, live Live, budget int64, out *Outcome) ([]pack.Info, error) {
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].share != candidates[j].share {
			return candidates[i].share < candidates[j].share
		}
		return r.packs[candidates[i].i].name < r.packs[candidates[j].i].name
	})
	size := r.o.PackSize
	if size == 0 {
		size = DefaultPackSize
	}
	codec, err := pack.NewCodec()
	if err != nil {
		return nil, err
	}
	defer codec.Close()
	var fresh []pack.Info
	var w *pack.Writer
	// The writer's buffer starts at what is left to copy, not at a pack
	// (#14): a repack of a few live KiB costs those KiB, and only a copy
	// past a pack's worth fills a pack-sized buffer.
	var toCopy int64
	for _, c := range candidates {
		toCopy += c.live
	}
	newWriter := func() (*pack.Writer, error) {
		return pack.NewWriterSized(r.o.Keys, r.o.Repo, codec, size, int(min(int64(size), toCopy-out.Copied)))
	}
	finish := func() error {
		if w == nil || w.Count() == 0 {
			return nil
		}
		b, err := w.Finish()
		w = nil
		if err != nil {
			return err
		}
		if err := r.o.Blobs.Put(ctx, b.Name, bytes.NewReader(b.Bytes), int64(len(b.Bytes))); err != nil && !errors.Is(err, blob.ErrExists) {
			return fmt.Errorf("packstore: uploading repacked %s: %w", b.Name, err)
		}
		fresh = append(fresh, b.Info)
		return nil
	}
	for _, cand := range candidates {
		if out.Copied >= budget {
			break
		}
		c, err := r.packInfo(ctx, r.packs[cand.i])
		if err != nil {
			return nil, err
		}
		// The live frames credited to this pack, in the order they lie in
		// it, read from one GET as a stream: a pack is never whole in
		// memory. A chunk another pack holds too counts for that pack
		// (join) and stays there: copied here, the fresh pack would hold a
		// chunk counted dead, look mostly dead next round, and be repacked
		// again, every round.
		var frames []pack.Entry
		for _, e := range c.Entries {
			if credited, err := r.creditedTo(e.Hash, cand.i); err != nil {
				return nil, err
			} else if !credited {
				continue
			}
			if ok, err := live.Has(e.Hash); err != nil {
				return nil, err
			} else if ok {
				frames = append(frames, e)
			}
		}
		sort.Slice(frames, func(i, j int) bool { return frames[i].Offset < frames[j].Offset })
		keys, err := pack.DeriveKeys(r.o.Keys, r.o.Repo, c.Salt)
		if err != nil {
			return nil, err
		}
		rc, err := r.o.Blobs.Get(ctx, c.Name, 0, -1)
		if err != nil {
			return nil, fmt.Errorf("packstore: repacking %s: %w", c.Name, err)
		}
		body := &packBody{r: rc, name: c.Name, size: c.Size}
		for _, e := range frames {
			frame, err := body.frame(e)
			if err != nil {
				_ = rc.Close()
				return nil, err
			}
			data, err := pack.OpenFrame(keys, codec, e, frame)
			if err != nil {
				_ = rc.Close()
				return nil, fmt.Errorf("%w: pack %s, chunk %s: %w", chunk.ErrCorrupt, c.Name, e.Hash.Short(), err)
			}
			if w == nil {
				if w, err = newWriter(); err != nil {
					return nil, err
				}
			}
			if err = w.Add(e.Hash, data); errors.Is(err, pack.ErrFull) {
				if err = finish(); err != nil {
					return nil, err
				}
				if w, err = newWriter(); err != nil {
					return nil, err
				}
				err = w.Add(e.Hash, data)
			}
			if err != nil {
				_ = rc.Close()
				return nil, err
			}
			out.Copied += int64(e.StoredLen)
		}
		if err := body.finish(); err != nil {
			return nil, err
		}
		out.Repacked++
	}
	if err := finish(); err != nil {
		return nil, err
	}
	return fresh, nil
}

// creditedTo reports whether the round's index credits h to pack pi: the
// join counts a chunk two packs hold for the first listed, and a repack
// copies a chunk from the pack it is counted for.
func (r *Round) creditedTo(h hash.Hash, pi int) (bool, error) {
	v, ok, err := r.index.table.Lookup(h)
	if err != nil || !ok {
		return false, err
	}
	if len(v) != locLen {
		return false, fmt.Errorf("%w: round index record of %d bytes", chunk.ErrCorrupt, len(v))
	}
	return binary.LittleEndian.Uint32(v[0:]) == uint32(pi), nil
}

// packBody streams a pack's bytes and hands out its frames in offset
// order, checking that the bytes are as long as the index says.
type packBody struct {
	r    io.ReadCloser
	name string
	size int64
	at   int64 // bytes consumed
}

// frame reads the frame of e, which must lie at or past the last one.
func (b *packBody) frame(e pack.Entry) ([]byte, error) {
	off, end := int64(e.Offset), int64(e.Offset)+int64(e.StoredLen)
	if off < b.at || end > b.size {
		return nil, fmt.Errorf("%w: pack %s: frame %s at %d..%d outside %d..%d", chunk.ErrCorrupt, b.name, e.Hash.Short(), off, end, b.at, b.size)
	}
	if _, err := io.CopyN(io.Discard, b.r, off-b.at); err != nil {
		return nil, fmt.Errorf("%w: pack %s is short: %w", chunk.ErrCorrupt, b.name, err)
	}
	frame := make([]byte, e.StoredLen)
	if _, err := io.ReadFull(b.r, frame); err != nil {
		return nil, fmt.Errorf("%w: pack %s is short: %w", chunk.ErrCorrupt, b.name, err)
	}
	b.at = end
	return frame, nil
}

// finish drains the pack to its end and checks nothing follows: bytes that
// are not what the index says are corrupt.
func (b *packBody) finish() error {
	defer b.r.Close()
	n, err := io.Copy(io.Discard, io.LimitReader(b.r, b.size-b.at+1))
	if err != nil {
		return err
	}
	if b.at+n != b.size {
		return fmt.Errorf("%w: pack %s is %d bytes, the index says %d", chunk.ErrCorrupt, b.name, b.at+n, b.size)
	}
	return nil
}

func containsSum(sums [][32]byte, s [32]byte) bool {
	for _, x := range sums {
		if x == s {
			return true
		}
	}
	return false
}

// writeIndexes stores the kept packs and the fresh ones as index objects,
// each within dedup's limits, streaming the old objects through so that
// one object's packs are in memory at a time, and returns the objects'
// sums in order.
func (r *Round) writeIndexes(ctx context.Context, keep map[string]bool, fresh []pack.Info) ([][32]byte, error) {
	w := &indexWriter{ctx: ctx, o: r.o}
	written := map[string]bool{}
	for _, sum := range r.man.indexes {
		infos, err := loadIndex(ctx, r.o, sum)
		if err != nil {
			return nil, err
		}
		for _, info := range infos {
			if keep[info.Name] && !written[info.Name] {
				written[info.Name] = true
				if err := w.add(info); err != nil {
					return nil, err
				}
			}
		}
	}
	for _, info := range fresh {
		if err := w.add(info); err != nil {
			return nil, err
		}
	}
	return w.finish()
}

// maxIndexObject bounds an index object's estimated size, well under
// dedup's limit: an object is decoded whole, so its size is memory a store
// or a round holds at a time (#6).
const maxIndexObject = 8 << 20

// indexWriter batches packs into index objects within maxIndexObject.
type indexWriter struct {
	ctx     context.Context
	o       Options
	batch   []pack.Info
	size    int
	sums    [][32]byte
	written []indexObject // each object written, with the packs it lists
	session int           // packstore's publish: the session packs taken from the queue
}

// indexObject is one index object written, the packs it lists and its
// estimated size.
type indexObject struct {
	sum   [32]byte
	packs []string
	est   int
}

func (w *indexWriter) add(info pack.Info) error {
	if len(w.batch) > 0 && (len(w.batch) >= dedup.MaxPacksPerObject || w.size+estimate(info) > maxIndexObject) {
		if err := w.flush(); err != nil {
			return err
		}
	}
	w.batch = append(w.batch, info)
	w.size += estimate(info)
	return nil
}

func (w *indexWriter) flush() error {
	if len(w.batch) == 0 {
		return nil
	}
	name, b, err := dedup.EncodeObject(w.o.Keys, w.o.Repo, w.batch)
	if err != nil {
		return err
	}
	if err := w.o.Blobs.Put(w.ctx, name, bytes.NewReader(b), int64(len(b))); err != nil && !errors.Is(err, blob.ErrExists) {
		return fmt.Errorf("packstore: writing index object: %w", err)
	}
	sum, err := indexSum(name)
	if err != nil {
		return err
	}
	obj := indexObject{sum: sum, est: w.size}
	for _, p := range w.batch {
		obj.packs = append(obj.packs, p.Name)
	}
	w.sums = append(w.sums, sum)
	w.written = append(w.written, obj)
	w.batch, w.size = nil, 0
	return nil
}

func (w *indexWriter) finish() ([][32]byte, error) {
	if err := w.flush(); err != nil {
		return nil, err
	}
	sort.Slice(w.sums, func(i, j int) bool { return bytes.Compare(w.sums[i][:], w.sums[j][:]) < 0 })
	return w.sums, nil
}

// estimate bounds a pack's share of an index object.
func estimate(p pack.Info) int { return 128 + 64*len(p.Entries) }
