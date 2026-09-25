package packstore

import (
	"context"
	"sync"

	"github.com/SmithOperatingSolutions/snapshot-core/core/pack"
)

// Publishes compact small index objects (DESIGN §6). Each publish adds an
// index object, and GC rewrites them only when a pack expires or is
// repacked, so a repository without garbage would list one per publish
// until the manifest refused to grow (maxIndexes), each refresh and swap
// costing more on the way. An object's tier is its estimated size's power
// of compactFanIn over tierBase; once a tier holds compactFanIn objects, a
// publish merges them into one of a higher tier, smallest tier first. An
// object of smallIndexObject or more is large and never merged again, so
// the manifest lists at most tiers × (compactFanIn − 1) small objects, a
// pack's entry is rewritten at most once per tier. A tier merges at most
// maxMerge objects in one publish, read loadWindow at a time, so a
// repository listing thousands from before compaction converges over many
// publishes, each bounded in requests and memory.
const (
	compactFanIn     = 8
	maxMerge         = compactFanIn * compactFanIn
	loadWindow       = 16
	tierBase         = 1 << 10
	smallIndexObject = tierBase << 9 // 512 KiB: three tiers of eight above the first
	smallTiers       = 4
)

// tierOf is the tier of a small object of est bytes.
func tierOf(est int) int {
	k := 0
	for lim := tierBase; est >= lim; lim *= compactFanIn {
		k++
	}
	return k
}

// objectSize is an index object's estimated size, as indexWriter counts it.
func objectSize(infos []pack.Info) int {
	n := 0
	for _, p := range infos {
		n += estimate(p)
	}
	return n
}

// compaction is what one publish's compaction wrote and replaced.
type compaction struct {
	written  []indexObject
	replaced [][32]byte
}

// landed records a compaction whose manifest swapped in: the store holds
// every pack the merged objects list, and the replaced objects are gone
// from the manifest.
func (c compaction) landed(s *Store) {
	for _, obj := range c.written {
		s.loaded[obj.sum] = true
		s.objEst[obj.sum] = obj.est
	}
	for _, sum := range c.replaced {
		delete(s.loaded, sum)
		delete(s.objEst, sum)
	}
}

// compact merges each tier of list's small index objects that holds
// compactFanIn or more into new objects, lowest tier first, and returns the
// list with the merged objects in the place of those they replace. It reads
// each object it merges and writes the merged ones before the swap names
// them; a swap that loses leaves them orphans for GC. The packs are copied
// as listed, so the new list locates exactly the packs the old one did,
// each once. An object whose size the store does not know stays as it is.
func (s *Store) compact(ctx context.Context, list [][32]byte) ([][32]byte, compaction, error) {
	sizes := make(map[[32]byte]int, len(list))
	s.mu.Lock()
	for _, sum := range list {
		if est, ok := s.objEst[sum]; ok {
			sizes[sum] = est
		}
	}
	s.mu.Unlock()
	var tiers [smallTiers][][32]byte
	for _, sum := range list {
		if est, ok := sizes[sum]; ok && est < smallIndexObject {
			k := tierOf(est)
			tiers[k] = append(tiers[k], sum)
		}
	}
	var c compaction
	for k := range tiers {
		if len(tiers[k]) < compactFanIn {
			continue
		}
		merging := tiers[k][:min(len(tiers[k]), maxMerge)]
		w := &indexWriter{ctx: ctx, o: s.o}
		for i := 0; i < len(merging); i += loadWindow {
			objs, err := s.loadAll(ctx, merging[i:min(i+loadWindow, len(merging))])
			if err != nil {
				return nil, compaction{}, err
			}
			for _, infos := range objs {
				for _, p := range infos {
					if err := w.add(p); err != nil {
						return nil, compaction{}, err
					}
				}
			}
		}
		if _, err := w.finish(); err != nil {
			return nil, compaction{}, err
		}
		c.replaced = append(c.replaced, merging...)
		for _, obj := range w.written {
			c.written = append(c.written, obj)
			// compactFanIn objects of a tier add up to the next one's
			// floor, so what a merge wrote sits higher and is merged, if
			// at all, later in this pass.
			if t := tierOf(obj.est); obj.est < smallIndexObject && t > k {
				tiers[t] = append(tiers[t], obj.sum)
			}
		}
	}
	if len(c.replaced) == 0 {
		return list, c, nil
	}
	gone := make(map[[32]byte]bool, len(c.replaced))
	for _, sum := range c.replaced {
		gone[sum] = true
	}
	out := make([][32]byte, 0, len(list))
	for _, sum := range list {
		if !gone[sum] {
			out = append(out, sum)
		}
	}
	for _, obj := range c.written {
		if !gone[obj.sum] {
			out = append(out, obj.sum)
		}
	}
	return out, c, nil
}

// loadAll loads index objects concurrently, in order.
func (s *Store) loadAll(ctx context.Context, sums [][32]byte) ([][]pack.Info, error) {
	out := make([][]pack.Info, len(sums))
	errs := make([]error, len(sums))
	var wg sync.WaitGroup
	for i, sum := range sums {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out[i], errs[i] = s.loadIndex(ctx, sum)
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}
