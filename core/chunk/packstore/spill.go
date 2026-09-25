package packstore

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/SmithOperatingSolutions/snapshot-core/core/chunk"
	"github.com/SmithOperatingSolutions/snapshot-core/core/dedup"
	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
	"github.com/SmithOperatingSolutions/snapshot-core/core/pack"
	"github.com/SmithOperatingSolutions/snapshot-core/core/seal"
)

// spilled is the index of published chunks on disk (#6, DESIGN §6): a
// dedup table from chunk hash to a location whose pack is an index into
// packs. Memory is a record per pack, none per chunk.
type spilled struct {
	table  *dedup.Table
	packs  []packRef
	listed map[[32]byte]bool // the packs in the table, by sum
}

// packRef is what a location needs of its pack.
type packRef struct {
	sum  [32]byte
	salt seal.Salt
	size int64
}

// locLen is a table value: pack u32 | offset u32 | stored u32 | raw u32 |
// codec u8.
const locLen = 17

func encodeLoc(pi uint32, e pack.Entry) []byte {
	var v [locLen]byte
	binary.LittleEndian.PutUint32(v[0:], pi)
	binary.LittleEndian.PutUint32(v[4:], e.Offset)
	binary.LittleEndian.PutUint32(v[8:], e.StoredLen)
	binary.LittleEndian.PutUint32(v[12:], e.RawLen)
	v[16] = e.Codec
	return v[:]
}

// lookup locates a chunk in the table.
func (sp *spilled) lookup(h hash.Hash) (dedup.Location, bool, error) {
	v, ok, err := sp.table.Lookup(h)
	if err != nil || !ok {
		return dedup.Location{}, false, err
	}
	if len(v) != locLen {
		return dedup.Location{}, false, fmt.Errorf("%w: index table record of %d bytes", chunk.ErrCorrupt, len(v))
	}
	pi := binary.LittleEndian.Uint32(v[0:])
	if pi >= uint32(len(sp.packs)) {
		return dedup.Location{}, false, fmt.Errorf("%w: index table names pack %d of %d", chunk.ErrCorrupt, pi, len(sp.packs))
	}
	p := sp.packs[pi]
	return dedup.Location{
		Pack: dedup.PackRef{Name: dedup.PackName(p.sum), Salt: p.salt, Size: p.size},
		Entry: pack.Entry{Hash: h, Offset: binary.LittleEndian.Uint32(v[4:]), StoredLen: binary.LittleEndian.Uint32(v[8:]),
			RawLen: binary.LittleEndian.Uint32(v[12:]), Codec: v[16]},
	}, true, nil
}

func (sp *spilled) has(h hash.Hash) (bool, error) { return sp.table.Has(h) }

func (sp *spilled) len() int64 { return sp.table.Len() }

func (sp *spilled) close() error { return sp.table.Close() }

// buildSpilled writes the table for manifest m: every pack its index
// objects list, those in service first and the condemned after, so a chunk
// two packs hold resolves to the one staying. Objects already in loaded are
// not read again.
func (s *Store) buildSpilled(ctx context.Context, m manifest, loaded map[[32]byte][]pack.Info, cond map[string]bool) (*spilled, error) {
	b, err := dedup.NewBuilder(s.o.IndexDir, locLen)
	if err != nil {
		return nil, err
	}
	sp := &spilled{listed: map[[32]byte]bool{}}
	add := func(info pack.Info) error {
		sum, err := dedup.PackSum(info.Name)
		if err != nil {
			return err
		}
		sp.listed[sum] = true
		pi := uint32(len(sp.packs))
		sp.packs = append(sp.packs, packRef{sum: sum, salt: info.Salt, size: info.Size})
		for _, e := range info.Entries {
			if err := b.Add(e.Hash, encodeLoc(pi, e)); err != nil {
				return err
			}
		}
		return nil
	}
	var later []pack.Info // the condemned packs, added last
	for _, sum := range m.indexes {
		infos, ok := loaded[sum]
		if !ok {
			if infos, err = s.loadIndex(ctx, sum); err != nil {
				b.Abort()
				return nil, err
			}
		}
		for _, info := range infos {
			if cond[info.Name] {
				later = append(later, info)
				continue
			}
			if err := add(info); err != nil {
				b.Abort()
				return nil, err
			}
		}
	}
	for _, info := range later {
		if err := add(info); err != nil {
			b.Abort()
			return nil, err
		}
	}
	if sp.table, err = b.Finish(); err != nil {
		return nil, fmt.Errorf("packstore: spilling the index to %s: %w", s.o.IndexDir, err)
	}
	return sp, nil
}

// entries counts the chunks of packs.
func entries(infos []pack.Info) int {
	n := 0
	for _, info := range infos {
		n += len(info.Entries)
	}
	return n
}
