package dedup

import (
	"bufio"
	"bytes"
	"container/heap"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"sort"
	"sync"

	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
)

// A table file (#6, docs/DESIGN.md §6): a header, then count records of
// hash.Size+valueLen bytes in strictly ascending key order, then one sample
// key per block of sampleEvery records (the block's first key). A lookup
// keeps one key per sampleEvery samples in memory, reads the sample block
// that key names, then the record block the sample names: two range reads,
// and memory of one key in 65,536 records.
//
// Header (32 bytes): "SCTB" | version u16 | valueLen u16 | count u64 |
// sampleEvery u32 | 12 zero bytes.
const (
	tableMagic   = "SCTB"
	tableVersion = 1
	tableHeader  = 32
	sampleEvery  = 256

	// MaxValueLen bounds a record's value.
	MaxValueLen = 255

	defaultRunSize = 1 << 19   // records sorted in memory at a time (about 25 MiB of 49-byte records)
	mergeFanIn     = 32        // runs merged at a time
	mergeBuffer    = 256 << 10 // per run being merged
)

// Builder makes a Table from records added in any order: records are
// sorted in memory a run at a time, each run spilled to disk, and the runs
// merged into the table. A key added more than once keeps its first value.
type Builder struct {
	dir      string
	valueLen int
	rec      int // record size
	runSize  int
	buf      []byte // the current run's records, in the order added
	n        int
	runs     []string // spilled runs, in the order spilled
}

// NewBuilder starts a table of fixed-width values in dir ("": the system's
// temporary directory).
func NewBuilder(dir string, valueLen int) (*Builder, error) {
	if valueLen < 0 || valueLen > MaxValueLen {
		return nil, fmt.Errorf("dedup: table value length %d outside 0..%d", valueLen, MaxValueLen)
	}
	if dir == "" {
		dir = os.TempDir()
	}
	return &Builder{dir: dir, valueLen: valueLen, rec: hash.Size + valueLen, runSize: defaultRunSize}, nil
}

// Add records a key and its value.
func (b *Builder) Add(h hash.Hash, value []byte) error {
	if len(value) != b.valueLen {
		return fmt.Errorf("dedup: table value of %d bytes, want %d", len(value), b.valueLen)
	}
	b.buf = append(append(b.buf, h[:]...), value...)
	b.n++
	if b.n >= b.runSize {
		return b.spill()
	}
	return nil
}

// spill sorts the current run (stably: the first of a key stays first) and
// writes it.
func (b *Builder) spill() error {
	if b.n == 0 {
		return nil
	}
	idx := make([]int32, b.n)
	for i := range idx {
		idx[i] = int32(i)
	}
	key := func(i int32) []byte { return b.buf[int(i)*b.rec : int(i)*b.rec+hash.Size] }
	slices.SortStableFunc(idx, func(i, j int32) int { return bytes.Compare(key(i), key(j)) })
	f, err := os.CreateTemp(b.dir, "snapshot-run-*")
	if err != nil {
		return err
	}
	b.runs = append(b.runs, f.Name())
	w := bufio.NewWriterSize(f, mergeBuffer)
	for _, i := range idx {
		if _, err := w.Write(b.buf[int(i)*b.rec : int(i+1)*b.rec]); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err := w.Flush(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	b.buf, b.n = b.buf[:0], 0
	return nil
}

// Finish merges the runs into the table and removes them.
func (b *Builder) Finish() (*Table, error) {
	t, err := b.finish()
	if err != nil {
		b.Abort()
		return nil, err
	}
	return t, nil
}

func (b *Builder) finish() (*Table, error) {
	if err := b.spill(); err != nil {
		return nil, err
	}
	b.buf = nil
	for len(b.runs) > mergeFanIn {
		// Too many runs to merge at once: merge them in groups, in order,
		// so the first of a key stays first.
		var next []string
		for i := 0; i < len(b.runs); i += mergeFanIn {
			group := b.runs[i:min(i+mergeFanIn, len(b.runs))]
			out, err := os.CreateTemp(b.dir, "snapshot-run-*")
			if err != nil {
				return nil, err
			}
			next = append(next, out.Name())
			if _, err := b.merge(group, out, nil); err != nil {
				_ = out.Close()
				return nil, err
			}
			if err := out.Close(); err != nil {
				return nil, err
			}
			removeAll(group)
		}
		b.runs = next
	}
	f, err := os.CreateTemp(b.dir, "snapshot-table-*")
	if err != nil {
		return nil, err
	}
	path := f.Name()
	samples, err := os.CreateTemp(b.dir, "snapshot-samples-*")
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	defer func() {
		_ = samples.Close()
		_ = os.Remove(samples.Name())
	}()
	var hdr [tableHeader]byte
	if _, err := f.Write(hdr[:]); err != nil {
		_ = f.Close()
		return nil, err
	}
	count, err := b.merge(b.runs, f, samples)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if _, err := samples.Seek(0, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, err
	}
	if _, err := io.Copy(f, samples); err != nil {
		_ = f.Close()
		return nil, err
	}
	copy(hdr[:], tableMagic)
	binary.LittleEndian.PutUint16(hdr[4:], tableVersion)
	binary.LittleEndian.PutUint16(hdr[6:], uint16(b.valueLen))
	binary.LittleEndian.PutUint64(hdr[8:], uint64(count))
	binary.LittleEndian.PutUint32(hdr[16:], sampleEvery)
	if _, err := f.WriteAt(hdr[:], 0); err != nil {
		_ = f.Close()
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	removeAll(b.runs)
	b.runs = nil
	t, err := OpenTable(path)
	if err != nil {
		_ = os.Remove(path)
		return nil, err
	}
	return t, nil
}

// Abort removes the runs of a build that will not finish.
func (b *Builder) Abort() {
	removeAll(b.runs)
	b.runs, b.buf = nil, nil
}

func removeAll(paths []string) {
	for _, p := range paths {
		_ = os.Remove(p)
	}
}

// merge writes the runs' records to out in key order, the first of each
// key only, and, when samples is not nil, each block's first key to it. It
// returns the records written.
func (b *Builder) merge(runs []string, out io.Writer, samples io.Writer) (int64, error) {
	m := &merger{rec: b.rec}
	defer m.close()
	for i, p := range runs {
		f, err := os.Open(p) //nolint:gosec // G304: a run this builder wrote
		if err != nil {
			return 0, err
		}
		m.files = append(m.files, f)
		r := &run{i: i, r: bufio.NewReaderSize(f, mergeBuffer), rec: make([]byte, b.rec)}
		ok, err := r.next()
		if err != nil {
			return 0, err
		}
		if ok {
			m.heads = append(m.heads, r)
		}
	}
	heap.Init(m)
	w := bufio.NewWriterSize(out, mergeBuffer)
	var count int64
	var last []byte
	for m.Len() > 0 {
		r := m.heads[0]
		if last == nil || !bytes.Equal(r.rec[:hash.Size], last) {
			if samples != nil && count%sampleEvery == 0 {
				if _, err := samples.Write(r.rec[:hash.Size]); err != nil {
					return 0, err
				}
			}
			if _, err := w.Write(r.rec); err != nil {
				return 0, err
			}
			last = append(last[:0], r.rec[:hash.Size]...)
			count++
		}
		ok, err := r.next()
		if err != nil {
			return 0, err
		}
		if ok {
			heap.Fix(m, 0)
		} else {
			heap.Pop(m)
		}
	}
	return count, w.Flush()
}

// run is one spilled run being merged; rec is its current record.
type run struct {
	i   int
	r   *bufio.Reader
	rec []byte
}

func (r *run) next() (bool, error) {
	_, err := io.ReadFull(r.r, r.rec)
	if errors.Is(err, io.EOF) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("dedup: reading a spilled run: %w", err)
	}
	return true, nil
}

// merger is a heap of runs by current key, then by run order, so that of
// two records with one key the earlier run's comes first.
type merger struct {
	rec   int
	files []*os.File
	heads []*run
}

func (m *merger) Len() int { return len(m.heads) }
func (m *merger) Less(i, j int) bool {
	c := bytes.Compare(m.heads[i].rec[:hash.Size], m.heads[j].rec[:hash.Size])
	return c < 0 || (c == 0 && m.heads[i].i < m.heads[j].i)
}
func (m *merger) Swap(i, j int) { m.heads[i], m.heads[j] = m.heads[j], m.heads[i] }
func (m *merger) Push(x any)    { m.heads = append(m.heads, x.(*run)) }
func (m *merger) Pop() any {
	x := m.heads[len(m.heads)-1]
	m.heads = m.heads[:len(m.heads)-1]
	return x
}

func (m *merger) close() {
	for _, f := range m.files {
		_ = f.Close()
	}
	m.files = nil
}

// Table is a sorted table of hash-keyed records on disk, read with two
// range reads per lookup and almost nothing in memory.
type Table struct {
	f        *os.File
	path     string
	valueLen int
	rec      int
	count    int64
	samples  int64             // one per block of sampleEvery records
	l1       [][hash.Size]byte // one sample per block of sampleEvery samples
	bufs     sync.Pool
}

// OpenTable opens a table file a Builder wrote, refusing one that does not
// decode (ErrCorrupt).
func OpenTable(path string) (*Table, error) {
	f, err := os.Open(path) //nolint:gosec // G304: a table a Builder wrote
	if err != nil {
		return nil, err
	}
	t, err := openTable(f, path)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return t, nil
}

func openTable(f *os.File, path string) (*Table, error) {
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := st.Size()
	var hdr [tableHeader]byte
	if size < tableHeader {
		return nil, fmt.Errorf("%w: table file of %d bytes", ErrCorrupt, size)
	}
	if _, err := f.ReadAt(hdr[:], 0); err != nil {
		return nil, err
	}
	valueLen := int(binary.LittleEndian.Uint16(hdr[6:]))
	count := binary.LittleEndian.Uint64(hdr[8:])
	switch {
	case string(hdr[:4]) != tableMagic:
		return nil, fmt.Errorf("%w: table magic %q", ErrCorrupt, hdr[:4])
	case binary.LittleEndian.Uint16(hdr[4:]) != tableVersion:
		return nil, fmt.Errorf("%w: table version %d", ErrCorrupt, binary.LittleEndian.Uint16(hdr[4:]))
	case valueLen > MaxValueLen:
		return nil, fmt.Errorf("%w: table value length %d", ErrCorrupt, valueLen)
	case binary.LittleEndian.Uint32(hdr[16:]) != sampleEvery:
		return nil, fmt.Errorf("%w: table sample period %d", ErrCorrupt, binary.LittleEndian.Uint32(hdr[16:]))
	case !bytes.Equal(hdr[20:], make([]byte, 12)):
		return nil, fmt.Errorf("%w: table header reserved bytes", ErrCorrupt)
	}
	rec := hash.Size + valueLen
	if count > uint64(size-tableHeader)/uint64(rec) {
		return nil, fmt.Errorf("%w: table of %d records in %d bytes", ErrCorrupt, count, size)
	}
	samples := (int64(count) + sampleEvery - 1) / sampleEvery
	if want := tableHeader + int64(count)*int64(rec) + samples*hash.Size; want != size {
		return nil, fmt.Errorf("%w: table of %d records is %d bytes, want %d", ErrCorrupt, count, size, want)
	}
	t := &Table{f: f, path: path, valueLen: valueLen, rec: rec, count: int64(count), samples: samples}
	t.bufs.New = func() any { return make([]byte, sampleEvery*rec) }
	for j := int64(0); j < samples; j += sampleEvery {
		var k [hash.Size]byte
		if _, err := f.ReadAt(k[:], t.sampleOff(j)); err != nil {
			return nil, err
		}
		t.l1 = append(t.l1, k)
	}
	return t, nil
}

func (t *Table) sampleOff(j int64) int64 { return tableHeader + t.count*int64(t.rec) + j*hash.Size }
func (t *Table) recordOff(i int64) int64 { return tableHeader + i*int64(t.rec) }

// Lookup finds a key's value.
func (t *Table) Lookup(h hash.Hash) ([]byte, bool, error) {
	if t.count == 0 {
		return nil, false, nil
	}
	// The sample block whose first key is the largest at or under h.
	i1 := sort.Search(len(t.l1), func(i int) bool { return bytes.Compare(t.l1[i][:], h[:]) > 0 }) - 1
	if i1 < 0 {
		return nil, false, nil // under the first key; the block search below would say so too
	}
	buf := t.bufs.Get().([]byte)
	defer t.bufs.Put(buf) //nolint:staticcheck // SA6002: the slice is what the pool holds
	from := int64(i1) * sampleEvery
	n := min(sampleEvery, t.samples-from)
	keys := buf[:n*hash.Size]
	if _, err := t.f.ReadAt(keys, t.sampleOff(from)); err != nil {
		return nil, false, fmt.Errorf("dedup: reading the table: %w", err)
	}
	j := sort.Search(int(n), func(i int) bool { return bytes.Compare(keys[i*hash.Size:(i+1)*hash.Size], h[:]) > 0 }) - 1
	if j < 0 {
		return nil, false, nil // cannot happen with a well-formed table: the block's first key is its sample
	}
	// The record block that sample names.
	block := from + int64(j)
	first := block * sampleEvery
	n = min(sampleEvery, t.count-first)
	recs := buf[:n*int64(t.rec)]
	if _, err := t.f.ReadAt(recs, t.recordOff(first)); err != nil {
		return nil, false, fmt.Errorf("dedup: reading the table: %w", err)
	}
	k := sort.Search(int(n), func(i int) bool { return bytes.Compare(recs[i*t.rec:i*t.rec+hash.Size], h[:]) >= 0 })
	if k == int(n) || !bytes.Equal(recs[k*t.rec:k*t.rec+hash.Size], h[:]) {
		return nil, false, nil
	}
	return bytes.Clone(recs[k*t.rec+hash.Size : (k+1)*t.rec]), true, nil
}

// Has reports whether a key is in the table.
func (t *Table) Has(h hash.Hash) (bool, error) {
	_, ok, err := t.Lookup(h)
	return ok, err
}

// Len is the number of records.
func (t *Table) Len() int64 { return t.count }

// Close closes and removes the table's file.
func (t *Table) Close() error {
	err := t.f.Close()
	if rerr := os.Remove(t.path); err == nil {
		err = rerr
	}
	return err
}

// Cursor reads the table in key order.
func (t *Table) Cursor() *Cursor {
	sr := io.NewSectionReader(t.f, tableHeader, t.count*int64(t.rec))
	return &Cursor{r: bufio.NewReaderSize(sr, mergeBuffer), left: t.count, rec: make([]byte, t.rec)}
}

// Cursor is a sequential read of a table.
type Cursor struct {
	r    *bufio.Reader
	left int64
	rec  []byte
}

// Next returns the next record, or ok false at the end. The value is valid
// until the next call.
func (c *Cursor) Next() (hash.Hash, []byte, bool, error) {
	var h hash.Hash
	if c.left == 0 {
		return h, nil, false, nil
	}
	if _, err := io.ReadFull(c.r, c.rec); err != nil {
		return h, nil, false, fmt.Errorf("dedup: reading the table: %w", err)
	}
	c.left--
	copy(h[:], c.rec)
	return h, c.rec[hash.Size:], true, nil
}
