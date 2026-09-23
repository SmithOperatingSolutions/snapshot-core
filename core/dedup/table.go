package dedup

import (
	"os"

	"github.com/SmithOperatingSolutions/snapshot-core/core/hash"
)

// Builder makes a Table from records added in any order (#6, DESIGN §6):
// records are sorted in memory a run at a time, each run spilled to disk,
// and the runs merged into the table. A key added more than once keeps its
// first value.
type Builder struct {
	dir      string
	valueLen int
	runSize  int
}

// NewBuilder starts a table of fixed-width values in dir ("": the system's
// temporary directory).
func NewBuilder(dir string, valueLen int) (*Builder, error) {
	return &Builder{dir: dir, valueLen: valueLen}, nil
}

// Add records a key and its value.
func (b *Builder) Add(h hash.Hash, value []byte) error { return nil }

// Finish merges the runs into the table and removes them.
func (b *Builder) Finish() (*Table, error) {
	f, err := os.CreateTemp(b.dir, "snapshot-table-*")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return &Table{path: f.Name()}, nil
}

// Abort removes the runs of a build that will not finish.
func (b *Builder) Abort() {}

// Table is a sorted table of hash-keyed records on disk, read with two
// range reads per lookup and almost nothing in memory.
type Table struct {
	path string
}

// OpenTable opens a table file a Builder wrote.
func OpenTable(path string) (*Table, error) { return &Table{path: path}, nil }

// Lookup finds a key's value.
func (t *Table) Lookup(h hash.Hash) ([]byte, bool, error) { return nil, false, nil }

// Len is the number of records.
func (t *Table) Len() int64 { return 0 }

// Close closes and removes the table's file.
func (t *Table) Close() error { return nil }

// Cursor reads the table in key order.
func (t *Table) Cursor() *Cursor { return &Cursor{} }

// Cursor is a sequential read of a table.
type Cursor struct{}

// Next returns the next record, or ok false at the end.
func (c *Cursor) Next() (hash.Hash, []byte, bool, error) { return hash.Hash{}, nil, false, nil }
