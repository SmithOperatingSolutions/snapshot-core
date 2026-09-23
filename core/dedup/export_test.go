package dedup

// SetRunSize makes a builder spill a run every n records, so tests build
// tables of several runs from few records.
func SetRunSize(b *Builder, n int) { b.runSize = n }

// TablePath is the file a table reads from.
func TablePath(t *Table) string { return t.path }
