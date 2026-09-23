package cache

// SetEntryLimit shrinks the largest read the cache keeps, so a test need not
// move 64 MiB to cross it.
func SetEntryLimit(s *Store, n int64) { s.entryLimit = n }
