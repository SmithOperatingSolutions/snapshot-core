package multivol

// WithFreeSpace replaces the free-space probe.
func WithFreeSpace(o Options, f func(path string) (free, total uint64, err error)) Options {
	o.freeSpace = f
	return o
}
