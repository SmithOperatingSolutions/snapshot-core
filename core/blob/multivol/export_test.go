package multivol

// WithFreeSpace replaces the free-space probe.
func WithFreeSpace(o Options, f func(path string) (free, total uint64, err error)) Options {
	o.freeSpace = f
	return o
}

// DecodeMap and EncodeMap expose the volume-map codec for fuzzing.
func DecodeMap(b []byte) ([][2]string, error) {
	es, err := decodeMap(b)
	out := make([][2]string, len(es))
	for i, e := range es {
		out[i] = [2]string{e.id, e.path}
	}
	return out, err
}

// EncodeMap is the volume-map encoder.
func EncodeMap(entries [][2]string) ([]byte, error) {
	es := make([]mapEntry, len(entries))
	for i, e := range entries {
		es[i] = mapEntry{id: e[0], path: e[1]}
	}
	return encodeMap(es)
}
