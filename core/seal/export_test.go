package seal

// SetSealLimit lowers a key's budget so the refusal can be tested without a
// million seals.
func (k *Key) SetSealLimit(n uint64) { k.setLimit(n) }
