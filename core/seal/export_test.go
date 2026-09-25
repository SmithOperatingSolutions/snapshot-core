package seal

import "github.com/SmithOperatingSolutions/snapshot-core/core/dnx"

// SetSealLimit lowers a key's budget so the refusal can be tested without a
// million seals.
func (k *Key) SetSealLimit(n uint64) { k.setLimit(n) }

// OpenKeyFileObserved opens a key file, calling observe with the Argon2
// costs before any derivation; an error from observe stops the open before
// it derives, so a test can see what a file would cost without paying it.
func OpenKeyFileObserved(file, passphrase []byte, observe func(Argon2Params) error) (*Keyring, error) {
	return openKeyFile(file, passphrase, func(pass, salt []byte, p Argon2Params) (*dnx.AEAD, error) {
		if err := observe(p); err != nil {
			return nil, err
		}
		return kekAEAD(pass, salt, p)
	})
}
