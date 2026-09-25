package seal

import (
	"bytes"
	"context"
	"fmt"

	"github.com/SmithOperatingSolutions/snapshot-core/core/dnx"
	"github.com/SmithOperatingSolutions/snapshot-core/core/wire"
)

// Argon2Params are the passphrase KDF's costs.
type Argon2Params struct {
	Time    uint32
	Memory  uint32 // KiB
	Threads uint8
}

// DefaultArgon2Params are disknexus's defaults: time 3, 64 MiB, 4 threads.
func DefaultArgon2Params() Argon2Params {
	d := dnx.DefaultArgon2Params()
	return Argon2Params{Time: d.Time, Memory: d.Memory, Threads: d.Threads}
}

// Bounds on Argon2id costs. The floor is OWASP's (19 MiB, 2 passes); the
// ceiling, a gibibyte and ten passes, is what a host derives in seconds
// (disknexus writes 64 MiB and three), so a crafted key file is not a way
// to exhaust memory or CPU: a file over it is refused before any
// derivation (#13; at 4 GiB and 64 passes the fuzzer hung the process).
const (
	minArgonTime   = 2
	maxArgonTime   = 10
	minArgonMemory = 19 * 1024
	maxArgonMemory = 1024 * 1024
	maxArgonThread = 64
	maxPassphrase  = 1024
)

func (p Argon2Params) validate() error {
	switch {
	case p.Time < minArgonTime || p.Time > maxArgonTime:
		return fmt.Errorf("%w: time %d outside %d..%d", ErrParams, p.Time, minArgonTime, maxArgonTime)
	case p.Memory < minArgonMemory || p.Memory > maxArgonMemory:
		return fmt.Errorf("%w: memory %d KiB outside %d..%d", ErrParams, p.Memory, minArgonMemory, maxArgonMemory)
	case p.Threads < 1 || p.Threads > maxArgonThread:
		return fmt.Errorf("%w: threads %d outside 1..%d", ErrParams, p.Threads, maxArgonThread)
	}
	return nil
}

func validPassphrase(p []byte) error {
	if len(p) == 0 || len(p) > maxPassphrase {
		return fmt.Errorf("%w: length %d outside 1..%d", ErrPassphrase, len(p), maxPassphrase)
	}
	return nil
}

// Key file v1 (139 bytes):
//
//	magic "SCKF" | version u16 | time u32 | memory u32 | threads u8 |
//	salt [32] | key id [32] | AES-GCM(KEK, master key) [60]
//
// The KEK is Argon2id(passphrase, salt, params). The wrap's associated data
// is "vdb/keyfile/v1" || 0 || the 79 header bytes, so the parameters, salt
// and key id are authenticated: a downgrade that stays in range still fails.
const (
	keyFileMagic     = "SCKF"
	keyFileVersion   = 1
	keyFileHeaderLen = 79
	keyFileTag       = "vdb/keyfile/v1"
)

func keyFileAAD(header []byte) []byte {
	return append(append([]byte(keyFileTag), 0), header...)
}

func kekAEAD(passphrase, salt []byte, p Argon2Params) (*dnx.AEAD, error) {
	kek, err := dnx.DeriveKEK(passphrase, salt, dnx.Argon2Params{Time: p.Time, Memory: p.Memory, Threads: p.Threads})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrParams, err)
	}
	defer clear(kek)
	return dnx.NewAEAD(kek)
}

// NewKeyFile wraps kr's master key under a passphrase. The result is for the
// host to store; never put it in the repository's backend. Calling it again
// with another passphrase is rotation: a new file, the same key.
func NewKeyFile(kr *Keyring, passphrase []byte, p Argon2Params) ([]byte, error) {
	if err := validPassphrase(passphrase); err != nil {
		return nil, err
	}
	if err := p.validate(); err != nil {
		return nil, err
	}
	salt, err := NewSalt()
	if err != nil {
		return nil, err
	}
	var w wire.Writer
	w.Raw([]byte(keyFileMagic))
	w.U16(keyFileVersion)
	w.U32(p.Time)
	w.U32(p.Memory)
	w.U8(p.Threads)
	w.Raw(salt[:])
	id := kr.ID()
	w.Raw(id[:])
	header := bytes.Clone(w.Bytes())

	aead, err := kekAEAD(passphrase, salt[:], p)
	if err != nil {
		return nil, err
	}
	defer aead.Destroy()
	var sealed []byte
	err = kr.withMaster(func(master []byte) error {
		var err error
		sealed, err = aead.Seal(master, keyFileAAD(header))
		return err
	})
	if err != nil {
		return nil, err
	}
	w.Raw(sealed)
	return w.Bytes(), nil
}

// kdf turns a passphrase into the AEAD that wraps the master key.
type kdf func(passphrase, salt []byte, p Argon2Params) (*dnx.AEAD, error)

// OpenKeyFile unwraps a key file.
func OpenKeyFile(file, passphrase []byte) (*Keyring, error) {
	return openKeyFile(file, passphrase, kekAEAD)
}

// openKeyFile is OpenKeyFile with the derivation handed in, so a test can
// see which Argon2 costs a file asks for without paying them.
func openKeyFile(file, passphrase []byte, derive kdf) (*Keyring, error) {
	if err := validPassphrase(passphrase); err != nil {
		return nil, err
	}
	r := wire.NewReader(file)
	magic := r.Fixed(4)
	version := r.U16()
	p := Argon2Params{Time: r.U32(), Memory: r.U32(), Threads: r.U8()}
	salt := r.Fixed(32)
	id := r.Fixed(32)
	sealed := r.Fixed(32 + dnx.Overhead)
	if err := r.Done(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrCorrupt, err)
	}
	if string(magic) != keyFileMagic || version != keyFileVersion {
		return nil, fmt.Errorf("%w: not a v%d key file", ErrCorrupt, keyFileVersion)
	}
	if err := p.validate(); err != nil {
		return nil, err // before derivation: a crafted file cannot demand 4 TiB
	}
	aead, err := derive(passphrase, salt, p)
	if err != nil {
		return nil, err
	}
	defer aead.Destroy()
	master, err := aead.Open(sealed, keyFileAAD(file[:keyFileHeaderLen]))
	if err != nil {
		return nil, ErrWrongPassphrase
	}
	defer clear(master)
	kr, err := KeyringFromBytes(master)
	if err != nil {
		return nil, err
	}
	if kid := kr.ID(); !bytes.Equal(kid[:], id) {
		return nil, fmt.Errorf("%w: key id does not match the key", ErrCorrupt)
	}
	return kr, nil
}

// KMS envelope v1: magic "SCKW" | version u16 | key id [32] | wrapped (len-prefixed).
const (
	envelopeMagic   = "SCKW"
	envelopeVersion = 1
	maxWrapped      = 8192
)

// WrapKeyring seals kr's master key with a KMS, in an envelope that names the
// key so a wrong one is caught by ID, not by a failed decrypt deep inside.
func WrapKeyring(ctx context.Context, w Wrapper, kr *Keyring) ([]byte, error) {
	var wrapped []byte
	err := kr.withMaster(func(master []byte) error {
		var err error
		wrapped, err = w.Wrap(ctx, bytes.Clone(master))
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("seal: KMS wrap: %w", err)
	}
	if len(wrapped) == 0 || len(wrapped) > maxWrapped {
		return nil, fmt.Errorf("seal: KMS returned %d wrapped bytes", len(wrapped))
	}
	var e wire.Writer
	e.Raw([]byte(envelopeMagic))
	e.U16(envelopeVersion)
	id := kr.ID()
	e.Raw(id[:])
	e.LenBytes(wrapped)
	return e.Bytes(), nil
}

// UnwrapKeyring opens an envelope WrapKeyring produced.
func UnwrapKeyring(ctx context.Context, w Wrapper, envelope []byte) (*Keyring, error) {
	r := wire.NewReader(envelope)
	magic := r.Fixed(4)
	version := r.U16()
	id := r.Fixed(32)
	wrapped := r.LenBytes(maxWrapped)
	if err := r.Done(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrCorrupt, err)
	}
	if string(magic) != envelopeMagic || version != envelopeVersion || len(wrapped) == 0 {
		return nil, fmt.Errorf("%w: not a v%d key envelope", ErrCorrupt, envelopeVersion)
	}
	secret, err := w.Unwrap(ctx, wrapped)
	if err != nil {
		return nil, fmt.Errorf("seal: KMS unwrap: %w", err)
	}
	defer clear(secret)
	if len(secret) != 32 {
		return nil, fmt.Errorf("%w: the KMS returned %d bytes", ErrWrongKey, len(secret))
	}
	kr, err := KeyringFromBytes(secret)
	if err != nil {
		return nil, err
	}
	if kid := kr.ID(); !bytes.Equal(kid[:], id) {
		kr.Destroy()
		return nil, fmt.Errorf("%w: the KMS returned a different key than the envelope names", ErrWrongKey)
	}
	return kr, nil
}
