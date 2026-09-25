# Security

snapshot-core stores other people's data, encrypted, and its history. A
vulnerability here is a way to read stored data without the key, to make a
store accept data it should refuse, to make a reader return data other than
what was written, or to take a process down with crafted input.

## Reporting a vulnerability

Please do not open a public issue for a vulnerability. Use GitHub's private
vulnerability reporting on this repository ("Report a vulnerability" under
the Security tab), which reaches the maintainers alone. Include the version
or commit, what the input or setup is, and what happens. You will get an
acknowledgement, and a fix or a stated reason before anything is published.

## What is claimed, and what is not

- **Encryption is mandatory.** Every chunk, index object, manifest and root
  is sealed with an AEAD under a key derived from the repository's master key
  with a per-object HKDF and a domain tag. There is no plaintext mode.
- **Every read is verified.** A chunk is re-hashed on every read from every
  backend, the cache included, and refused if it does not match its name.
- **Every on-disk structure has a hand-written, bounds-checked decoder and a
  fuzz target**, and every length, count, exponent and depth read from input
  is checked against a limit before anything is sized from it.
- **Authorization is default-deny**, per principal, per branch, tag and
  written path.

Not claimed:

- **No independent audit.** The cryptography and the sealed formats have
  been reviewed only by the project itself. The project is pre-1.0.
- **Deletion is logical.** Deleted data stays in older commits until a
  purge operation exists (#36). Master-key rotation with re-encryption does
  not exist yet (#37).
- **The host's environment.** Key files, KMS access, filesystem permissions,
  the S3 bucket policy, and memory holding the master key are the host's
  responsibility. The core never writes the master key to disk unsealed.

## Supported versions

Fixes land on `main` and are released as the next tag. Until `v1`, only the
latest minor version receives fixes.
