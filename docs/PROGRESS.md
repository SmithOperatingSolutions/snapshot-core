# Progress

Where the storage core stands against the Storage Core Spec's milestones and
every "first failing test" checkbox in both specs. Updated at each milestone
boundary and whenever a checklist item turns green; the evidence for each item
is the named test, and the commit that added it carries its red.

**Updated 2026-09-23** · branch `storage-core` (local, not pushed) · 45 commits
· red-check: 21 `test:` commits, 108 tests, all proven red · 67 checked-in
mutants, all killed · lint clean

## Milestones

| Milestone | Status | Delivered so far | Exit criteria |
| --- | --- | --- | --- |
| **C0 Foundations** | ✅ Done | Repo, `mise.toml` (Go 1.27), CI (static, race on Linux+macOS, MinIO tier, red-check; nightly fuzz/crash/slow/mutants), `tools/ci` run-all, `tools/redcheck`, `tools/mutate`, `core/dnx` + `core/dnx/compat` | Compat suite green against the pinned disknexus tag ✅ |
| **C1 Blobs and chunks** | 🔶 In progress | `core/hash`, `core/internal/wire`, `core/cdc`, `core/seal` (per-object HKDF keys, key files, KMS port + contract), `core/blob` port + contract, `blob/mem`, `blob/local`, `blob/multivol` | Blob contract green on all backends (3 of 4); crash harness passes ✅ (local, multivol) |
| **C2 Keyed data** | ⏳ | — | Determinism and bounded-diff properties hold on 1M entries |
| **C3 History and models** | ⏳ | — | A folder of files branches, diffs and merges end to end; model interface frozen |
| **C4 GC and hardening** | ⏳ | (per-object keys already landed in `seal`) | GC safety property holds; security table fully verified |

**C1 remaining:** `blob/s3` (pure-Go in-process S3 server for the local run,
startup probe, MinIO tier), local read cache, `core/pack`, `core/dedup`, the
chunk port + contract + `chunk/memstore` + `chunk/packstore`; and the coverage
gate (below).

**Open issue: coverage gate.** Statement coverage over the merged suite:
`core/blob/internal/fsutil` 75.6%, `core/blob/local` 84.7%,
`core/blob/multivol` 87.5% (gate 90%). The gaps are I/O error paths. They get
fault-injection tests (the Engine Spec's "on any error, roll back fully"), and
local's root/marker and multivol's volume-map decoders get fuzz targets, before
C1 closes. Every other package is at or above its gate (`cdc` and `wire` 100%,
`blob/mem` 98.4%, `hash` 94.1%, `blob` 93.5%, `dnx` 91.7% of an 80% gate,
`seal` 91.1%).

## Storage Core Spec checklists

### Blob backends
- [x] A shared `blob/contract` suite runs against every backend: put, get, range get, list paging, put-existing refused, root swap, stale swap refused (`core/blob/contract`; passing on `mem`, `local`, `multivol`; `s3` ⏳)
- [ ] 50 concurrent root swappers on `s3` (MinIO in CI): exactly one wins per round, none lost
- [x] kill -9 during `SwapRoot` on `local` and `multivol`, 1,000 times: reopened root is old or new, never torn (`TestCrashDuringSwapRootLeavesOldOrNew`; local run at 1,000 locally, both at 1,000 nightly)
- [ ] `s3` startup probe refuses an endpoint that ignores `If-Match`

### Chunkers
- [x] `cdc` via `core/dnx`: golden boundaries for a fixed 64 MiB corpus match a checked-in list (`TestCDCGoldenBoundariesRepoGeometry64MiB`, cross-checked against an independent reference implementation)
- [x] `cdc`: inserting 1 byte near the start of a 1 GiB file changes at most 3 chunks (`TestSlowOneByteInsertChangesAtMostThreeChunks1GiB`, `-tags slow`; 16 MiB on every run)
- [ ] `cdc`: stored bytes re-hash to their chunk id on every read path
- [ ] `prolly`: all Engine Spec L1 tests (determinism, history independence, bounded diff cost)
- [ ] A prolly value over the inline limit is stored via `cdc` and reads back byte-identical

### Object model and commit graph
- [ ] One commit holding a table, a blob and a JSON document round-trips all three
- [ ] Changing one file in a 100,000-path namespace reads fewer than 200 nodes to diff
- [ ] An object with an unregistered model id returns `ErrUnknownModel`; nothing is decoded
- [ ] Invalid paths (`a/../b`, empty segment, 256-byte segment, NUL) are rejected (property test)
- [ ] GC never deletes a chunk reachable from any ref or working set (property test), and does delete unreachable ones after the grace window

### Data-model plugins
- [ ] NewRegistry with two models sharing an id returns an error and no registry
- [ ] A repo opened with a registry that lacks a model its objects use refuses those objects with ErrUnknownModel
- [ ] `model/blob`: both sides edit one file → exactly one conflict; one side edits → clean merge
- [ ] `model/tree`: branch A deletes `x/`, branch B edits `x/y` → delete-vs-edit conflict on `x/y`
- [ ] Every registered model passes `model/contract`

### Security: disknexus gaps and how the core closes them
| disknexus behavior | Status | Evidence |
| --- | --- | --- |
| `InitRepo` creates dirs `0755`, files `0644` | ✅ | `core/blob/local` never calls `InitRepo`; `TestNothingIsWiderThanOwnerOnly` (under umask 0), `TestOpenRefusesAStoreOthersCanRead` |
| `EncryptNone` is a valid mode | 🔶 | `seal` has no plaintext mode; "opening a repo without a key fails" lands with `core/repo` (C3) |
| `Encrypt`/`Decrypt` accept no domain tag | ✅ | `dnx` refuses untagged calls (`TestAEADBindsTheDomainTag`); `seal` seals only under its `Domain` constants |
| Identity can be a hash of normalized bytes | ✅ | `TestChunkIdentityIsSHA256OfExactBytes` |
| Random 96-bit nonces under one master key | 🔶 | `seal`: per-object HKDF keys and a 2^20 seal budget per key; wiring into packs is `core/pack` (C1) |
| Index encryption unverified | ⏳ | `core/dedup` index objects, "no plaintext hash on disk" test (C1) |

### Security: core requirements
- [ ] AES-256-GCM for all data and metadata at rest (`seal` ✅; pack, index, root, config wiring ⏳)
- [x] Master keys from a KMS or Argon2id passphrase; keys never stored beside data (`seal.Wrapper` + contract, key files returned to the host, never written to a backend)
- [ ] SHA-256 verified on every read, from every backend, cached or not (chunk layer ⏳)
- [ ] Hand-written, bounds-checked decoders, fuzzed (`wire`, key file, KMS envelope ✅; local root/marker and multivol map fuzz targets ⏳; the rest as they land)
- [ ] Model ids resolved only against the compiled-in registry (C3)
- [ ] Every public call takes a `Principal`; default-deny `Authorizer` (C3)
- [ ] Hard limits: object size ✅, list page ✅, path depth, conflicts per merge, `Log` length (C3)
- [x] Supply chain: disknexus pinned by version and `go.sum`; `go mod verify`, `govulncheck` in CI
- [x] CI: `staticcheck` (via golangci-lint), `golangci-lint` warnings as errors, `govulncheck`, red-check

## Engine Spec L0–L3 checklists (moved into the Storage Core)

### L0 chunk store (lands at the chunk layer: `chunk/memstore`, `chunk/packstore`)
- [ ] `Put` then `Get` returns identical bytes
- [ ] `Get` on an unknown hash returns `ErrNotFound`
- [ ] `Put` of the same bytes twice returns the same hash and stores one copy
- [ ] Flipping one byte on disk makes `Get` return `ErrCorrupt`
- [ ] `CompareAndSetRoot` with a stale `expected` returns `ErrRootConflict` and leaves the root unchanged
- [ ] 100 goroutines racing `CompareAndSetRoot`: exactly one wins per round (✅ at the blob layer)
- [ ] Crash harness: kill mid-write 1,000 times; reopened store is at the old or new root (✅ at the blob layer)
- [ ] `Put` of 1 MiB + 1 byte returns `ErrTooLarge`

### L0 backends
- [ ] Every backend runs the full contract suite in CI (S3 against MinIO per push; nightly real S3) — mem, local, multivol ✅
- [x] filestore: starting on an SMB, NFS, or unrecognized filesystem fails (`TestOnlyAllowlistedFilesystems`)
- [x] multistore: 4 volumes × 10,000 files, each 25% ± 3% (`TestPlacementIsEvenAcrossFourVolumes`)
- [x] multistore: adding a 5th volume moves no existing files, ~20% of new files go to it (`TestAddingAVolumeMovesNothingAndTakesAFifth`)
- [x] multistore: unmounting a secondary makes the store read-only with `ErrVolumeMissing`; no writes land (`TestMissingSecondaryMakesTheStoreReadOnly`, plus `TestVolumeSwappedWhileOpenGoesReadOnly`)
- [x] multistore: a volume whose marker UUID doesn't match is refused (`TestSwappedVolumeIsRefused`)
- [x] multistore: kill -9 during commit; old or new root (`TestCrashDuringSwapRootLeavesOldOrNew`)
- [ ] s3store: 50 concurrent committers; no lost updates; every published root fully readable
- [ ] s3store: forced 412 follows the retry path; after 10 conflicts `ErrRootConflict`, manifest unchanged
- [ ] s3store: a tampered table file returns `ErrCorrupt`
- [ ] s3store: an endpoint that ignores `If-Match` is refused by the startup probe
- [ ] s3store: a single-row commit stays within its request budget

### L1 prolly tree, L2 version graph, L3 diff and merge
All ⏳ (C2, C3). The items are in `docs/specs/engine-spec.md`; they will be
listed here with their tests as they land.

## Decisions made while building (see also `docs/DESIGN.md` §1–2)

- **local put-if-absent is temp + fsync + `link(2)`**, not `O_CREAT|O_EXCL` on the final name: equally atomic, and never exposes or strands a torn object.
- **local object files are `<segment>~`** so a name and a longer name under it (`a/b`, `a/b/c`) coexist, as the contract requires.
- **multivol volumes are local stores**, so each inherits the allowlist, marker, permissions and crash safety; the volume map and root live on the primary.
- **Backfills carry a `Red-Check: mutants` trailer**: a test for behavior that already exists proves itself with a mutant it kills.
- **Coverage is measured over the merged `-coverpkg` profile**, so `core/dnx` counts its compat suite.
- **The 1 GiB CDC property runs under `-tags slow`**; the normal suite checks it at 16 MiB.
- **Not mutation-provable here:** removing an `fsync`. kill -9 keeps the page cache, so only a power cut would show it.

## What mutation testing has found so far

| Mutant | What it showed | Fix |
| --- | --- | --- |
| `dnx-open-requires-tag` survived | The test opened a *tagged* ciphertext without a tag, which fails authentication anyway | Test now uses an untagged ciphertext built with the stdlib |
| wire LenBytes pre-check survived | The guard was redundant (Fixed already refuses without allocating) | Guard deleted |
| `seal-hkdf-binds-domain` survived | Domain separation is two layers; a behavioral test can't see either alone | Format-pinning tests re-derive keys and AAD with the stdlib |
| `seal-x25519-wrap-only` survived | disknexus rejects a nil key anyway; nothing asserted the error | `ErrWrapOnly`, asserted |
| identity check (multivol) | No test swapped a volume while open | `TestVolumeSwappedWhileOpenGoesReadOnly` |
| (redcheck on this branch) | Build-tagged tests unjudged; `TestMain` judged; pairs not matched by scope; a changed passing test | Tool fixed three times; the test made meaningful |

## Next

1. Fault-injection tests and decoder fuzz targets for the disk backends (coverage gate).
2. `blob/s3` with a pure-Go in-process S3 server, the startup probe, and the MinIO tier.
3. `core/pack`, `core/dedup`, the chunk port and its implementations; then close C1.
