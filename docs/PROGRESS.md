# Progress

Where the storage core stands against the Storage Core Spec's milestones and
every "first failing test" checkbox in both specs. Updated at each milestone
boundary and whenever a checklist item turns green; the evidence for each item
is the named test, and the commit that added it carries its red.

**Updated 2026-09-23** · branch `storage-core` (local, not pushed) · 102 commits
· red-check clean · 181 checked-in mutants, all killed · lint clean · every
package at or above its coverage gate

## Milestones

| Milestone | Status | Delivered | Exit criteria |
| --- | --- | --- | --- |
| **C0 Foundations** | ✅ Done | Repo, `mise.toml` (Go 1.27), CI (static, race on Linux+macOS, MinIO tier, red-check; nightly fuzz/crash/slow/mutants), `tools/ci` run-all, `tools/redcheck`, `tools/mutate`, `core/dnx` + `core/dnx/compat` | Compat suite green against the pinned disknexus tag ✅ |
| **C1 Blobs and chunks** | ✅ Done | `core/hash`, `core/internal/wire`, `core/cdc`, `core/seal` (per-object HKDF keys, key files, KMS port + contract); `core/blob` port + contract, `blob/mem`, `blob/local`, `blob/multivol`, `blob/s3` (pure-Go in-process S3 server `s3fake`, startup probe, MinIO tier), `blob/cache`; `core/pack`, `core/dedup`; `core/chunk` port + contract, `chunk/memstore`, `chunk/packstore` | Blob contract green on all backends ✅ (mem, local, multivol, s3 in-process and MinIO, and through the cache); crash harness passes ✅ (local and multivol at the blob layer, packstore at the chunk layer) |
| **C2 Keyed data** | ⏳ Next | — | Determinism and bounded-diff properties hold on 1M entries |
| **C3 History and models** | ⏳ | — | A folder of files branches, diffs and merges end to end; model interface frozen |
| **C4 GC and hardening** | ⏳ | (per-object keys, and the manifest's gcGen and condemned list, already landed) | GC safety property holds; security table fully verified |

**Closing C1** took more than the last component. The coverage gate found
guards nothing executed, each now backfilled with a test and the mutant it
kills (the cache's restart cleanup, pass-through and write failures; the
pack writer's finished state; the manifest, the one sealed format without a
golden file, forgery tests or fuzz target). The chunk port gained a rule it
had left open (a closed store refuses every call) and the blob contract one
it had assumed (a root read during swaps is whole). The full mutation run
caught a mutant the crash harness cannot kill and two stale ones; the full
red-check caught a scope mismatch in an early commit.

**Coverage gate: met.** Statement coverage over the merged suite (`go run ./tools/ci -only cover`):

| Package | Coverage | Gate |
| --- | --- | --- |
| `cdc`, `internal/wire` | 100% | 90% |
| `blob/mem`, `chunk/memstore` | 98.4% | 90% |
| `hash` | 94.1% | 90% |
| `blob` | 93.5% | 90% |
| `blob/cache` | 93.0% | 90% |
| `blob/multivol` | 92.6% | 90% |
| `pack` | 92.3% | 90% |
| `blob/local` | 92.2% | 90% |
| `dnx` | 91.7% | 80% |
| `chunk/packstore` | 91.4% | 90% |
| `seal` | 91.1% | 90% |
| `dedup` | 91.0% | 90% |
| `blob/internal/fsutil` | 90.7% | 90% |
| `blob/s3` | 86.1% | 80% |

The disk backends got there through fault-injection tests (the Engine Spec's
"on any error, roll back fully"), the cache and chunk layer through
backfills that each name a mutant, not by testing trivia. Contract and fake
packages have no gate: their callers exercise them.

## Storage Core Spec checklists

### Blob backends
- [x] A shared `blob/contract` suite runs against every backend: put, get, range get, list paging, put-existing refused, root swap, stale swap refused, plus atomic puts and whole root reads under concurrent swaps (`core/blob/contract`; passing on `mem`, `local`, `multivol`, `s3` against the in-process server and MinIO, and through `blob/cache`)
- [x] 50 concurrent root swappers on `s3` (MinIO in CI): exactly one wins per round, none lost (`ConcurrentSwappersOneWinnerPerRound` with 50 swappers in `TestContractAgainstTheInProcessServer` and `TestContractAgainstRealS3`)
- [x] kill -9 during `SwapRoot` on `local` and `multivol`, 1,000 times: reopened root is old or new, never torn (`TestCrashDuringSwapRootLeavesOldOrNew`; 1,000 nightly)
- [x] `s3` startup probe refuses an endpoint that ignores `If-Match` (`TestProbeRefusesEndpointsThatIgnoreConditionalWrites`)

### Chunkers
- [x] `cdc` via `core/dnx`: golden boundaries for a fixed 64 MiB corpus match a checked-in list (`TestCDCGoldenBoundariesRepoGeometry64MiB`, cross-checked against an independent reference implementation)
- [x] `cdc`: inserting 1 byte near the start of a 1 GiB file changes at most 3 chunks (`TestSlowOneByteInsertChangesAtMostThreeChunks1GiB`, `-tags slow`; 16 MiB on every run)
- [x] `cdc`: stored bytes re-hash to their chunk id on every read path: unfinished pack, in-flight pack, cache and backend (chunk contract `IdentityIsSHA256` and `FlippedByteIsCorrupt` on every backend, `TestCachedReadsAreVerified`, `TestFailedUploadStaysReadableAndIsRetried`)
- [ ] `prolly`: all Engine Spec L1 tests (determinism, history independence, bounded diff cost) (C2)
- [ ] A prolly value over the inline limit is stored via `cdc` and reads back byte-identical (C2)

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
| `InitRepo` creates dirs `0755`, files `0644` | ✅ | `core/blob/local` never calls `InitRepo`; `TestNothingIsWiderThanOwnerOnly` (under umask 0), `TestOpenRefusesAStoreOthersCanRead`; the disk cache is owner-only (`TestCacheFilesAreOwnerOnly`) |
| `EncryptNone` is a valid mode | 🔶 | `seal` has no plaintext mode; "opening a repo without a key fails" lands with `core/repo` (C3) |
| `Encrypt`/`Decrypt` accept no domain tag | ✅ | `dnx` refuses untagged calls (`TestAEADBindsTheDomainTag`); `seal` seals only under its `Domain` constants |
| Identity can be a hash of normalized bytes | ✅ | `TestChunkIdentityIsSHA256OfExactBytes`; chunk contract `IdentityIsSHA256` |
| Random 96-bit nonces under one master key | ✅ | Every pack, index object and manifest has its own salt and so its own HKDF keys (`TestSaltsAreFreshPerPack`); a key's seal budget is 2^20 and a pack holds at most 2^16 chunks |
| Index encryption unverified | ✅ | Index objects sealed under `vdb/index/v1` and bound to their repository (`TestNoPlaintextHashInAnIndexObject`, `TestIndexObjectsRefuseTamperingAndStrangers`); nothing plaintext reaches a backend (`TestNoPlaintextOnDisk`) |

### Security: core requirements
- [ ] AES-256-GCM for all data and metadata at rest (chunks, pack indexes, index objects and the manifest ✅; the repository config object lands with `core/repo` in C3)
- [x] Master keys from a KMS or Argon2id passphrase; keys never stored beside data (`seal.Wrapper` + contract, key files returned to the host, never written to a backend)
- [x] SHA-256 verified on every read, from every backend, cached or not (chunk contract on every backend, `TestCachedReadsAreVerified`; disk-cache entries carry their own SHA-256, `TestDamagedEntryIsRefetched`)
- [ ] Hand-written, bounds-checked decoders, fuzzed (`wire`, key file, KMS envelope, local root file and marker, multivol volume map, pack, index object and manifest ✅, each sealed format also with a v1 golden file and forgery tests; the rest as they land)
- [ ] Model ids resolved only against the compiled-in registry (C3)
- [ ] Every public call takes a `Principal`; default-deny `Authorizer` (C3)
- [ ] Hard limits: object size ✅, list page ✅, chunk size ✅, pack size and chunks per pack ✅, index objects per manifest ✅, path depth, conflicts per merge, `Log` length (C3)
- [x] Supply chain: disknexus pinned by version and `go.sum`; `go mod verify`, `govulncheck` in CI
- [x] CI: `staticcheck` (via golangci-lint), `golangci-lint` warnings as errors, `govulncheck`, red-check

## Engine Spec L0–L3 checklists (moved into the Storage Core)

### L0 chunk store (`chunk/contract`, run on `chunk/memstore` and on `chunk/packstore` over every backend)
- [x] `Put` then `Get` returns identical bytes (`PutGetIdentical`)
- [x] `Get` on an unknown hash returns `ErrNotFound` (`UnknownIsNotFound`)
- [x] `Put` of the same bytes twice returns the same hash and stores one copy (`SameBytesAreOneChunk`)
- [x] Flipping one byte on disk makes `Get` return `ErrCorrupt` (`FlippedByteIsCorrupt`, through the raw backend)
- [x] `CompareAndSetRoot` with a stale `expected` returns `ErrRootConflict` and leaves the root unchanged (`StaleCASChangesNothing`)
- [x] 100 goroutines racing `CompareAndSetRoot`: exactly one wins per round (`RacingCASOneWinnerPerRound`; 50 on s3)
- [x] Crash harness: kill mid-write 1,000 times; reopened store is at the old or new root (`TestCrashDuringCommitLeavesOldOrNew`, 1,000 nightly)
- [x] `Put` of 1 MiB + 1 byte returns `ErrTooLarge` (`TooLarge`)
- [x] (port rule) A closed store refuses every call with `ErrClosed` (`ClosedRefusesEveryCall`)

### L0 backends
- [ ] Every backend runs the full contract suite in CI (S3 against MinIO per push; nightly real S3): mem, local, multivol, s3 against MinIO ✅; the nightly real-S3 run needs credentials configured as CI secrets
- [x] filestore: starting on an SMB, NFS, or unrecognized filesystem fails (`TestOnlyAllowlistedFilesystems`)
- [x] multistore: 4 volumes × 10,000 files, each 25% ± 3% (`TestPlacementIsEvenAcrossFourVolumes`)
- [x] multistore: adding a 5th volume moves no existing files, ~20% of new files go to it (`TestAddingAVolumeMovesNothingAndTakesAFifth`)
- [x] multistore: unmounting a secondary makes the store read-only with `ErrVolumeMissing`; no writes land (`TestMissingSecondaryMakesTheStoreReadOnly`, plus `TestVolumeSwappedWhileOpenGoesReadOnly`)
- [x] multistore: a volume whose marker UUID doesn't match is refused (`TestSwappedVolumeIsRefused`)
- [x] multistore: kill -9 during commit; old or new root (`TestCrashDuringSwapRootLeavesOldOrNew`)
- [x] s3store: 50 concurrent committers; no lost updates; every published root fully readable (`TestContractOverEveryBackend/s3` with 50 racers, `TestWritersOnOneBackendSeeEachOther`)
- [x] s3store: forced 412 follows the retry path; after 10 conflicts `ErrRootConflict`, manifest unchanged (`TestManifestConflictsRetryThenGiveUp`)
- [x] s3store: a tampered table file returns `ErrCorrupt` (`TestContractOverEveryBackend/s3/FlippedByteIsCorrupt`)
- [x] s3store: an endpoint that ignores `If-Match` is refused by the startup probe (`TestProbeRefusesEndpointsThatIgnoreConditionalWrites`)
- [x] s3store: a single-row commit stays within its request budget (`TestSmallCommitRequestBudget`: at most 4 requests, 3 of them PUTs)

### L1 prolly tree, L2 version graph, L3 diff and merge
All ⏳ (C2, C3). The items are in `docs/specs/engine-spec.md`; they will be
listed here with their tests as they land.

## Decisions made while building (see also `docs/DESIGN.md` §1–2, §6)

- **local put-if-absent is temp + fsync + `link(2)`**, not `O_CREAT|O_EXCL` on the final name: equally atomic, and never exposes or strands a torn object.
- **local object files are `<segment>~`** so a name and a longer name under it (`a/b`, `a/b/c`) coexist, as the contract requires.
- **S3 object keys end in `!`**: MinIO hides a key that is also a path prefix of another (`a/b` beside `a/b/c`) from listings.
- **multivol volumes are local stores**, so each inherits the allowlist, marker, permissions and crash safety; the volume map and root live on the primary.
- **The chunk layer publishes by manifest swap**: packs and one index object per commit are durable before the sealed manifest names the new root; a swap that loses only to a manifest change is retried (at most 10), a moved root is the caller's conflict at once.
- **A closed chunk store refuses every call** (`ErrClosed`), so a caller holding one past `Close` fails loudly instead of reaching a released backend.
- **Backfills carry a `Red-Check: mutants` trailer**: a test for behavior that already exists proves itself with a mutant it kills.
- **redcheck judges a contract change on the tests that run the contract**; a caller that skips for want of an environment (the MinIO tier) is not judged, but a change every caller skipped is refused. With no base named it checks from `main`, else the root commit.
- **Coverage is measured over the merged `-coverpkg` profile**, so `core/dnx` counts its compat suite.
- **The 1 GiB CDC property runs under `-tags slow`**; the normal suite checks it at 16 MiB.
- **Not mutation-provable here:** removing an `fsync`. kill -9 keeps the page cache, so only a power cut would show it.
- **Equivalent mutants, not catalogued:** the cache's `Dir` check (`MkdirAll("")` fails anyway) and its `MkdirAll` error path (the `Chmod` after it fails); the pack index's per-entry read check (the reader's error is sticky and `Done` reports it); `OpenFrame`'s length check (authentication fails anyway); the manifest's count limits (the read fails at the first missing entry). Their statements are covered; no test can tell the mutant from the original.

## What testing has found so far

| Found by | What it showed | Fix |
| --- | --- | --- |
| mutant `dnx-open-requires-tag` survived | The test opened a *tagged* ciphertext without a tag, which fails authentication anyway | Test now uses an untagged ciphertext built with the stdlib |
| wire LenBytes pre-check survived | The guard was redundant (Fixed already refuses without allocating) | Guard deleted |
| mutant `seal-hkdf-binds-domain` survived | Domain separation is two layers; a behavioral test can't see either alone | Format-pinning tests re-derive keys and AAD with the stdlib |
| mutant `seal-x25519-wrap-only` survived | disknexus rejects a nil key anyway; nothing asserted the error | `ErrWrapOnly`, asserted |
| multivol identity check survived | No test swapped a volume while open | `TestVolumeSwappedWhileOpenGoesReadOnly` |
| pack region/header check survived | Redundant with the offset walk; two forgeries tested the wrong thing | Check removed, forgeries fixed |
| cache recency survived | No test checked that a hit makes an entry recent | `TestHitsRefreshRecency` |
| dedup, fsutil, multivol, local, s3, packstore survivors | Guards with no test: duplicate index entries, temp cleanup after a failed rename, rollback and create-over-map, I/O errors read as benign states | A forgery, a rename-failure test, fixtures, fault-injection tests |
| MinIO tier | MinIO hides `a/b/c` from listings when object `a/b` exists | `!` suffix on S3 object keys |
| a red test | packstore's upload dropped the rest of a batch after the first failed pack: a chunk the next root reaches that nothing durable held | Every failed pack kept and retried (`TestFailedUploadStaysReadableAndIsRetried`) |
| the chunk contract | Closed stores kept answering some calls, differently per implementation | Port rule and `ClosedRefusesEveryCall` |
| mutant `local-root-written-by-rename` survived 1,000 kills | A kill almost never lands between truncate and write | Blob contract `RootReadsDuringSwapsAreWhole` catches it every run |
| full mutation run | Two mutants whose code had moved (a lint comment, a refactor) were invalid, not killed | Re-anchored; the full run is part of closing a milestone |
| (redcheck on this branch) | Build-tagged tests unjudged; `TestMain` judged; pairs not matched by scope; contract changes invisible; environment-bound callers refused; no `main` in a new clone | Tool fixed each time, with a red test |

## Next

1. **C2 keyed data**: `core/boundary` (the integer split threshold, DESIGN §7),
   `core/prolly` (Map, Editor, DiffIter; every Engine L1 test, the 1M-entry
   properties under `-tags slow`), `core/stream` (CDC byte streams under a
   content-defined index tree; prolly values over 256 KiB).
2. **Before the PR**: this clone has no `main` (its first branch is
   `storage-core`, rooted at the scaffold commit). Opening the one PR needs a
   base branch on GitHub; to settle when the push is asked for.
