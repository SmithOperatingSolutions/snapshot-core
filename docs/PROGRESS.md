# Progress

Where the storage core stands against the Storage Core Spec's milestones and
every "first failing test" checkbox in both specs. Updated at each milestone
boundary and whenever a checklist item turns green; the evidence for each item
is the named test, and the commit that added it carries its red.

**Updated 2026-09-23** · red-check clean · every checked-in mutant killed ·
lint clean · every package at or above its coverage gate

## Milestones

| Milestone | Status | Delivered | Exit criteria |
| --- | --- | --- | --- |
| **C0 Foundations** | ✅ Done | Repo, `mise.toml` (Go 1.27), CI (static, race on Linux+macOS, MinIO tier, red-check; weekly fuzz/crash/slow/mutants), `tools/ci` run-all, `tools/redcheck`, `tools/mutate`, `core/dnx` + `core/dnx/compat` | Compat suite green against the pinned disknexus tag ✅ |
| **C1 Blobs and chunks** | ✅ Done | `core/hash`, `core/internal/wire`, `core/cdc`, `core/seal` (per-object HKDF keys, key files, KMS port + contract); `core/blob` port + contract, `blob/mem`, `blob/local`, `blob/multivol`, `blob/s3` (pure-Go in-process S3 server `s3fake`, startup probe, MinIO tier), `blob/cache`; `core/pack`, `core/dedup`; `core/chunk` port + contract, `chunk/memstore`, `chunk/packstore` | Blob contract green on all backends ✅ (mem, local, multivol, s3 in-process and MinIO, and through the cache); crash harness passes ✅ (local and multivol at the blob layer, packstore at the chunk layer) |
| **C2 Keyed data** | ✅ Done | `core/boundary` (the integer split rule), `core/stream` (CDC byte streams under a content-defined index tree), `core/prolly` (Map, Editor with incremental Flush, Diff) | Determinism and bounded-diff properties hold on 1M entries ✅ (`-tags slow`, about 9 s) |
| **C3 History and models** | ✅ Done | `core/auth` (Principal, default-deny Authorizer), `core/model` (the port, frozen, and the registry), `core/object` (46-byte object references, the path grammar, namespaces and their diff), `model/contract`, `model/blob`, `model/tree`, `core/merge` (the zipped three-way driver), `core/vcs` (refs, commits, tags, working sets, merge base, log, merge and conflicts), `core/repo` (Init and Open over the sealed config object) | A folder of files branches, diffs and merges end to end ✅ (`TestAFolderBranchesDiffsAndMerges`, on a local disk store through encrypted packs); model interface frozen ✅ (`core/model`, port version 1) |
| **C4 GC and hardening** | ✅ Done | Walks: `stream.Walk`, `prolly.Walk`, `object.Walk`, `vcs.Walk`, and `model.Walker` (optional, beside the frozen port) in `model/blob` and `model/tree`; packstore's GC rounds (condemn, reprieve, expire, compact) and the writer's fence (a lost session, `chunk.ErrSessionLost`, since #3); `core/gc` (mark, apply, delete, orphans by the backend's clock); `repo.GC`; the repository on `NoDelete`; writes authorized per path; fuzz targets for every decoder | GC safety property holds ✅ (`TestGCSafetyProperty` over random histories; `TestAFoldersHistoryComesThroughGC` end to end); security table fully verified ✅ (below) |

**Closing C1** took more than the last component. The coverage gate found
guards nothing executed, each now backfilled with a test and the mutant it
kills (the cache's restart cleanup, pass-through and write failures; the
pack writer's finished state; the manifest, the one sealed format without a
golden file, forgery tests or fuzz target). The chunk port gained a rule it
had left open (a closed store refuses every call) and the blob contract one
it had assumed (a root read during swaps is whole). The full mutation run
caught a mutant the crash harness cannot kill and two stale ones; the full
red-check caught a scope mismatch in an early commit.

**Closing C2** was mostly the mutation check finding tests that did not
reach what they claimed: the split rule enforced its maximum twice (one
copy removed); the stream reader had three guards against forgeries that
would otherwise read back with no error at all; the rapid properties drew
almost only single-leaf trees until they drew seeded large sets and had to
prove they reached height 2; Flush's resync only changed cost, so a read
bound now holds it; and every operation's store-error paths were dark
until a store failing exactly one read or write at every point showed
each error surfacing. One real defect: a flush that changed nothing still
re-stored the nodes it re-chunked (now fixed). A mutant that hangs led to
a per-mutant timeout in the mutation engine.

**Closing C3** began at the coverage gate: four packages under 90%, nearly
all of it untested error paths and refusals. Backfilling them found three
real defects. `UpdateWorkingSet` stored whatever merge state it was handed,
so a host could drop a merge's conflicts, or empty them, and commit past
them, or name a commit never merged as the second parent; it now refuses
to change a merge in progress (`ErrMergeState`). Merging a branch's own
head started a merge whose commit named the head as both parents, which
the decoder refuses, so the branch could never be read again; merging what
a branch already holds is now a no-op. An `Init` that stopped between
writing the config and writing the refs left a store that neither `Init`
(the config was there) nor `Open` (no refs) would take; the next `Init`
now finishes it. Two test weaknesses showed too: `DenyAll` cannot tell
which check stopped a call (a reader could have resolved conflicts
unseen), so authorization is also tested with read-only permission; and
a conflict list of one node never exercised reading it.

**Closing C4** was mostly design under test. The walk needed a leaf flag:
the same bytes can be a file in one place and a node in another, and a
marker pruning on "seen" alone would skip a node first met as a file, so
GC would delete what it reaches; a writer can craft that on purpose, and
`TestGCIsNotSteeredByAFileThatIsANode` runs the attack. The version graph's
walk reads its own chunks once each and refuses one named in two roles.
Orphan ages were read against GC's clock but stamped by the backend's, so
a backend running behind would have deleted a writer's fresh upload; they
are now measured by the backend's clock, read from a probe. Writers that
counted on a pack GC expired are fenced (`chunk.ErrStale`, a conflict to
the host), and a store rebuilds its index when packs expire. The mutation
check found test fixtures too kind: a vcs walk fixture where the merge's
theirs and the conflict's ours were reachable some other way; an e2e run
at a pack size where file data shares packs with live nodes, so an
under-marking walk deleted nothing visible; a property that never
exercised the fence until an edit spanned two collections. A flaky test
counted objects by a prefix a real pack name shares one run in sixty.
Writes are now authorized per path, against what is stored.

**Coverage gate: met.** Statement coverage over the merged suite (`go run ./tools/ci -only cover`):

| Package | Coverage | Gate |
| --- | --- | --- |
| `auth`, `cdc`, `internal/wire`, `model`, `object` | 100% | 90% |
| `gc` | 98.7% | 90% |
| `model/tree` | 98.5% | 90% |
| `blob/mem`, `chunk/memstore` | 98.4% | 90% |
| `prolly` | 97.8% | 90% |
| `stream` | 97.7% | 90% |
| `repo` | 97.2% | 90% |
| `vcs` | 97.1% | 90% |
| `boundary` | 97.1% | 90% |
| `model/blob` | 96.6% | 90% |
| `merge` | 95.0% | 90% |
| `hash` | 94.1% | 90% |
| `blob` | 93.5% | 90% |
| `chunk/packstore` | 93.5% | 90% |
| `blob/cache` | 93.0% | 90% |
| `blob/multivol` | 92.6% | 90% |
| `pack` | 92.3% | 90% |
| `blob/local` | 92.2% | 90% |
| `dnx` | 91.7% | 80% |
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
- [x] kill -9 during `SwapRoot` on `local` and `multivol`, 1,000 times: reopened root is old or new, never torn (`TestCrashDuringSwapRootLeavesOldOrNew`; 1,000 weekly)
- [x] `s3` startup probe refuses an endpoint that ignores `If-Match` (`TestProbeRefusesEndpointsThatIgnoreConditionalWrites`)

### Chunkers
- [x] `cdc` cuts disknexus's boundaries: golden boundaries for a fixed 64 MiB corpus match a checked-in list (`TestCDCGoldenBoundariesRepoGeometry64MiB`, cross-checked against an independent reference implementation), and `core/cdc`'s own Buzhash cuts where disknexus cuts byte for byte over every stream shape and reader behaviour (`TestTheChunkerCutsWhereDisknexusCuts`, #10)
- [x] `cdc`: inserting 1 byte near the start of a 1 GiB file changes at most 3 chunks (`TestSlowOneByteInsertChangesAtMostThreeChunks1GiB`, `-tags slow`; 16 MiB on every run)
- [x] `cdc`: stored bytes re-hash to their chunk id on every read path: unfinished pack, in-flight pack, cache and backend (chunk contract `IdentityIsSHA256` and `FlippedByteIsCorrupt` on every backend, `TestCachedReadsAreVerified`, `TestFailedUploadStaysReadableAndIsRetried`)
- [x] `prolly`: all Engine Spec L1 tests (determinism, history independence, bounded diff cost); see the L1 list below
- [x] A prolly value over the inline limit is stored via `cdc` and reads back byte-identical (`TestAValueOverTheInlineLimitIsAStream`)

### Object model and commit graph
- [x] One commit holding a table, a blob and a JSON document round-trips all three (`TestACommitHoldingThreeModelsRoundTrips`; the table and document models are fakes here, being the consuming repository's)
- [x] Changing one file in a 100,000-path namespace reads fewer than 200 nodes to diff (`TestChangingOneFileInA100kNamespaceReadsFewNodes`)
- [x] An object with an unregistered model id returns `ErrUnknownModel`; nothing is decoded (`TestAnUnregisteredModelIsUnknownAndNothingIsRead`: nothing of it is even read)
- [x] Invalid paths (`a/../b`, empty segment, 256-byte segment, NUL) are rejected (property test) (`TestInvalidPathsAreRejected`, and the rapid property `TestPathsAgreeWithTheGrammar` against an independent statement of the grammar)
- [x] GC never deletes a chunk reachable from any ref or working set (property test), and does delete unreachable ones after the grace window (`TestGCSafetyProperty`: random histories with GC between the steps as the clock moves on, the whole repository read back after every run, nothing left to condemn at the end; `TestGCKeepsWhatTheRefsReachAndDeletesTheRest`, `TestAFoldersHistoryComesThroughGC` on a disk store with the real models)

### Data-model plugins
- [x] NewRegistry with two models sharing an id returns an error and no registry (`TestNewRegistryRefusesTwoModelsWithOneID`)
- [x] A repo opened with a registry that lacks a model its objects use refuses those objects with ErrUnknownModel (`TestAnUnregisteredModelIsUnknownAndNothingIsRead`, `TestResolveKnowsOnlyItsModelsAndFormats`; a format newer than the model knows is refused the same way, `TestDetailNeverHandsAModelAnUnknownFormat`)
- [x] `model/blob`: both sides edit one file → exactly one conflict; one side edits → clean merge (`TestBothSidesEditingIsOneConflict`)
- [x] `model/tree`: branch A deletes `x/`, branch B edits `x/y` → delete-vs-edit conflict on `x/y` (`TestDeletingADirectoryAgainstAnEditIsAConflict`)
- [x] Every registered model passes `model/contract` (`TestContract` in `model/blob` and `model/tree`: identity, round trip, determinism, diff against the edits made, merge identities, garbage refused)

### Security: disknexus gaps and how the core closes them
| disknexus behavior | Status | Evidence |
| --- | --- | --- |
| `InitRepo` creates dirs `0755`, files `0644` | ✅ | `core/blob/local` never calls `InitRepo`; `TestNothingIsWiderThanOwnerOnly` (under umask 0), `TestOpenRefusesAStoreOthersCanRead`; the disk cache is owner-only (`TestCacheFilesAreOwnerOnly`) |
| `EncryptNone` is a valid mode | ✅ | `seal` has no plaintext mode, and a repository is neither created nor opened without a key (`TestThereIsNoRepositoryWithoutAConfigOrAKey`); a destroyed key creates and opens nothing (`TestARefusedInitWritesNothing`) |
| `Encrypt`/`Decrypt` accept no domain tag | ✅ | `dnx` refuses untagged calls (`TestAEADBindsTheDomainTag`); `seal` seals only under its `Domain` constants |
| Identity can be a hash of normalized bytes | ✅ | `TestChunkIdentityIsSHA256OfExactBytes`; chunk contract `IdentityIsSHA256` |
| Random 96-bit nonces under one master key | ✅ | Every pack, index object and manifest has its own salt and so its own HKDF keys (`TestSaltsAreFreshPerPack`); a key's seal budget is 2^20 and a pack holds at most 2^16 chunks |
| Index encryption unverified | ✅ | Index objects sealed under `vdb/index/v1` and bound to their repository (`TestNoPlaintextHashInAnIndexObject`, `TestIndexObjectsRefuseTamperingAndStrangers`); nothing plaintext reaches a backend (`TestNoPlaintextOnDisk`) |

### Security: core requirements
- [x] AES-256-GCM for all data and metadata at rest (chunks, pack indexes, index objects, the manifest, and the repository config: `TestTheConfigIsTheDocumentedFormat`)
- [x] Master keys from a KMS or Argon2id passphrase; keys never stored beside data (`seal.Wrapper` + contract, key files returned to the host, never written to a backend)
- [x] SHA-256 verified on every read, from every backend, cached or not (chunk contract on every backend, `TestCachedReadsAreVerified`; disk-cache entries carry their own SHA-256, `TestDamagedEntryIsRefetched`)
- [x] Hand-written, bounds-checked decoders, fuzzed: `wire`, key file, KMS envelope, local root file and marker, multivol volume map, pack, index object, manifest, stream index node, prolly node, commit, tag, working set, conflict record, tree entry, object reference (`FuzzDecodeRef`) and repository config (`FuzzOpenConfig`); each sealed format also with a v1 golden file, every format with forgery tests, and CI's fuzz step lists the targets itself
- [x] Model ids resolved only against the compiled-in registry (registries are built explicitly by the host, `model.NewRegistry`; nothing registers itself, and an id or format the registry lacks is `ErrUnknownModel` before anything is read)
- [x] Every public call takes a `Principal`; the core passes it to a default-deny `Authorizer` per branch and per path prefix. Per branch, tag and repository (`TestEveryCallIsAuthorized` over every version-graph call under a recording, a denying, a nil and a read-only authorizer; `Init` and `repo.GC` ask for admin, `TestARefusedInitWritesNothing`, `TestAnAdminCollectsTheRepositoryOnTheRawStore`; a nil authorizer denies, `TestDenyAllRefusesEveryCall`); per path, every write asks for each path it changes, checked against what is stored (`TestWritesAreAuthorizedPerPath`). Argued in DESIGN §8: `Namespace` opens what a hash names without asking, and reads stay per branch (a host holding the chunk store reads what it can open)
- [x] Hard limits: object size, list page, chunk size, pack size and chunks per pack, index objects per manifest, key size, inline value size, tree height; path depth and length (`TestInvalidPathsAreRejected`), conflicts per merge (`TestTooManyConflictsAbort`, default 100,000), `Log` length (`TestLogIsBoundedAndHighestFirst`), commit and tag messages (`TestMessagesAreBoundedUTF8`), principal ids
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
- [x] Crash harness: kill mid-write 1,000 times; reopened store is at the old or new root (`TestCrashDuringCommitLeavesOldOrNew`, 1,000 weekly)
- [x] `Put` of 1 MiB + 1 byte returns `ErrTooLarge` (`TooLarge`)
- [x] (port rule) A closed store refuses every call with `ErrClosed` (`ClosedRefusesEveryCall`)

### L0 backends
- [x] Every backend runs the full contract suite in CI (S3 against MinIO per push; real S3 weekly): mem, local, multivol, s3 against MinIO on every push; the real provider weekly (`TestContractAgainstRealS3`, `TestARepositoryRunsOnTheRealProvider`, green in the weekly run of 2026-09-23; #4)
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

### L1 prolly tree (`core/prolly`; 1M-entry versions under `-tags slow`)
- [x] Empty map has a fixed, documented root hash (`TestEmptyMapHasTheDocumentedRoot`)
- [x] Put then Get returns the value; Get of a missing key returns `ok=false` (`TestPutThenGet`)
- [x] IterRange yields keys in byte order and respects both bounds (`TestIterRangeRespectsOrderAndBounds`)
- [x] Determinism (property): any shuffled order yields the same root (`TestDeterminismProperty`, `TestSlowDeterminismOn1MEntries`)
- [x] History independence (property): insert then delete k is the same root as never inserting k (`TestHistoryIndependenceProperty`)
- [x] Changing one value in a 1M-entry map rewrites at most tree-height + 1 nodes: exactly that many (`TestOneValueChangeRewritesHeightPlusOneNodes`, `TestSlowOneValueChangeOn1MEntries`)
- [x] Diff of maps differing by n entries returns exactly those n and reads O(n log N) nodes: at most 4·n·(height+1) (`TestDiffReturnsExactlyTheChangesAndReadsLittle`, `TestSlowDiffOn1MEntriesReadsLittle`)
- [x] Node sizes stay within 512 B – 16 KiB across 1M random entries (`TestNodeSizesStayWithinBounds`, `TestSlowNodeSizesOn1MRandomEntries`)
- [x] Fuzz the node decoder (`FuzzDecodeNode`)
- [x] Oversized key returns `ErrKeyTooLarge`; no partial write (`TestOversizedKeyIsRefusedWithoutAPartialWrite`)

Beyond the list: every incremental Flush equals a bulk build (`TestIncrementalFlushEqualsBulkBuild`), a flush reads only around its edits and a no-op flush writes nothing, and every store error surfaces at every point (`TestStoreErrorsSurfaceAtEveryPoint`).

### L2 version graph (`core/vcs`)
- [x] A new repo has one branch `main` pointing at an empty-root initial commit (`TestANewRepoHasMainAtAnEmptyInitialCommit`)
- [x] Committing a working set produces a commit whose parent is the old head (`TestCommittingParentsTheOldHead`)
- [x] Two sessions commit to the same branch concurrently: both commits land, in some order, and neither is lost (`TestConcurrentCommitsBothLand`)
- [x] `UpdateWorkingSet` with a stale `prev` returns `ErrConflict` (`TestAStaleWorkingSetUpdateIsAConflict`)
- [x] `MergeBase` is correct on linear history, a simple fork, and a criss-cross merge (`TestMergeBase`)
- [x] Invalid branch names (`../x`, `a..b`, 129 chars, empty, `x.lock`) are rejected (`TestInvalidBranchNamesAreRejected`)
- [x] Deleting the checked-out branch of an active session returns `ErrBranchInUse` (`TestDeletingACheckedOutBranchIsRefused`)
- [x] Golden test: a fixed sequence of commits yields a fixed head hash (`TestAFixedHistoryHasAFixedHead`, which pins the store root too)
- [x] (rules) Every ref update is one `CompareAndSetRoot`, retried on a lost swap and bounded (`TestAWriterThatKeepsLosingGivesUp`); the author is the principal (`TestTheAuthorIsThePrincipal`); `Log` is capped (`TestLogIsBoundedAndHighestFirst`)

Beyond the list: every store error at every point surfaces and leaves the refs as they were (`TestStoreErrorsSurfaceAndLeaveTheRefs`, about two hundred failure points), forged refs and chunks are `ErrCorrupt` (`TestForgedRefsAreCorrupt`, `TestForgedChunksAreCorrupt`, three fuzz targets), refs name only stored objects (`TestRefsNameOnlyStoredObjects`), and a lost `Init` race is `ErrExists` (`TestAnInitThatLosesTheRaceIsErrExists`).

### L3 diff and merge (`core/merge`, with the merge state in `core/vcs`)
- [x] One table-driven test per row in the rule table (`TestEveryRuleOfTheTable`, with the rows DESIGN §8 adds)
- [x] Fast-forward: when base == ours, result root equals theirs' root with zero work (`TestFastForwardDoesNoWork`)
- [x] Property: merge(base, ours, ours) == ours for any edits (`TestMergingTheSameEditsIsIdentity`)
- [x] Property: merges with no overlapping keys are symmetric (`TestDisjointMergesAreSymmetric`)
- [x] Two branches edit different parts of one object: the model combines them with no conflict (the spec's "columns of a row" is the table model's, in the consuming repository; here `TestChangesToDifferentEntriesCombine` for trees and the line-set model in `TestConflictsBlockTheCommitUntilResolved`)
- [x] Two branches edit the same part: exactly one conflict, with the right base, ours and theirs (`TestEveryRuleOfTheTable`, `TestBothSidesEditingIsOneConflict`, `TestConflictsPerEntry`)
- [x] A `CellMerger` error mid-merge leaves the working set hash unchanged (`TestAFailedMergeLeavesTheWorkingSetUnchanged`, which also passes the conflict limit)
- [x] Past the conflict limit the merge returns `ErrTooManyConflicts` and changes nothing (`TestTooManyConflictsAbort` at a limit of 10, the default being 100,000; `TestAFailedMergeLeavesTheWorkingSetUnchanged`)
- [x] Merging 1M-row tables with 10 changed rows reads fewer than 1,000 nodes (`TestSlowMergingA1MNamespaceReadsLittle`, a million paths under `-tags slow`; 100,000 in `TestMergingLargeNamespacesReadsLittle`)
- [x] (rules) Conflicts are stored in the working set; a commit is refused while any remain (`TestConflictsBlockTheCommitUntilResolved`), and nothing but merging, resolving and committing changes a merge in progress (`TestUpdateWorkingSetKeepsTheMergeState`)

Beyond the list: merging what a branch already holds changes nothing (`TestMergingWhatIsAlreadyMergedChangesNothing`), and merges out of turn are refused (`TestMergesOutOfTurnAreRefused`).

## Decisions made while building (see also `docs/DESIGN.md` §1–2, §6)

- **local put-if-absent is temp + fsync + `link(2)`**, not `O_CREAT|O_EXCL` on the final name: equally atomic, and never exposes or strands a torn object.
- **local object files are `<segment>~`** so a name and a longer name under it (`a/b`, `a/b/c`) coexist, as the contract requires.
- **S3 object keys end in `!`**: MinIO hides a key that is also a path prefix of another (`a/b` beside `a/b/c`) from listings.
- **multivol volumes are local stores**, so each inherits the allowlist, marker, permissions and crash safety; the volume map and root live on the primary.
- **The chunk layer publishes by manifest swap**: packs and one index object per commit are durable before the sealed manifest names the new root; a swap that loses only to a manifest change is retried (at most 10), a moved root is the caller's conflict at once.
- **A closed chunk store refuses every call** (`ErrClosed`), so a caller holding one past `Close` fails loudly instead of reaching a released backend.
- **Backfills carry a `Red-Check: mutants` trailer**: a test for behavior that already exists proves itself with a mutant it kills.
- **redcheck judges a contract change on the tests that run the contract**; a caller that skips for want of an environment (the MinIO tier) is not judged, but a change every caller skipped is refused. With no base named it checks from `main`, else the root commit. A backfill's mutants are built with the tags of the tests that prove them. A fuzz target counts as a test; in a red commit one that passes against the stub stands beside the failing tests, but alone it shows nothing failing.
- **Coverage is measured over the merged `-coverpkg` profile**, so `core/dnx` counts its compat suite.
- **The 1 GiB CDC property runs under `-tags slow`**; the normal suite checks it at 16 MiB.
- **Not mutation-provable here:** removing an `fsync`. kill -9 keeps the page cache, so only a power cut would show it.
- **The split rule is exact integer arithmetic** (λ⁴ and a 128-bit divide), with a fresh digest window per level and two entries before an internal node may end; its golden boundaries came from an independent reimplementation.
- **Every format test carries its own codec**, written from the DESIGN text, so the writer is checked against the documented format and the reader against hand-built input; goldens pin what only the implementation could produce.
- **A mutant that hangs counts as killed** at the mutation engine's per-run timeout (three minutes).
- **Property tests prove their reach**: a property over trees fails unless enough of its cases built deep ones.
- **The merge state is the version graph's**: `UpdateWorkingSet` changes a branch's namespaces and nothing else, checked against the stored working set; only merging, resolving, committing and abandoning change it.
- **Abandoning a merge puts back where it started**: `Merge` merges into the working namespace as it is, so the merge state records the working and staged namespaces it started from, and `AbortMerge` restores them; edits made before the merge survive it. The two roots joined the working set's merge layout in place, before any release.
- **Merging what a branch already holds is a no-op**, and merging a descendant is not fast-forwarded (it makes a two-parent commit).
- **`Init` is resumable**: it claims the store by writing the config, and the next `Init` with the same key finishes one that stopped after that; `Open` reports such a store as `ErrNoRepo`.
- **`vcs.Namespace` asks no authorizer**: it opens what a hash names, and a host holding the hash holds the chunk store.
- **Store errors are swept, not sampled**: every package with a store under it has a test that fails exactly one store call at every point an operation makes one (chunk layer, keyed data, objects, trees, the version graph and, at the blob level, `Init` and `Open`).
- **GC's reachability comes from the models**: `model.Walker` is optional, beside the frozen port, and the model contract requires it (what a model names must hold its object); GC refuses to collect a repository holding an object it cannot walk.
- **A walk says which chunks are leaves**, and the marker prunes only nodes it has gone into, so bytes that are a file in one place and a node in another cannot steer it.
- **GC condemns, waits, re-marks, deletes**; writers never deduplicate against a condemned pack; a store rebuilds its index when gcGen moves.
- **GC records the orphans it deletes**, in the swap before the deletion, for a grace window and an hour (condemned kinds 3 and 4); a writer's publish is refused when the manifest records one of its unpublished uploads, or when one over an hour old is gone.
- **The root apart from the objects** (#8): objects on any S3-compatible provider (`s3.Options.ObjectsOnly`: HEAD then an unconditional PUT, no root), the root on a compare-and-swap store, a copy of the root on the objects store in one of four modes (wait, the default; background; periodic; off), recovered from when the root's disk is lost. The spike, on MinIO with 50 ms per S3 request: reads 56 ms → 2.7 ms, small commits 810 → 540 ms, opens 812 → 697 ms, never more S3 requests than all-S3. `Init` claims a store with the root's first swap, each `Init`'s config under its own repository id.
- **A lost session, not a conflict**: a store that finds writes it promised gone (a chunk a put counted on in a pack GC expired, an upload GC deleted as an orphan, found at publish or on a read) fails with `chunk.ErrSessionLost` and refuses every later write; the host reopens the repository. Retrying on the same store could keep failing, and fencing only counted-on chunks would miss chunks written into a deleted pack.
- **Expiry by GC's clock, orphan ages by the backend's**, read from a probe object.
- **The repository runs on `blob.NoDelete`**; `repo.GC`, with admin and the raw store, is the one path that deletes.
- **Writes are authorized per path, reads per branch.**
- **Equivalent mutants, not catalogued:** the cache's `Dir` check (`MkdirAll("")` fails anyway) and its `MkdirAll` error path (the `Chmod` after it fails); the pack index's per-entry read check (the reader's error is sticky and `Done` reports it); `OpenFrame`'s length check (authentication fails anyway); the manifest's count limits (the read fails at the first missing entry); the repository config's 4 KiB bound (the read is cut there anyway and a cut config fails to authenticate); `gc.Run`'s options check (a missing store, key or registry fails deeper down all the same); the version graph's walk of a conflict's sides (`vcs-walk-follows-conflicts`, retired: since a merge records where it started, the base, theirs and starting namespaces hold every side too, so the visit is defense in depth). Their statements are covered; no test can tell the mutant from the original.

- **The slow tiers run weekly, not nightly** (2026-09-23). The specs say
  "fuzzed nightly"; the fuzz job is 21 targets, hours at any useful budget
  (30 minutes each exceeded its own 6-hour timeout), and the code lands in
  batches, so `weekly.yml` runs fuzz (10 minutes a target), the crash
  harness, the mutant catalog, the slow tier and the real-provider S3
  suites every Sunday and on demand.

- **`core/cdc` cuts disknexus's boundaries itself** (2026-09-23, #10).
  The spec marks the chunker "reuse" through `core/dnx`; disknexus reads a
  byte at a time and a write could not go past about 200 MB/s whatever
  ran in parallel. `core/cdc` rolls the same Buzhash with the same rules
  over each read buffer; disknexus stays the oracle in `core/dnx/compat`
  (the goldens, and a differential test over every stream shape and reader
  behaviour), as the spec's rule 3 allows: a package of our own beside it.

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
| mutant `boundary-hard-maximum` survived | The maximum was enforced twice (the threshold already clamps at Max) | The splitter's copy removed |
| three stream mutants survived | A Ref shorter than its tree, a zero-length entry, and data read one level too deep as an index node would all read back with no error | `TestForgeriesThatWouldReadCleanly` |
| mutant `stream-data-length-checked` hung | Without the check, a short chunk makes ReadAt copy zero bytes forever | Per-mutant timeout in the mutation engine |
| rapid's 100 cases in 9 ms | The properties drew almost only single-leaf trees | Seeded large sets, bulk grow and collapse ops, and a height check |
| mutant `prolly-flush-resyncs` survived | Resyncing changes only cost, which nothing measured | `TestFlushReadsOnlyAroundItsEdits` |
| a red test | A flush that changed nothing re-stored the nodes it re-chunked | Identical nodes are named, not stored |
| the coverage gate | Every operation's store-error paths were dark | `TestStoreErrorsSurfaceAtEveryPoint`, with a store failing exactly one read or write |
| the coverage gate, C3 | Store-error paths and refusals dark in `vcs`, `repo`, `object` and `tree` | Fault sweeps and refusal tests, each with the mutants it kills |
| a red test | `UpdateWorkingSet` let a host drop or empty a merge's conflicts, or forge its second parent | `ErrMergeState`, checked against the stored working set |
| a red test | Merging a branch's own head made a commit with one parent twice, and the branch unreadable | Merging what a branch holds is a no-op |
| a red test | An `Init` that stopped after the config left a store no call would take | The next `Init` finishes it; `Open` says `ErrNoRepo` |
| mutant `vcs-resolve-authorized` survived | `DenyAll` cannot tell which check stopped a call | A read-only authorizer pass |
| mutant `vcs-conflicts-surface-iteration-errors` survived | A one-node conflict list is never read past its root | A conflict list spanning nodes |
| mutant `repo-init-finishes-on-the-configs-geometry` survived at first | Nothing `Init` writes depends on the node geometry | Five hundred objects written through the finished repository, compared by root |
| a design review under test | A marker pruning on "seen" alone can be steered by a file whose bytes are a node | The walk's leaf flag; `TestGCIsNotSteeredByAFileThatIsANode` |
| the vcs walk's forgery table | A head naming the working set's own chunk walked clean: the chunk was gone into as a working set first | The version graph's walk reads its chunks once each and refuses two roles |
| a red test | GC read orphan ages against its own clock: a backend clock running behind would delete a fresh upload | Ages by the backend's clock, from a probe |
| a red test | A writer that counted on a pack GC expired published a root onto a chunk that no longer existed | The fence: `chunk.ErrStale` |
| mutants `vcs-walk-follows-the-merge` and `-conflicts` would have survived | The fixture's theirs and ours were reachable some other way | Theirs' branch deleted, ours rewritten during the merge |
| e2e mutants survived | At the default pack size a file's data shares packs with live nodes | The GC e2e test runs with 64 KiB packs |
| the GC property's counters | The fence never fired in sequential histories | An edit that spans two collections |
| a flaky test | A fresh object looked up by a prefix a real pack name shares one run in sixty | Looked up by name |
| the full gates, closing C4 | A commit changing a signature mechanically across tests was blocked (each changed test judged); a stored-namespace check the path checks made dead; a branch check the path checks masked | The commit split into a refactor and a red; the check removed; a test granting paths and not the branch |
| the PR's first CI run | Docker Hub no longer serves `minio/minio`: the MinIO tier had passed locally on a copy cached a year before, and the failure said only "exit status 125" | The tier runs MinIO's release from quay.io, pinned by digest; a failed command's error carries what it printed |
| the PR's second CI run | On a cold runner `docker run -d` writes its pull progress on stderr and the container id on stdout; `tools/ci` merged the two, so `docker port` was handed an id that began with the progress | `output()` returns stdout alone; stderr only says why a command failed |
| the design of #3 | A writer's session index objects are as exposed as its packs, and worse: a manifest naming a deleted index object opens nowhere | Index objects are recorded and checked like packs |
| the design of #3 | Answering a lost write with a conflict to retry on the same store could fail forever (a lost chunk the retry no longer writes), and a fence on counted-on chunks misses chunks written into a deleted pack | A lost session: `ErrSessionLost`, writes refused, the host reopens (the fence on expired packs too) |
| the GC property, run 150 times | A host reading back what it had just written found a chunk its put had deduplicated against a pack GC expired meanwhile: a plain `ErrNotFound`, before any publish could be fenced (one run in about 120) | A read of a promised chunk that is gone ends the session (`TestReadingAPromisedChunkThatIsGoneLosesTheSession`) |
| the GC property's reach | The spanning edit lost a session in only about 94% of runs, so a check that one did was flaky | A slow-writer step, whose pack GC deletes as an orphan before it publishes |
| the design of #8 | `Init` claimed a store by writing one config put-if-absent; on a best-effort store two `Init`s overwrote each other's config and the first key lost its repository (`TestInitsRacingDoNotOverwriteEachOther` made the race certain) | Each `Init` writes `config/<repo id>` and claims the store with the root's first swap |
| the split store's tests | Two fixtures held only when the scheduler cooperated (a writer catching the first of three swaps; a copy landing before Close), and a string order check read "three" before "two" | The writer is held inside its first copy before the next swaps; numbered roots |
| the GC property's convergence bound (#1) | Repacking left the store over twice its live bytes for one grace window more than it should: a condemned pack found live again and mostly dead was reprieved in one round and repacked only in the next; and a pack whose every chunk is live looked mostly dead by its overhead, so tiny root packs were rewritten for nothing | A reprieved pack is repacked in the same round; a pack with no dead frame is never a candidate |
| the ordering mutants (#1, #6) | Two mutants on "packs in service first" were killed by a test whose pack order was a coin flip: index objects sort packs by hash, so the repacked pack came first one run in two | `TestPacksInServiceComeFirstInTheIndex` draws the fixture until the repacked pack sorts first, and both mutants anchor on it |
| the memory measurement (#6) | A session's packs went into one index object whatever their number, so a session of about 1.5 million chunks could not publish (`dedup.MaxObjectSize`), and every store decoded that object whole | Index objects are written in batches of at most 8 MiB estimated (`TestALargeSessionPublishesSeveralIndexObjects`) |
| the memory measurement (#6) | With the index and the mark on disk a collection still peaked at 185 bytes a chunk: GC's reader kept a 64 MiB chunk cache the walk never read twice from, and a candidate pack was read whole to repack it | The reader runs without a cache; a candidate's frames stream from one GET |
| the measurement at two million (#6) | The peak grew 48 bytes a chunk from one million to two: a refresh decoded every index object into memory before deciding to spill, and, larger, the in-memory blob backend held what the collection stored (it repacked one small mixed pack and rewrote every index object), which a heap measure cannot tell from the collector's own memory | A refresh keeps at most the bound's worth of decoded objects; the measurement's repository is on disk |
| the write path's profile (#10) | With hashing and compression on sixteen workers a write went 1.3× faster, not 3×: three quarters of the wall time was disknexus's chunker, serial by nature | `core/cdc` cuts the same boundaries itself at 800 MB/s (`TestSlowTheChunkerOutrunsDisknexus`) |
| the write path's profile, again (#10) | With the chunker fast the storer bound the write: appending frames to the pending pack grew and copied its buffer over and over, a third of the storer's time | The pending pack's buffer is allocated at the pack's size once |
| the chunker's differential test (#10) | Two mutants survived: the byte that makes a chunk exactly Min long is judged by the easy mask, which random streams reach one chunk in thousands, and bytes returned together with a read error, which no `iotest` reader does | A stream drawn from the corpus so a chunk is exactly Min by the easy mask alone; a reader that hands 64 KiB over with its error |
| the write path's profile, a third time (#10) | With the chunker fast the storer did the per-pack work on the per-chunk path: naming (SHA-256 of 32 MiB), building and uploading each full pack, half its time | Full packs are finished and uploaded beside the writer, two at a time |
| the throughput measurement (#10) | The compressible figure stopped moving at 330 MB/s: the test's text generator wrote a byte at a time through a modulo and had become the source being measured | It copies from a page of the pattern |
| the finisher's tests (#10) | The publish-waits test judged the order of what landed before the held pack had landed at all, so a publish that did not wait passed; the finishing-pack read test read a pack already named and held at its upload, where the ordinary path serves it | Every pack is waited for before the order is judged; a hold seam stops a finisher before it names its pack |
| the write path's profile, a fourth time (#10) | With packs finished beside the writer, the cutter bounded the write at about 800 MB/s: a Buzhash rolls a byte at a time, and the serial dependency looked like the end of it | The hash at a byte depends on the 48 bytes before it alone, so the masks' hits are marked in parallel and only the cut placement is serial: 2.4 GB/s |
| the parallel chunker's tests (#10) | Two mutants survived at first: a bit set past the search's end within its last word, which random data never places, and a reader blocked handing over a block, which the differential test's reader never was | A unit test of the bit search's bounds; a reader that fills every slot ahead and then blocks, closed with nobody reading |
| the race detector, on the finisher (#10) | The GC property test's clock offsets were plain fields, written by the test and now read by a finisher goroutine uploading a pack | Atomic offsets, set and read through methods |
| the write path's profile, a fifth time (#10) | With the storer trivial the wall did not move: a 256 MiB write allocated 3.4 GB, and both measurement tests were bounded by their own sources (a byte-at-a-time text generator, then a ChaCha8 stream at 700 MB/s) | One copy per chunk, recycled blocks, a compression scratch, packs built in place; the tests generate their data once beforehand |
| the commit's trace (#10) | A commit was two publishes, each a pack, an index object and a root swap written one after the other, six fsyncs on this disk at about 11 ms each whatever the pack's size, plus the file's last pack still pending | One publish per commit; the packs and the index objects written together; the last pack flushed when the stream ends |
| the GC property, under the finisher (#10) | Its slow-writer step jumped the clock the moment its puts returned, before the pack they filled had been uploaded beside them, so GC never met the orphan and no history lost a session; and its end asked GC to be done three windows after the last write, while a repack's chain of expiries (the pack it empties, then the index objects rewritten over it) is longer, and starts at whichever window the repack falls in | The step waits for the pack to land; the end runs quiet windows until a collection condemns, reprieves, repacks and deletes nothing, at most eight, then checks the next is quiet too; the failure names each deleted object's writer and age, and each collection's report |
| the GC property, waiting for convergence (#10) | Histories in which GC repacked a pack every window for ever: a repack copied every live chunk of the pack it emptied, while the join credits a chunk two packs hold to the first listed, so a fresh pack holding a chunk credited elsewhere counted as mostly dead next window and was repacked again, each time into a pack with the same flaw. #1's repacking had this from the start; three quiet windows hid it | A repack copies the chunks credited to the pack it empties alone (`TestARepackLeavesAChunkCreditedToAnotherPack`); a shared chunk is proved copied once by the new packs' entries |
| the memory measure (#10) | A store's open cost came out at 18 EB: a sync.Pool's buffers survive one collection in its victim cache, so the heap after opening was under the heap before it, and the difference was unsigned | Two collections per sample, signed differences |
| the coverage gate (#10) | `core/stream` fell to 79.5% when its pipeline tests moved to `e2e` (depguard forbids the stream package a pack store): the gate counts the `core/` and `model/` test binaries, so an `e2e` test proves nothing to it | The pipeline's tests run in the package against a preparer over the memstore, with the store's own tests in `e2e` beside them; the pack store's flush, index bound, stats over a finishing pack, unpublished packs across a spilled refresh and a miss that reads another store's publish each have a test and a mutant in the package |
| the memory figures after #10 (#14) | The collection peak read 54 MiB where #6 had measured 31: not the 8 MiB index-object batches (1 MiB batches measure the same) and not the sampler (collecting twice changes nothing); a heap profile at the peak is 32 MiB of `pack.NewWriter`, the repack writer's buffer presized at the pack size since #10 for a repack of a few KiB of working-set nodes, and 20 MiB of zstd encoder that was always there | The figures restated with the reason; whether GC sizes the writer to what it copies is #14's decision |
| (redcheck on this branch) | Build-tagged tests unjudged; `TestMain` judged; pairs not matched by scope; contract changes invisible; environment-bound callers refused; no `main` in a new clone; a tagged backfill's mutants built without its tag; fuzz targets not counted as tests | Tool fixed each time, with a red test |

### Batch 2 (in progress)

- [x] #8 The root apart from the objects: `TestObjectsOnlyRunsWhereConditionalWritesAreIgnored`, `TestObjectsOnlyPutIsAHeadThenAPut`, `TestTheMirrorIsReplacedInPlace`; the split store's contract and modes (`TestContractOverASplitStore`, `TestWaitMirrorsEachSwapBeforeReturning`, `TestBackgroundMirrorsTheNewestRootAndCloseFlushes`, `TestBackgroundCopiesEachSwap`, `TestBackgroundReportsAndRetriesAFailedCopy`, `TestPeriodicMirrorsOnItsClock`, `TestOffKeepsNoCopy`, `TestRecoverSeedsAnEmptyRootStoreFromTheCopy`); `TestTheRootsCopyPassesThroughTheCache`; two `Init`s racing (`TestAnotherInitCannotTakeOverARepository`, `TestInitsRacingDoNotOverwriteEachOther`, `TestAStoppedInitFinishedTwiceAtOnceIsFinishedOnce`, `TestOpenTakesTheConfigThatAuthenticatesTheRoot`); end to end on an endpoint ignoring conditional writes, recovered from the copy, `TestARepositoryRunsOnAnEndpointWithoutConditionalWrites`.
- [x] #1 Reclaiming space in mixed packs: a round repacks kept packs that are mostly dead, emptiest first within a byte budget, records them under a kind of their own and expires them a grace window on (`TestARoundRepacksAPackThatIsMostlyDead`, `TestAPackAboveTheThresholdIsKeptWhole`, `TestRepackingSpendsItsBudgetOnTheEmptiestPacksFirst`, `TestAfterARepackWritersFindTheNewPacks`, `TestRepackingCopiesAChunkTwoPacksShareOnce`, `TestRepackingRefusesACorruptPack`); the GC property converges to at most twice the live bytes (`TestGCSafetyProperty`).
- [x] #6 The index in memory: a `dedup` table on disk (`TestATableAnswersEveryRecordItWasBuiltFrom`, `TestTheFirstRecordOfAKeyWins`, `TestATableHoldsAlmostNothingInMemory`, `TestATableThatDoesNotDecodeIsRefused`, `FuzzOpenTable`); a store spills its index past a bound (`TestAStoreSpillsItsIndexToDisk`, `TestASpilledIndexIsRebuiltWhenGCMovesIt`, `TestASpilledIndexNeedsItsDirectory`, `TestContractOverASpilledIndex`, `TestPacksInServiceComeFirstInTheIndex`); GC marks and indexes on disk (`TestGCNeedsItsWorkDirectory`); with 64 Ki in memory, a million chunks open in 21 KiB and collect in a 54 MiB peak, two million in 25 KiB and the same 54 MiB (`TestSlowMemoryPerChunkOn1MChunks`; 31 and 34 MiB before #10 presized the repack writer's buffer at the pack size, #14).
- [x] #4 The real provider, weekly: `TestContractAgainstRealS3` and `TestARepositoryRunsOnTheRealProvider` ran green against the real provider in the weekly run of 2026-09-23 (the first run had found the endpoint given as a bare host: accepted as HTTPS). The MinIO tier runs the e2e package on every push.

## Next

The storage core's milestones, C0 to C4, are done. What remains is outside
them, each tracked as an issue:

1. ✅ **Reclaiming space in mixed packs** (#1): a GC round repacks packs
   that are mostly dead, within a per-run byte budget
   (`TestARoundRepacksAPackThatIsMostlyDead`,
   `TestRepackingSpendsItsBudgetOnTheEmptiestPacksFirst`).
2. ✅ **Reading and deleting tags** (#2): `Tags`, `Tag` and `DeleteTag`
   (`TestTagsAreListedAndReadBack`, `TestAMissingOrDeletedTagIsNotFound`,
   `TestForgedTagRefsAreCorrupt`; what only a deleted tag reached is
   collected, `TestADeletedTagsHistoryIsCollected`).
3. ✅ **A slow writer's unpublished packs** (#3): GC records the orphans it
   deletes, a writer checks its uploads at publish, and a lost write ends
   the session (`TestARoundRecordsTheOrphansItDeletes`,
   `TestAPublishFailsOnAPackRecordedAsADeletedOrphan`,
   `TestAPublishFailsOnAnOldUploadThatIsGone`, the index-object pair,
   `TestReadingAPromisedChunkThatIsGoneLosesTheSession`; end to end,
   `TestAWriterCannotPublishAPackGCDeletedAsAnOrphan`; slow writers in
   `TestGCSafetyProperty`).
4. **Real S3 in the weekly run** (#4): the job is written
   (`real-provider`: `TestContractAgainstRealS3` objects only and
   `TestARepositoryRunsOnTheRealProvider`, on Backblaze B2 or iDrive e2 with
   the root on the runner's disk); it fails until an admin adds the
   `SNAPSHOT_S3_*` secrets, then the item closes on its first green night.
5. ✅ **Abandoning a merge** (#5): `AbortMerge` puts back the working and
   staged namespaces the merge started from
   (`TestAnAbandonedMergeLeavesTheBranchAsItWas`,
   `TestOnlyAMergeInProgressCanBeAbandoned`; per path,
   `TestWritesAreAuthorizedPerPath`; GC keeps the starting namespaces,
   `TestAWalkNamesAllTheRepositoryHolds`, and collects an abandoned merge,
   `TestAnAbandonedMergeIsCollected`).
6. ✅ **The index in memory** (#6): the index past a bound, the mark and
   the round's own index are tables on disk; a million chunks with 64 Ki
   in memory open in 16 KiB and collect in a 31 MiB peak, against 117 MiB
   and 371 MiB before, and two million in 20 KiB and 34 MiB
   (`TestSlowMemoryPerChunkOn1MChunks`).
7. ✅ **The write path was single-threaded** (#10): chunks are hashed and
   compressed on `stream.Config.Workers` goroutines and stored in order
   (`TestSlowWorkersOutrunOneWorker`, `TestTheStreamIsTheSameAtAnyWorkerCount`),
   and `core/cdc` cuts disknexus's boundaries itself three times as fast
   (`TestSlowTheChunkerOutrunsDisknexus`). On the same laptop over
   `blob/local`: 321 MB/s writing random data (from 114), 509 compressible
   (from 188), 376 re-snapshotting (from 203); in memory 830 on sixteen
   workers against 300 on one; reads and commits unchanged
   (`TestSlowThroughputOnLocalDisk`, weekly). Full packs are finished and
   uploaded beside the writer, at most two at once
   (`TestAnUploadDoesNotHoldUpTheWriter`, `TestAtMostTwoPacksAreInFlight`,
   `TestAPublishWaitsForItsUploads`); `cdc.Parallel` marks the masks' hits
   on every core and places the cuts on one, 2.4 GB/s against 750 serial
   (`TestSlowParallelCuttingOutrunsTheChunker`,
   `TestTheParallelChunkerCutsWhereTheChunkerCuts`); chunks are sealed on
   the preparing goroutine (`TestAChunkPreparedForOnePackStoresInTheNext`)
   and the path copies each chunk once. On disk the fsync'ed write bounds
   it; more packs in flight measured the same at two, four and eight. A
   commit is one publish (`Repo.Commit`, `TestACommitIsOnePublish`) whose
   packs and index objects are written together
   (`TestAPublishWritesItsPackAndIndexObjectTogether`), the stream's last
   pack having uploaded when the stream ended if it held at least an eighth
   of a pack (`TestAStreamsLastPackIsUploadedWhenTheStreamEnds`): 81 / 39 / 33 ms
   for the three files, from 125–142 / 80 / 86; 37 ms when any work
   separates the write from the commit.
