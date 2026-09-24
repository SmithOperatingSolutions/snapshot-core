# snapshot-core design

This is how the Storage Core Spec (`docs/specs/storage-core-spec.md`) becomes
code: the decisions made while reviewing it against disknexus-engine, the
layering the build enforces, the on-disk formats, and the concurrency and GC
protocols. Where a rule came from the Engine Spec's L0–L3 (which moved into the
Storage Core), it says so. Where we deviate from either spec, it says why.

## 1. Decisions from the spec review (Sep 23, 2026)

Reviewed against disknexus-engine **v0.2.11**, which is commit `b2c02f1`, the
exact commit the spec's package map was reviewed at. That is the pin.

| # | Question | Decision | Evidence |
| --- | --- | --- | --- |
| D1 | Module name (spec open question) | `github.com/SmithOperatingSolutions/snapshot-core`, one Go module | Repo name chosen by the owner. The Engine Spec's one-module-per-layer is replaced by one module with `depguard` layer rules: same boundary, no `replace`/`go.work` plumbing |
| D2 | disknexus release cadence (open question) | Pin an exact tag; upgrade only deliberately (security fix or needed feature), in its own PR, behind `core/dnx/compat` | disknexus is pre-1.0 and says minor versions may change signatures |
| D3 | Spike: can `core/store` pack framing serve hash-addressed packs? | **No. `core/pack` is new.** | `store.ChunkStore` writes `dir/chunks/NNNN.pack` with sequential `uint32` numbers, creates dirs `0755` and files `0644`, encrypts every chunk with the *master* key under disknexus's own AAD (`disknexus/chunk/v1`), and has no in-pack index or chunk hash in its frames. The framing is an 8-byte length header — nothing worth wrapping. We use `klauspost/compress/zstd` (the library disknexus uses) directly |
| D4 | `core/dedup`: wrap `core/index`? | **No. `core/dedup` is new** (an in-memory index built from our encrypted index objects) | `index.NewDedupIndex` decrypts `hash-index.db.enc`/`bloom.bin.enc` into plaintext working files and builds a plaintext `hash-index.htab` on disk. The spec's own rule ("a test asserts no plaintext hash appears on disk") cannot hold without editing disknexus. It also keys packs by `uint32` and is bound to one local directory, so it cannot serve S3 multi-writer |
| D5 | `core/seal` and per-pack keys | `seal` owns the raw master key and derives every object key with stdlib HKDF-SHA-256. disknexus supplies the AEAD (`MasterKeyFromBytes(k).EncryptWithAAD`), Argon2id (`DeriveKEK`, `Argon2Params.Validate`) and X25519 wrapping (`WrapSecretAsymmetric`) | disknexus's `MasterKey` never exposes its bytes, so HKDF from it is impossible; its `KeyFile`/`WrapKey` seal the key with **no** domain tag, which the spec forbids. So we do not use `KeyFile`, `GenerateKeyFile`, `OpenMasterKey`, `Encrypt`/`Decrypt` (untagged) |
| D6 | `core/hash` reuse | Routed through `core/dnx` as specified (`hasher.Sum(b).StrongHash`); the compat suite pins it equal to `crypto/sha256` | `hasher.Sum` is `sha256.Sum256` plus an xxHash filter hint. The reuse is nominal; the pin is what matters |
| D7 | `core/cdc` geometry | `cdc` validates geometry before handing it to disknexus (min ≥ 64 B, max ≤ 1 MiB chunk limit, min < max, mask non-zero and of the form 2ⁿ−1) | `chunker.New` accepts `max = 0` (a chunk per byte) and `min > max` silently |
| D8 | What "reuse" amounts to | hasher (identity), crypto (AEAD, Argon2id, X25519). The chunker was reused through `core/dnx` until #10; `core/cdc` now cuts disknexus's boundaries itself (D10). Everything else is ours | Matches the spec's "hardest-won quarter" |
| D10 | The chunker (2026-09-23, #10) | `core/cdc` rolls disknexus's Buzhash and cut rules over each read buffer, byte for byte disknexus's boundaries; disknexus stays the oracle, imported in `core/dnx` and held to by `core/dnx/compat`'s goldens and a differential test. `cdc.Parallel` marks where the masks hit on many goroutines and places the cuts on one, since the hash at a byte depends on the 48 bytes before it alone: the same boundaries at 2.4 GB/s on sixteen threads | disknexus reads a byte at a time and bounds a write at about 200 MB/s on one core; ours cuts the same 8,531 chunks of 256 MiB at 750 MB/s serially. The spec's rule 3 allows a package of our own beside it |
| D11 | The S3 tier's server (2026-09-24) | SeaweedFS (`chrislusf/seaweedfs`, pinned by digest in `tools/ci`), started in Docker by the `s3` step with its credentials from the environment. It honors If-None-Match and If-Match, so the probe and the root swap run as on S3; it lists a directory's children right after the directory rather than in byte order, which the contract is told (`contract.AnyTotalOrder`: one total order kept across pages, prefixes and `after`) and no caller in the core depends on (GC's orphan scan and the repo's config listing page with `after` and read sets; multivol merges only its own local volumes) | MinIO ended its public distribution (quay.io, Docker Hub and dl.min.io all gone on 2026-09-24); a fork of MinIO built from source remains the fallback |
| D9 | Where the Engine Spec lives | Its L4 (tables) is implemented in a separate, consuming repository. Its L0–L3 rules apply here as below | Owner decision |

## 2. How the Engine Spec's L0–L3 land here

The Storage Core Spec wins where the two disagree (Engine Spec, Sep 22 note).

| Engine Spec rule | Here |
| --- | --- |
| L0 `ChunkStore` port is the *backend* port | The backend port is the Storage Core's `BlobStore` (immutable named objects + one root CAS). The Engine's `ChunkStore` port (Get/Has/Put/Root/CompareAndSetRoot, SHA-256 re-verified on read, `ErrCorrupt`, 1 MiB `ErrTooLarge`, "a root may only point at chunks that exist", 100-way CAS race, crash harness) becomes the **chunk layer's** port, `core/chunk`, implemented by `core/chunk/packstore` over any `BlobStore` and by `core/chunk/memstore` for tests. Both ports have contract suites |
| filestore / multistore / s3store / memstore | `core/blob/local`, `core/blob/multivol`, `core/blob/s3`, `core/blob/mem`. The backend rules (statfs allowlist, volume map and markers, read-only on a missing volume, free-space floor, 64-volume limit, S3 probe, 10-attempt jittered CAS retry, request budget, local read cache) apply to them unchanged |
| "table files" and "manifest" | Packs (`packs/<sha256>`) and the root object. The root object *is* the manifest: it holds the refs root hash and the list of live index objects |
| Optional at-rest encryption | **Mandatory** (Storage Core wins). No plaintext mode exists |
| `refs/heads/<b>`, `refs/tags/<t>`, `workingSets/<b>` | `heads/<b>`, `tags/<t>`, `work/<b>` (Storage Core naming) |
| `RootValue` (table name → table root) | The namespace tree: path → `ObjectRef`. The table plugin (model id 3) stores its tables as objects in it |
| L3 `CellMerger` | `Model.Merge`. The generic driver applies the per-key rule table; a model decides only when both sides changed the same path |
| Unit tests with `testify/require` | Standard `testing` with failure messages written per `docs/TESTING.md` §1 (Storage Core adopts that standard). Property tests use `pgregory.net/rapid` as the Engine Spec says |
| Prolly split: `sha256(e.key)` as uint32 under a threshold rising with node byte size; 512 B / 4 KiB / 16 KiB | As written for level 0 (bytes 0–3 of `sha256(key)`, big-endian). Internal levels read the next 4-byte window of the same digest, because re-using bytes 0–3 at every level correlates splits across levels (a key that ended a leaf is biased to end its parent too). The exact threshold, computed in integers so every platform draws the same tree, lands with `core/boundary` in C2 (§7) |
| "Boundaries depend on keys only, so value edits never reshape the tree" | **Reconciled, not literal.** The threshold rises with the node's byte size, and byte size includes values; that is what keeps nodes inside 512 B–16 KiB for tables with wide rows. So a value edit that keeps the value's length never reshapes the tree (tested: exactly height+1 nodes rewritten); one that changes its length can move a boundary locally, and the tree resyncs at the next boundary (tested: bounded) |
| Key ≤ 4 KiB, value ≤ 256 KiB inline, larger values in a blob tree | As written. The inline limit is repo geometry (recorded at init). A node that reaches 16 KiB ends after the entry that crossed it, so a node is at most 16 KiB plus one inline value |

## 3. Layers (enforced by `depguard`)

Lower rows never import higher rows.

| Layer | Packages | Imports |
| --- | --- | --- |
| Adapter | `core/dnx` | disknexus-engine (the only importer), stdlib |
| Primitives | `core/hash`, `core/auth`, `core/internal/wire` | `core/dnx` (hash only), stdlib |
| Crypto, chunking | `core/seal`, `core/cdc`, `core/boundary` | primitives, `core/dnx` (seal only) |
| Backends | `core/blob`, `core/blob/{mem,local,multivol,s3,cache}` | stdlib, AWS SDK (s3 only) |
| Packs | `core/pack`, `core/dedup` | seal, hash, zstd |
| Chunk layer | `core/chunk` (port), `core/chunk/{memstore,packstore}` | blob, pack, dedup, seal |
| Keyed data | `core/stream`, `core/prolly` | chunk, cdc, boundary |
| Objects | `core/model` (port), `core/object` | prolly, stream |
| History | `core/vcs`, `core/merge` | object, model, auth |
| GC | `core/gc` | everything below; the only caller of `BlobStore.Delete` |
| Entry point | `core/repo` | everything below |
| Models | `model/blob`, `model/tree`, `model/contract` | `core/model`, `core/stream`, `core/prolly`, `core/object` |
| End to end (tests only) | `e2e` | everything, models included |

Limits live in the package they bound (`chunk.MaxChunkSize`,
`prolly.MaxKeySize`, `auth.MaxIDLen`, `vcs.MaxLog`, `vcs.MaxMessageLen`,
`merge.DefaultMaxConflicts`, the path limits in `core/object`); there is no
separate limits package. Nothing under `core/` may import a concrete model,
so tests that drive the core through real models live in `e2e`.

## 4. What a repository looks like in a BlobStore

| Name | Mutable | Sealed under | Holds |
| --- | --- | --- | --- |
| *(root)* | yes, CAS only | `vdb/refs/v1` | the manifest: refs root hash, live index objects, GC generation, condemned objects |
| `config/<repo id>` | no | `vdb/config/v1` | repo id, key id, CDC and prolly geometry, pack limits; one per `Init` that started, and the root's manifest names the one that counts |
| `packs/<sha256 of pack bytes>` | no | `vdb/chunk/v1` (frames), `vdb/pack-index/v1` (trailer) | many chunks, each compressed then sealed, plus an in-pack index |
| `index/<sha256 of object bytes>` | no | `vdb/index/v1` | chunk hash → pack and offset, for the packs of one or more commits |

Keys never live in the BlobStore ("keys never stored beside data"): the host
holds the master key through a KMS wrapper or a passphrase key file it stores
elsewhere.

### A root apart from the objects (`core/blob/split`, #8)

Some S3-compatible providers ignore `If-None-Match` and `If-Match`
(Backblaze B2 and Google Cloud Storage's S3 API among them), and `s3.Open`
refuses them: without those two headers no root swap is atomic. To run on
one, a repository keeps its objects there and its root elsewhere.

- **Objects only.** With `s3.Options.ObjectsOnly`, the S3 store serves
  objects and no root. `Put` asks with HEAD and then PUTs without a
  condition: one writer at a time still gets `ErrExists`, and two writers
  racing on one name may both land, which is harmless because every name a
  repository writes is unique (packs and index objects by their hash, the
  config by its repository id, probes at random). `Root` and `SwapRoot` are
  `ErrObjectsOnly`. `Open` probes reading, writing and deleting, not the
  conditions. The blob contract runs on such a store with
  `contract.Options{ObjectsOnly: true}`: no root cases, and racing puts need
  no single winner.
- **The split store.** `split.Store{Objects, Roots}` sends objects to the
  first and the root to the second, which must compare-and-swap:
  `blob/local` on one host today, a SQL row or etcd later through the same
  seam. The repository, the chunk layer and GC see one store; GC's clock
  probe lands on the objects store, whose clock stamps the orphans.
- **The mirror.** The root is the only record of the repository's state,
  and it now lives on one disk, so the split store keeps a copy of it on
  the objects store (`WriteMirror` and `ReadMirror`, the unconditional
  replacement of one object, `<prefix>mirror` on S3). The mode is the
  host's: `MirrorWait` (the zero value) returns from a swap once the copy
  has landed; `MirrorBackground` has one writer upload the newest root after
  each swap, skipping any it was superseded on; `MirrorPeriodic` uploads the
  newest root every `Every` if it changed; `MirrorOff` keeps none, and the
  host backs up the root store. `Close` flushes the copy. `MirrorStatus`
  says whether the newest root is mirrored, when the last copy landed and
  what last went wrong. In `MirrorWait` a copy that fails is retried within
  the call; if it still fails the swap is reported all the same, since it
  happened and cannot be undone, and the copy trails until a later attempt
  lands: a host that must not lose a commit reads the status. Within one
  process the copy never goes backwards; several processes on one host can
  leave it a swap behind until the next swap. `Recover(ctx, objects, roots)`
  seeds a root store that has no root from the copy. A root store restored
  some other way, from an older backup, would carry an older root than the
  copy and nothing detects it: recover from the copy instead.
- Only GC deletes, still: the copy lies outside `objects/`, no listing
  reaches it, and GC never names it.

## 5. On-disk formats (a compatibility contract)

All integers are little-endian; varints are minimal LEB128 (`core/internal/wire`,
which refuses anything else). Every format has a hand-written decoder and a fuzz
target; packs and index objects also have a checked-in v1 file that must read
forever (`core/pack/testdata/pack_v1.bin`, `core/dedup/testdata/index_v1.bin`).

**Key derivation (`core/seal`).** An object key is
`HKDF-SHA-256(ikm = master, salt = object salt, info = "snapshot-core/object-key/v1" 0 tag 0 repo-id)`;
associated data is `tag 0 context`. The key id is
`HKDF-SHA-256(master, no salt, "snapshot-core/key-id/v1")`. Pinned by
`TestObjectKeyDerivationAndAssociatedDataArePinned`.

| Structure | Layout |
| --- | --- |
| Key file (139 B) | `"SCKF"` · version u16 · Argon2 time u32 · memory u32 · threads u8 · salt [32] · key id [32] · AES-GCM(KEK, master) [60]; AAD `"vdb/keyfile/v1" 0 header[0:79]`. Costs are validated before any derivation: 2–10 passes, 19 MiB–1 GiB, 1–64 lanes (#13: a file inside a 4 GiB, 64-pass ceiling hung the process) |
| KMS envelope | `"SCKW"` · version u16 · key id [32] · wrapped (len-prefixed, ≤ 8 KiB) |
| Pack | header `"SCPK"` · version u16 · flags u16 · salt [32]; frames `seal(Chunk, ctx = chunk hash, zstd-or-raw)`; index `seal(PackIndex, ctx = header, "SCPI" · version · count · entries sorted by hash: hash [32] · offset · stored · raw · codec)`; trailer index-offset u64 · index-length u32 · `"SCPE"` |
| Index object | `"SCIX"` · version u16 · salt [32] · `seal(Index, ctx = header, "SCIP" · version · packs: pack hash [32] · salt [32] · size · entries …)`, packs and entries strictly sorted |
| Manifest (the root value) | `"SCMF"` · version u16 · salt [32] · `seal(Refs, ctx = header, "SCMP" · version · seq u64 · gcGen u64 · root [32] · count · index-object hashes [32] strictly sorted · count · condemned (kind u8: 1 pack, 2 index object, 3 orphan pack deleted, 4 orphan index object deleted, 5 pack repacked · hash [32] · at i64 unix ns))` |
| local store | `.snapshot-core` marker (`"SCLS"` · version · store id [16]) · `objects/<segment>~` · `tmp/` · `root` (`"SCRF"` · version · value · SHA-256) · `root.lock` |
| multivol map | `"SCMV"` · version u16 · count u16 · (volume id [16] · path) … · SHA-256 |
| S3 keys | `<prefix>objects/<name>!` (the `!` keeps any key from being both an object and a path prefix, which MinIO hides from listings; it sorts below every name byte) · `<prefix>root` = 16-byte nonce ‖ value · `<prefix>mirror` = the root's copy, replaced in place · `<prefix>probe/…` |
| Disk cache entry | `"SCCE"` · object name · SHA-256 of content · content |

Object names: packs are `packs/<first byte hex>/<SHA-256 of the pack bytes>`,
index objects `index/<SHA-256 of the object bytes>`; readers verify both.

Every sealed format has a v1 golden file written once and checked in, which
the current code must keep opening byte for byte: `core/pack/testdata/pack_v1.bin`,
`core/dedup/testdata/index_v1.bin`, `core/chunk/packstore/testdata/manifest_v1.bin`.
Each also has forgery tests (sealed under the right key, so only the decoder
can refuse them) and a fuzz target.

## 6. The chunk layer protocol (`core/chunk/packstore`)

- **Put** adds to an in-memory pack writer. A full pack is handed to a
  finisher goroutine that names it, builds it and uploads it (#10); the
  writer moves to a fresh pack at once. Until the pack is named its chunks
  read and deduplicate from the unfinished writer, then from its bytes kept
  in memory until the upload is confirmed. At most two packs are finishing
  or uploading at once: a writer with a third full pack waits, so a slow
  backend costs time, never memory. A failed upload is kept and retried by
  the next CompareAndSetRoot, which waits for every finisher, finishes and
  uploads the pending pack itself, and refuses to publish while any pack is
  unstored. The packs a publish has to upload and the index objects that
  list them are written together, each waiting on the backend and neither
  on the other, then the root is swapped: one fsync latency for the two on
  `blob/local` (a pack that fails leaves its index objects orphans, which
  GC deletes). `Flush` (`chunk.Flusher`) hands the pending pack to a
  finisher at once when it holds at least an eighth of a pack; `stream.Write`
  flushes when a stream ends, so the pack holding a big file's last chunks
  uploads beside the host's next work rather than inside the publish. A
  smaller pending pack waits for the publish and shares its pack with what
  comes next: a pack put costs one fsync latency (one round trip on S3)
  whatever its size, so flushing it would save nothing and cost a pack, a
  fsync and an index entry per file. A commit that follows a gibibyte's write by any
  other work is one metadata pack, one index object and a swap: 37 ms here.
  Put is two halves (`chunk.Preparer`, #10): **Prepare**, the hash and the
  compression, on the caller's goroutine with no lock; **PutPrepared**, the
  deduplication check and the seal into the pending pack, under the lock.
  `stream.Write` uses the halves to hash and compress on `Config.Workers`
  goroutines (GOMAXPROCS by default; 1 is the serial path) while
  `cdc.Parallel` reads and marks the stream on its own goroutines and the
  caller's places the cuts and stores in stream order, so the stream is
  the same at any worker count; at most about 2×Workers chunks and a few
  blocks are in flight. The pending pack's buffer is allocated at the
  pack's size once. Measured on an i7-1360P (`TestSlowThroughputOnLocalDisk`,
  the weekly run): a gibibyte of random data writes to `blob/local` at 262
  MB/s where it wrote at 114, compressible text at 485 where it wrote at
  188, a one-byte re-snapshot deduplicates at 322; in memory on sixteen
  workers 830 MB/s where one worker does 300. Chunks are sealed on the
  goroutine that prepares them, under the pending pack's keys, and sealed
  again under the lock only if the pack rolled over meanwhile; the placer
  copies a chunk out of its block once and recycles the block, compression
  writes into a scratch buffer and keeps only a shorter result, and a pack
  is built in its writer's own buffer. Reads 0.5–1.2 GB/s and commits about
  100 ms are unchanged. On disk the backend's fsync'ed write bounds a
  write, which more packs in flight do not raise (measured the same at two,
  four and eight).
- **CompareAndSetRoot(expected, next)** refuses a `next` that is not a stored
  chunk, uploads every pending pack, writes one index object for the session's
  packs, then swaps the manifest (root := next, index list += the session's
  objects, seq + 1). Every chunk a published root reaches is therefore durable
  before the manifest names it. If the root moved, `ErrRootConflict` returns at
  once (the caller re-reads and re-applies); if only the manifest changed
  (another writer's index objects, GC), it refreshes and retries with jittered
  backoff, at most 10 attempts (Engine Spec).
- **Get** serves from the pending pack, then a byte-bounded LRU, then the
  backend by one range read (the index object carries each pack's salt, so no
  header fetch). A hash the index does not know triggers one manifest refresh.
  Every path re-hashes what it returns.
- **Memory does not grow with the repository** (#6). A store indexes the
  published chunks in memory up to `Options.IndexInMemory` (512 Ki chunks,
  about 60 MiB, by default) and past that spills the whole published index
  to a *table* in `Options.IndexDir` (the system's temporary directory by
  default): `dedup.Builder` sorts records a run at a time, spills each run
  and merges them into a file of fixed-width records in hash order with one
  sample key per block of 256; `dedup.Table` keeps one sample in 256 in
  memory (a key per 65,536 chunks) and answers a lookup with two range
  reads, the sample block then the record block. The packs in service are
  added before the condemned and repacked ones, so a chunk two packs hold
  resolves to the one staying; the session's own packs, and what other
  writers publish until the bound is passed again, stay in memory. GC
  moving gcGen rebuilds the table; Close removes it. Index objects are
  written in batches of at most 8 MiB (estimated), since an object is
  decoded whole; a refresh keeps at most the bound's worth of decoded
  objects before it switches to streaming them into the table. Measured on
  64-byte chunks with 64 Ki in memory, the repository on disk
  (`TestSlowMemoryPerChunkOn1MChunks`, the slow tier): a million chunks open
  in 21 KiB where they took 117 MiB, and collect in a peak of 30 MiB where
  they took 371; two million open in 25 KiB and collect in 34 MiB (§9). Of
  that peak, 20 MiB is the codec's zstd encoder; a repack's pack writer is
  sized to what is left to copy, not to a pack (#14: at the pack's size,
  as the store's pending pack is since #10, it was 32 MiB whatever the
  repack copied).
- The manifest lists every index object until GC compacts them (§9).

*(The prolly tree, the version graph and merge are §7–8; GC is §9.)*

## 7. Keyed data (`core/boundary`, `core/stream`, `core/prolly`)

Everything above the chunk layer is chunks that name other chunks by hash.
Two structures do it: prolly trees (ordered key-value maps, Engine Spec L1)
and streams (byte sequences cut by CDC). Both draw node boundaries from
content alone, so equal contents are equal chunks whatever history produced
them. These formats are a compatibility contract like those in §5.

**Chunk kinds.** A structured chunk's first byte says what it is:

| First byte | Chunk |
| --- | --- |
| `0x01` | prolly node, v1 |
| `0x02` | stream index node, v1 |

Raw stream data (the bytes of a file or a long value) has no header. It is
reached only through a stream index node or a stream ref, which say what it
is. C3 adds kinds for commits and refs.

### The split rule (`core/boundary`)

A node ends after an entry when the entry's window falls under a threshold
that rises with the node's size:

- **Size** is the encoded length of the node's entries so far, without the
  node header; `before` and `after` are that length without and with the entry.
- `after < Min` (512 B): never. `after ≥ Max` (16 KiB): always.
- Otherwise when `window < T`, with `T = ⌊(after⁴ − before⁴) · 2³² / λ⁴⌋`
  (2³² or more: always), computed in 128-bit integers (`math/bits`) so that
  every platform draws the same tree.
- `λ = round(Target / Γ(5/4))`: 4,519 for the 4 KiB target, `λ⁴ =
  417,031,985,092,321`. `(s/λ)⁴` is the cumulative hazard of a Weibull
  distribution with shape 4 and scale λ, whose mean is the target, and `T` is
  the entry's share of it: nodes cluster around 4 KiB, about 1 in 6,000 would
  end before 512 B, and reaching 16 KiB has probability around 10⁻⁷⁵. `T`
  reaches 2³² once one entry carries a whole unit of hazard (`4·s³·e ≳ λ⁴`
  for an `e`-byte entry at size `s`): past 16 KiB for 20-byte entries, near
  8 KiB for 200-byte ones. Such forced ends are rare (a node survives to
  8 KiB about once in 18,000) and still a function of content alone.
- **Window** is 4 bytes of a 32-byte digest, big-endian: bytes `[4L, 4L+4)` at
  level `L < 8`, and at `L ≥ 8` the same window of `SHA-256(digest ‖ byte(L/8))`.
  Prolly uses `sha256(key)` (the leaf key, or at internal levels the child's
  last key); streams use the child chunk's hash. A fresh window per level
  keeps a key that ended a leaf from being biased to end its parent as well.
- **At levels ≥ 1 a node takes at least two entries** before it may end (a
  level's last node excepted). One internal entry with a 4 KiB key already
  passes `Min`; without this rule a level could fail to shrink and a tree
  would have no height bound. With it, height ≤ log₂ N + 1.

### Prolly nodes (`core/prolly`)

```
node            0x01 · level u8 · count uvarint · entry × count
leaf entry      key · value                                    (level 0)
internal entry  key · child [32] · entries uvarint             (level ≥ 1)
key             length uvarint (0..4096) · bytes
value           0x00 · length uvarint (≤ inline limit) · bytes (inline)
              | 0x01 · stream ref                              (longer values)
stream ref      root [32] · size uvarint · depth u8
```

- Keys are strictly increasing within a node and across a level (raw byte
  order). An internal entry's key is its child's last key, and `entries`
  counts the leaf entries under it, so `Count` is O(1).
- Canonical forms, enforced on decode (`ErrCorrupt`): a value at or under
  the inline limit (256 KiB, repo geometry) is inline and a longer one is a
  stream ref; a level with one node is the top (no single-child root); only
  the empty map has a node without entries; level ≤ 63.
- **The empty map** is the leaf `01 00 00`, root
  `fb50dc0717ff266cf9baf82b1ce7a1c2ef6d9247859680b11a19fb7077f5f222`.
- A key over 4 KiB is `ErrKeyTooLarge`, and the edit is not applied.

### Streams (`core/stream`)

```
index node  0x02 · level u8 (≥ 1) · count uvarint (≥ 1) · (child [32] · size uvarint) × count
stream ref  root [32] · size · depth
```

- Data is cut by `core/cdc` with the repo's geometry, each piece a raw data
  chunk (at most 1 MiB). Level-1 index nodes list data chunks and higher
  levels list index nodes; each entry carries the byte length under it, so a
  read at any offset descends straight to its chunk. Index nodes split by the
  rule above, windowed on the child hashes.
- Depth 0: the root is the stream's only data chunk (every one-piece stream,
  including the empty stream, which is the empty chunk). Otherwise the root is
  an index node of level = depth, with more than one child.
- Reads verify every chunk by SHA-256 (the chunk store) and every size
  against the bytes actually found (`ErrCorrupt`).

### Editing and diff

- An `Editor` buffers puts and deletes; `Flush` applies them in key order,
  level by level. At each level it re-chunks from the start of the first node
  an edit touches and stops once a new boundary falls where an old one was,
  past the last edit of that stretch; from there the old nodes are reused,
  not rewritten. A value edit that keeps the value's length changes no
  boundary, so it rewrites exactly one node per level: height + 1 nodes.
- `Diff` walks both trees in key order and skips every pair of aligned
  subtrees whose hashes match, so its reads grow with the size of the change
  times the height, not with the size of the map. Wherever both sides' parent
  entries match (same key, same child), the child is skipped without being
  read, at the highest level where they match (Dolt's `skipCommon`); two maps
  with equal roots read nothing.
- Bounds the tests hold the code to: one edit reads at most 3·(height+1)
  nodes to flush; a same-length value edit writes exactly height+1; a flush
  that changes nothing writes nothing (a re-chunked node identical to the
  one it replaces is named, not stored again); a diff of n changes reads at
  most 4·n·(height+1).
- Every store error surfaces as that error, from every operation at every
  point it touches the store: a failed read is never a missing key, the end
  of an iteration or "no difference", and a flush that failed leaves the
  editor's edits in place to retry.
- Values over the inline limit are written as streams when the editor
  flushes; `Get`, iteration and `Diff` read them back whole. The map and its
  editor are concrete types, not a port: there is one implementation, and
  the chunk store beneath it is the swappable part.

## 8. History and models (C3)

A repository is a history of **namespaces**: each commit names one prolly map
from path to typed object, so a table, a folder of files and a JSON document
branch, diff and merge together (Storage Core Spec, "Object model and commit
graph"). The Engine Spec's L2 and L3 rules apply unchanged.

**Chunk kinds**, continuing §7:

| First byte | Chunk |
| --- | --- |
| `0x03` | commit, v1 |
| `0x04` | tag, v1 |
| `0x05` | working set, v1 |

### The repository config (`config/<repo id>`, in the BlobStore)

Written once by `Init`, never changed, and read by `Open` before the chunk
layer: it says how everything else was written.

```
config     "SCRC" · version u16 · repo id [16] · key id [32] · salt [32] · seal(Config, ctx = header, plaintext)
plaintext  "SCRP" · version u16 · cdc min u32 · cdc max u32 · cdc mask u64 ·
           node min u32 · node target u32 · node max u32 · inline limit u32 · pack size u32
```

The repo id and key id ride in the header in the clear (neither is a secret,
and no key can be derived without the repo id); the header is
authenticated. The key id is outside the seal so that `Open` can tell a
wrong master key (`ErrWrongKey`, before trying to decrypt) from a damaged
config (`ErrConfig`). A header or plaintext of another magic or version, a
plaintext with bytes left over, and a geometry the core cannot use are
`ErrConfig` even when they authenticate.

`Init` asks for admin before it writes anything. It writes its config under
a name of its own, `config/<repo id>`, so no two `Init`s ever write one
name, and then claims the store with the first swap of the root, which is
atomic wherever the root lives: the `Init` whose swap lands owns the store,
and another's config is garbage nothing reads. (`Init` once claimed the
store by writing one `config` object put-if-absent, which a store whose
put-if-absent is best effort could let two `Init`s overwrite.) `Open` reads
the configs that open under its key and takes the one whose repo id
authenticates the root's manifest; with a root and none of them, the key did
not create the repository (`ErrWrongKey`). Configs and no root mean an
`Init` stopped before the version graph (a crash, a backend that went away):
`Open` reports the store as `ErrNoRepo`, and the next `Init` with the same
key finishes it, on the repo id and geometry of the config it left (the
first by name). A store holding a finished repository is `ErrExists` to
`Init`.

### Objects (`core/object`, `core/model`)

- An **object reference** is the namespace's value, a fixed 46-byte record:
  `model u16 · format u16 · flags u8 · depth u8 · size u64 · root [32]`.
  `root` is the object's top chunk, `size` its logical size, and `depth`
  the stream depth for objects rooted in a stream (0 otherwise). The spec's
  "flags" is split into flags (all zero in v1) and depth.
- **Paths** follow an allowlist: valid UTF-8, `/`-separated, each segment 1
  to 255 bytes, not `.` or `..`, no byte under 0x20 or 0x7f, at most 64
  segments and 4,096 bytes in all (the prolly key limit). Anything else is
  `ErrInvalidPath` at write; nothing is normalized.
- The **model port** is `core/model`: a model has a stable id and a format
  version, and validates, diffs and merges its objects given their roots.
  `model.Root` (hash, size, depth, and the format version the object was
  written in, so a model can read its older formats) is what a model sees;
  the object reference adds the model id and flags. An object in a format
  newer than its model knows is refused, like an unknown model. Registries are
  built explicitly (`model.NewRegistry(...)`, a duplicate id is an error);
  an id the registry lacks is `ErrUnknownModel`, and nothing is decoded.
- **Generic diff** is a prolly diff of two namespaces: a path added, removed,
  or changed; for a change within one model, that model's diff says where.

### History (`core/vcs`)

- The chunk store's root is the **refs map**, a prolly map: `heads/<branch>`
  → commit, `tags/<name>` → tag, `work/<branch>` → working set, each a 32-byte
  hash. Every change to it is one `CompareAndSetRoot`; a writer that loses
  re-reads and re-applies.

```
commit       0x03 · parents u8 (0–2) · parent [32] × parents · namespace [32] · height uvarint ·
             time i64 (UTC, unix ns) · author (uvarint length ≤ 256, UTF-8) · message (≤ 64 KiB, UTF-8)
tag          0x04 · target [32] · time i64 · tagger (≤ 256) · message (≤ 64 KiB)
working set  0x05 · working [32] · staged [32] · merging u8 (0 or 1) ·
             [base [32] · theirs [32] · conflicts [32] ·
              working before [32] · staged before [32]]   (when merging)
```

- **Height** is 0 for a root commit and one more than the higher parent
  otherwise, so `MergeBase` can stop early. The first parent of a merge is
  the branch merged into.
- The **author** comes from the `Principal` of the call, never from input.
- **Branch and tag names**: `^[a-zA-Z0-9][a-zA-Z0-9._/-]{0,127}$`, with no
  `..`, no `//`, and no trailing `/` or `.lock`; anything else is refused.
- `Log` takes a limit, capped at 10,000.
- **Tags** are read back by name (`Tag`, read on `tag:<name>`), listed
  (`Tags`, read on the repository) and deleted (`DeleteTag`, manage on
  `tag:<name>`); a name the refs map does not hold is `ErrTagNotFound`. A
  tag does not move: deleting it and creating it again names another
  commit. What only a deleted tag reached is unreachable, and GC collects
  it.
- A working set names stored namespaces, and a branch or tag a stored
  commit. `UpdateWorkingSet` changes the namespaces only: the merge state
  it is handed must be the stored one (`ErrMergeState`), since only
  `Merge`, `ResolveConflict`, `CommitWorkingSet` and `AbortMerge` change it.
  `Commit` is `UpdateWorkingSet` then `CommitWorkingSet` in one publish
  (#10): a host that edits a namespace and commits it pays one swap, one
  index object and one pack of metadata, not two of each.
- **Abandoning a merge.** `Merge` merges into the working namespace as it
  is, uncommitted edits and all, so the merge state records the working and
  staged namespaces it started from. `AbortMerge` drops the merge state and
  puts those two back: what the merge brought in, its resolutions and every
  edit made since it began are discarded, and edits made before it are
  not. It asks for write on the branch and on every path it changes, and a
  branch with no merge in progress is `ErrNoMerge`. (The two roots joined
  the working set's merge layout before any release; no repository held
  the shorter one.)
- A writer that loses the root swap re-reads and re-applies, up to 1,000
  times in a row, then gives up with an error; nothing it did reaches the
  root. A store error anywhere is the call's error, and leaves the refs as
  they were.
- Every call takes a `Principal` and asks the `Authorizer` about exactly
  what it does (read, write or manage a branch, read or manage a tag, admin
  the repository), except `Namespace`: it opens what a hash names, and a host
  that has the hash has the chunk store it came from. Writes are also
  authorized per path (the spec's "per path prefix"): every write asks for
  write on each path it changes, `path:<branch>:<path>`. `UpdateWorkingSet`
  diffs the working set stored under `prev`, never the caller's copy;
  `CommitWorkingSet` diffs the head against what is staged, so committing
  someone else's staged change needs the committer's own permission;
  `Commit` diffs all three, the stored working and staged namespaces and
  the head, against the namespace it commits;
  `Merge` diffs the working namespace against the result;
  `ResolveConflict` asks for its path. Reads stay per branch: a host holding
  the chunk store can read what it can open, so a per-path read rule belongs
  in the host's own read API.

### Merging (`core/merge`)

- Both diffs (base → ours, base → theirs) stream in path order and are
  zipped; memory stays bounded whatever the size of the namespaces.
- Per path, the Engine Spec's table: one side changed, take it; both made
  the same change, take it; both changed the same object under one model,
  ask that model; an add against a different add, a delete against an edit,
  or two models for one path, a conflict.
- Conflicts go into the working set (`conflicts`, a prolly map from path to
  record) so a session can resolve them later; a commit is refused while any
  remain. More than 100,000 is `ErrTooManyConflicts`, and a model's error
  aborts the merge; either way the working set is left exactly as it was.
- One merge at a time per branch. Merging a commit the branch already holds
  (its head or an ancestor) changes nothing and starts no merge. Merging a
  descendant is not fast-forwarded: it merges, and the commit that finishes
  it has two parents.

```
conflict   kind u8 (1 both changed · 2 delete against edit · 3 add against add · 4 model change) ·
           (present u8 · object reference [46]) × 3 (base, ours, theirs) ·
           model conflicts uvarint (≤ 10,000) · (location (≤ 4 KiB) · reason (≤ 1 KiB)) × that many
```

## 9. Garbage collection (C4, `core/gc`)

GC deletes packs nothing reaches, and nothing else. The Storage Core Spec asks
that it mark "everything reachable from refs, working sets and a configurable
grace window (default 7 days)" and delete "through the GC role only"; this is
how, with any number of writers on the same backend.

**The GC role.** The repository runs on `blob.NoDelete(store)`; `core/gc` is
handed the raw store and is the only caller of `Delete`.

**Reachability.** Marking starts at the manifest's root, the refs map, and
follows: the map's nodes; `heads/<b>` to a commit; `tags/<t>` to a tag and its
target; `work/<b>` to a working set, its working and staged namespaces, and,
during a merge, its base and theirs commits, its conflicts map (whose
records name objects too) and the namespaces it started from; a commit to its namespace and its parents; a
namespace to its nodes and to every object it names. What an object reaches
only its model knows (a tree entry holds its blob's root inside the model's
bytes), so a model makes its objects collectable by implementing
`model.Walker`, an optional interface beside the frozen port:

```go
type Walker interface {
	// Walk calls visit for every chunk the object reaches, root first. For a
	// chunk that reaches others (leaf false), visit says whether to go on into
	// it: no, for one already gone into, so history shared between commits
	// is walked once. A leaf (true) is named and not gone into.
	Walk(ctx context.Context, root Root, r chunk.Reader, visit func(h hash.Hash, leaf bool) (bool, error)) error
}
```

The leaf flag is not a nicety. The same bytes can be a data chunk in one
place and a node in another (a file whose bytes are a node's), and a marker
that pruned on "seen" alone, meeting them first as data, would never go into
the node, and GC would delete what it reaches. A writer could craft that on
purpose. So the marker prunes only a node it has already gone into.

`stream.Walk` (a stream's index nodes, and its data chunks, named but never
read) and `prolly.Walk` (a map's nodes, the streams of its long values, and
each value handed to the caller) walk the core's own structures, so a model
built on them walks in a few lines. GC refuses to collect a repository holding
an object whose model is not registered or cannot walk: it fails closed,
condemning and deleting nothing.

**Sweep.** A pack is garbage when none of its chunks is marked. Packs are not
rewritten in v1: a pack holding one live chunk is kept whole.

**Condemn, wait, delete.** GC never deletes what it has just found
unreachable. One run:

1. Read the manifest (version v, root R, gcGen g, condemned list) and mark
   from R.
2. A condemned pack that is marked again is reprieved: something published a
   root that reaches it. A pack condemned at least the grace window ago and
   still unmarked expires. Any other garbage pack is condemned now.
3. Swap the manifest from v: the condemned list updated; expired packs
   dropped from the index objects, which are rewritten into one (the index
   objects it replaces are condemned in turn, so readers holding an older
   manifest can still load them for a grace window); gcGen + 1 when anything
   expired. A swap that loses to a writer starts the run over, from a fresh
   read and a fresh mark.
4. Delete the expired packs and the expired index objects, then the
   orphans: packs and index objects no manifest names (left by a writer
   that never published) that are older than the grace window. GC lists
   them before its swap and records each in it as deleted, at its own
   clock, and deletes only once the swap has landed; the record stays for a
   grace window and an hour. An object under `packs/` or `index/` whose
   name no pack or index object could have is deleted unrecorded, since no
   writer uploaded it; so is every orphan of a store with no manifest yet,
   where there is nothing to record in and no manifest to publish against.

**Writers.** Put never deduplicates against a chunk whose only copy is in a
condemned pack; it stores it again. A writer remembers the chunks it did
deduplicate, and when the manifest's gcGen has moved by the time it
publishes, it first checks each is still stored. A store that sees a new
gcGen rebuilds its index from the manifest's index objects, so nothing
points into a deleted pack.

A writer's own unpublished packs and index objects are checked at publish
too: the publish is refused if the manifest it would replace records one of
them as a deleted orphan, or if one it uploaded more than an hour ago (by
its own clock) is no longer stored. The record makes a publish that races
the deletion fail inside the swap itself: GC's swap comes first, so the
writer either swaps against a manifest that carries the record or loses to
it and reads it next; the existence check covers deletions older than a
record lasts, and an ordinary publish, whose uploads are fresh, checks
nothing.

**A lost session.** Either way, what the store told its callers was stored
is partly gone, and it cannot know which roots in flight reach it: a chunk
written into a deleted pack was promised to a put as surely as one found
stored. So the publish fails with `chunk.ErrSessionLost` (the version
graph's `ErrSessionLost`), and from then on the store refuses every write;
reads go on. A read that finds such a chunk gone ends the session the same
way, since a host reads what it has just written before it publishes: a
chunk a put counted on, or one in a pack of the store's own it has not
published, is `ErrSessionLost` when it cannot be read, never a plain miss
or corruption. The host reopens the repository, which starts a fresh
session, and writes again. Only a host that broke the grace window gets
here.

**Clocks.** Expiry is GC's own reckoning: it dates condemnations by its
clock, and an expired pack is deleted by name whatever else is true. An
orphan's age is the backend's to tell, since the backend stamps its
objects: GC puts a probe under `gc/`, reads the stamp it got, and measures
ages against that, never against its own clock. A backend whose clock runs
behind GC's would otherwise make a writer's upload of a moment ago look old.

**Entry point.** `repo.GC(ctx, principal, options, grace)` needs admin and
the raw store. Run on a `NoDelete` store it condemns but cannot delete
(`ErrDeleteForbidden`); what it expired is an orphan by then, and the next
run on the raw store deletes it.

**Reclaiming space.** GC frees whole packs, and a pack that is mostly dead
is rewritten (#1): in a round, a kept pack whose live bytes are under half
its size (`Repack.MaxLive`) is a candidate, and candidates are repacked
emptiest first until the round has copied its budget (`Repack.Budget`, a
GiB by default; `Repack.Off` turns it off). The round reads each candidate
in one GET, opens the live frames credited to it (a chunk two packs hold
counts for the first listed and is copied from that pack alone, or the new
pack would hold a chunk counted dead and be repacked again every round)
and seals them into new packs, uploads
those before its swap (a swap that loses leaves them orphans, which a later
run deletes), lists them in its index objects, and records the old pack as
repacked (condemned kind 5): a repacked pack is never reprieved by the
chunks it still holds, since the new packs hold them; it stays listed in
the index objects and expires a grace window later like any condemned
pack, so a reader that located a chunk in it still reads it, and one that
reads it after it is gone refreshes and finds the chunk in its new pack.
A pack whose every chunk is live is never a candidate, whatever its
overhead; a condemned pack found live again and mostly dead is repacked in
the round that reprieves it. The round moves gcGen, so every store
rebuilds its index, adding the packs still in service before the
condemned and repacked ones, and finds the live chunks in the new packs
at once; a writer never deduplicates against a repacked pack.

**Memory** (#6). A round holds a record per pack and none per chunk: Begin
indexes every listed pack's chunks in a `dedup` table under
`Options.IndexDir` (the packs in service first, the condemned and repacked
after, in a second pass over the objects listing them), and the mark is a
table too, built as the walk visits chunks; the walk remembers up to
`gc.Options.MarkNodes` nodes (1 Mi by default) to skip when reached again,
and past that walks a shared subtree again, at a cost in time and never in
chunks. Apply joins the two tables in one pass over both, in hash order,
counting each pack's live chunks and frame bytes (a chunk two packs hold
counts for the first, the one in service if either is), decides each pack
on the counts, checks a candidate's frames against the mark as it streams
the pack's bytes from one GET, and rewrites the index objects by streaming
the old ones through, one object's packs in memory at a time. GC's reader
runs without a chunk cache, since the walk reads each chunk once.

**Proof.** `TestGCSafetyProperty` drives random histories the way a host
following the contract does, with GC between the steps as the clock moves
on, edits that span collections (and are fenced), merges left in conflict,
and deleted branches, and checks after every run that the whole repository
reads, and at the end that nothing unreachable is left to condemn.

**What a host must do.** Publish what it writes, and use what it reads,
within the grace window. A chunk is deleted only when it was unreachable at
two marks at least a grace window apart, so a host that holds a hash, or an
unpublished write, across that span can lose it; the checks at publish
cover deduplication and the writer's own uploads, and `UpdateWorkingSet`
refuses namespace roots that are not stored, but a host that sits on hashes for a week is outside the contract, as
it is with git's `gc.pruneExpire`.
