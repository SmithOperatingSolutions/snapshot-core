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
| D8 | What "reuse" amounts to | chunker (geometry, rolling hash, table), hasher (identity), crypto (AEAD, Argon2id, X25519). Everything else is ours | Matches the spec's "hardest-won quarter" |
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
| Prolly split: `sha256(e.key)` as uint32 under a threshold rising with node byte size; 512 B / 4 KiB / 16 KiB | As written for level 0 (bytes 0–3 of `sha256(key)`, big-endian). Internal levels read the next 4-byte window of the same digest, because re-using bytes 0–3 at every level correlates splits across levels (a key that ended a leaf is biased to end its parent too). See §6 for the exact threshold, computed in integers so every platform draws the same tree |
| "Boundaries depend on keys only, so value edits never reshape the tree" | **Reconciled, not literal.** The threshold rises with the node's byte size, and byte size includes values; that is what keeps nodes inside 512 B–16 KiB for tables with wide rows. So a value edit that keeps the value's length never reshapes the tree (tested: exactly height+1 nodes rewritten); one that changes its length can move a boundary locally, and the tree resyncs at the next boundary (tested: bounded) |
| Key ≤ 4 KiB, value ≤ 256 KiB inline, larger values in a blob tree | As written. The inline limit is repo geometry (recorded at init). A node that reaches 16 KiB ends after the entry that crossed it, so a node is at most 16 KiB plus one inline value |

## 3. Layers (enforced by `depguard`)

Lower rows never import higher rows.

| Layer | Packages | Imports |
| --- | --- | --- |
| Adapter | `core/dnx` | disknexus-engine (the only importer), stdlib |
| Primitives | `core/hash`, `core/auth`, `core/limits` | `core/dnx` (hash only), stdlib |
| Crypto, chunking | `core/seal`, `core/cdc`, `core/boundary` | primitives, `core/dnx` |
| Backends | `core/blob`, `core/blob/{mem,local,multivol,s3,cache}` | stdlib, AWS SDK (s3 only) |
| Packs | `core/pack`, `core/dedup` | seal, hash, zstd |
| Chunk layer | `core/chunk` (port), `core/chunk/{memstore,packstore}` | blob, pack, dedup, seal |
| Keyed data | `core/stream`, `core/prolly` | chunk, cdc, boundary |
| Objects | `core/model` (port), `core/object` | prolly, stream |
| History | `core/vcs`, `core/merge` | object, model, auth |
| GC | `core/gc` | everything below; the only caller of `BlobStore.Delete` |
| Entry point | `core/repo` | everything below |
| Models | `model/blob`, `model/tree`, `model/contract` | `core/model`, `core/stream`, `core/prolly`, `core/object` |

## 4. What a repository looks like in a BlobStore

| Name | Mutable | Sealed under | Holds |
| --- | --- | --- | --- |
| *(root)* | yes, CAS only | `vdb/refs/v1` | the manifest: refs root hash, live index objects, GC generation, condemned objects |
| `config` | no | `vdb/config/v1` | repo id, key id, CDC and prolly geometry, pack limits |
| `packs/<sha256 of pack bytes>` | no | `vdb/chunk/v1` (frames), `vdb/pack-index/v1` (trailer) | many chunks, each compressed then sealed, plus an in-pack index |
| `index/<sha256 of object bytes>` | no | `vdb/index/v1` | chunk hash → pack and offset, for the packs of one or more commits |

Keys never live in the BlobStore ("keys never stored beside data"): the host
holds the master key through a KMS wrapper or a passphrase key file it stores
elsewhere.

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
| Key file (139 B) | `"SCKF"` · version u16 · Argon2 time u32 · memory u32 · threads u8 · salt [32] · key id [32] · AES-GCM(KEK, master) [60]; AAD `"vdb/keyfile/v1" 0 header[0:79]` |
| KMS envelope | `"SCKW"` · version u16 · key id [32] · wrapped (len-prefixed, ≤ 8 KiB) |
| Pack | header `"SCPK"` · version u16 · flags u16 · salt [32]; frames `seal(Chunk, ctx = chunk hash, zstd-or-raw)`; index `seal(PackIndex, ctx = header, "SCPI" · version · count · entries sorted by hash: hash [32] · offset · stored · raw · codec)`; trailer index-offset u64 · index-length u32 · `"SCPE"` |
| Index object | `"SCIX"` · version u16 · salt [32] · `seal(Index, ctx = header, "SCIP" · version · packs: pack hash [32] · salt [32] · size · entries …)`, packs and entries strictly sorted |
| Manifest (the root value) | `"SCMF"` · version u16 · salt [32] · `seal(Refs, ctx = header, "SCMP" · version · seq u64 · gcGen u64 · root [32] · index-object hashes (sorted) · condemned list)` |
| local store | `.snapshot-core` marker (`"SCLS"` · version · store id [16]) · `objects/<segment>~` · `tmp/` · `root` (`"SCRF"` · version · value · SHA-256) · `root.lock` |
| multivol map | `"SCMV"` · version u16 · count u16 · (volume id [16] · path) … · SHA-256 |
| S3 keys | `<prefix>objects/<name>!` (the `!` keeps any key from being both an object and a path prefix, which MinIO hides from listings; it sorts below every name byte) · `<prefix>root` = 16-byte nonce ‖ value · `<prefix>probe/…` |
| Disk cache entry | `"SCCE"` · object name · SHA-256 of content · content |

Object names: packs are `packs/<first byte hex>/<SHA-256 of the pack bytes>`,
index objects `index/<SHA-256 of the object bytes>`; readers verify both.

## 6. The chunk layer protocol (`core/chunk/packstore`)

- **Put** adds to an in-memory pack writer. A full pack is finished, added to
  the in-memory index, and uploaded; until the upload is confirmed its bytes
  stay readable from memory. A failed upload is kept and retried by the next
  CompareAndSetRoot, which refuses to publish while any pack is unstored.
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
- Scale limit (v1): the whole index lives in memory (~70–80 bytes per chunk),
  and the manifest lists every index object until GC compacts them (C4).

*(The prolly tree, the version graph, merge and GC are added below as each lands.)*
