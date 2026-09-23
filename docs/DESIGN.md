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

*(Formats, the chunk layer protocol, prolly, the version graph, merge and GC
are added below as each lands.)*
