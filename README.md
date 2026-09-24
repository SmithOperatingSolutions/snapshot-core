# snapshot-core

The storage core of the Versioned DB: it versions any kind of data. It stores
content-addressed, encrypted chunks on pluggable backends (local disk, several
mount points, S3), records commits, branches and tags, and diffs and merges by
delegating meaning to data-model plugins. It knows nothing about tables, SQL
or protocols; the table model lives in a consuming repository as a plugin.

**Status:** the milestones of the [Storage Core Spec](docs/specs/storage-core-spec.md),
C0 to C4, are done: backends and encrypted packs, content-defined chunking and
prolly trees, commits and merges over typed objects, and garbage collection.
Every checklist item and the test that proves it: [`docs/PROGRESS.md`](docs/PROGRESS.md).
Remaining work is tracked in the [issues](https://github.com/SmithOperatingSolutions/snapshot-core/issues).

## Using it

A host (an application, or the consuming repository's engine) opens a
repository with four things: a backend, a master key, the data models its
objects use, and an authorizer. Everything below is taken from
[`e2e/example_test.go`](e2e/example_test.go), which runs with the tests.
Packages are under the module `github.com/SmithOperatingSolutions/snapshot-core`:
`local` is `core/blob/local`, and `blob` and `tree` are the models `model/blob`
and `model/tree` ([Packages](#packages)).

```go
store, _ := local.Create("/srv/repo", local.Options{})    // or mem.New(), s3.Open(ctx, ...), multivol.Create(...)
keys, _ := seal.NewKeyring()                               // keep it with seal.NewKeyFile (a passphrase) or seal.WrapKeyring (a KMS)
models, _ := model.NewRegistry(blob.Model{}, tree.Model{Config: repo.DefaultGeometry().Prolly()})
me := auth.Principal{ID: "user:me"}
o := repo.Options{Blobs: store, Keys: keys, Registry: models, Authorizer: auth.AllowAll{}}

r, _ := repo.Init(ctx, me, o)   // a new repository: main, at an empty first commit
// r, _ := repo.Open(ctx, o)    // an existing one
defer r.Close()
```

**Write and commit.** Objects are written into the repository's chunk store
through their model, named by a path in the branch's working set, and
committed:

```go
root, _ := blob.Write(ctx, r.Chunks(), strings.NewReader("hello\n"), r.Config.Geometry.Stream())

ws, _ := r.WorkingSet(ctx, me, vcs.MainBranch)
n, _ := r.Namespace(ctx, ws.Working)
e := n.Editor()
e.Put("notes/hello.txt", object.Ref{Model: blob.ID, Root: root})
n, _ = e.Flush(ctx)
next := ws
next.Working, next.Staged = n.Root(), n.Root()
_, err := r.UpdateWorkingSet(ctx, me, vcs.MainBranch, ws, next) // vcs.ErrConflict: read again, write again

commit, _ := r.CommitWorkingSet(ctx, me, vcs.MainBranch, "add a note")
```

**Branch and merge.** `CreateBranch`, `Merge` (conflicts are recorded in the
working set, and a commit is refused until they are resolved), `Conflicts`,
`ResolveConflict`, then `CommitWorkingSet`, which makes a commit with two
parents; or `AbortMerge`, which puts the working set back as it was before
the merge. `Log`, `MergeBase`, `Head` and `Branches` read history; `CreateTag`,
`Tag`, `Tags` and `DeleteTag` name commits.

**Read.** Open a commit's namespace, `Get` a path, and read the object through
its model (`blob.Open`, `tree.Read`).

**Collect.** `repo.GC(ctx, admin, o, grace)` deletes what nothing reaches: it
needs admin and the raw store (the repository itself runs on one that cannot
delete). It condemns first and deletes a grace window later (seven days by
default), so run it on a schedule.

### What a host must do

- **Retry on `vcs.ErrConflict`** by reading again and writing again: another
  writer got in first.
- **Reopen on `vcs.ErrSessionLost`** and write again: GC deleted writes the
  repository held unpublished past the grace window, and the open repository
  refuses every further write.
- **Publish within the grace window** what it writes, and use within it the
  hashes it reads. GC deletes only what was unreachable at two marks a window
  apart (docs/DESIGN.md §9).
- **Register every model its objects use.** An object of an unknown model is
  refused, and GC will not collect a repository holding an object whose model
  cannot walk (`model.Walker`). New models must pass [`model/contract`](model/contract).
- **On a provider that ignores conditional writes** (Backblaze B2 is reported
  to; `s3.Open` refuses such an endpoint), keep the objects there and the root
  on a local disk: `s3.Open` with `ObjectsOnly: true`, `local.Create` for the
  root, and `split.New(split.Options{Objects: objects, Roots: roots})`, which
  keeps a copy of the root on the objects store (`split.Mirror` picks the mode;
  the default waits for each copy). Close the split store after the
  repository; if the disk holding the root is lost, `split.Recover` seeds a
  fresh root store from the copy.
- **Authorize.** Every call takes a `Principal` and asks the `Authorizer`
  about `repo`, `branch:<name>`, `tag:<name>` and, for writes, every
  `path:<branch>:<path>` it changes. A nil authorizer denies everything.

## Packages

| Layer | Packages |
| --- | --- |
| Backends | `core/blob` (the port) · `core/blob/mem`, `core/blob/local`, `core/blob/multivol`, `core/blob/s3`, `core/blob/cache` · `core/blob/split` (objects on one store, the root on another) |
| Primitives | `core/hash` · `core/wire` (bounded reader and writer for every record) · `core/auth` |
| Crypto, chunking | `core/seal` (keys, key files, KMS wrapping) · `core/cdc` · `core/boundary` |
| Chunk layer | `core/pack`, `core/dedup` · `core/chunk` (the port) · `core/chunk/packstore`, `core/chunk/memstore` |
| Keyed data | `core/stream` (byte streams) · `core/prolly` (the ordered map) |
| Objects | `core/model` (the plugin port) · `core/object` (namespaces of typed objects) |
| History | `core/vcs` (commits, branches, tags, working sets) · `core/merge` |
| GC, entry point | `core/gc` · `core/repo` |
| Access | `core/auth` (principals, the default-deny authorizer) |
| Models | `model/blob` (files) · `model/tree` (folders) · `model/contract` (the suite every model passes) · `model/mapobject` (what a map-shaped model needs beside the port) |

Pure Go (`CGO_ENABLED=0`). Depends on
[disknexus-engine](https://github.com/SmithOperatingSolutions/disknexus-engine)
as a pinned, unmodified module, imported only by `core/dnx`.

## Versioning and stability

Releases are tags `vX.Y.Z` on `main`, cut after CI is green; every change
lands through a squash-merged pull request, so a tag's history is the list
of pull requests. Until `v1`, a minor version may change Go APIs and says so
in its release notes; a patch version does not.

What is promised from `v0.1.0` on:

- **A later release opens a repository an earlier one wrote.** Every on-disk
  structure carries a version and has a hand-written, bounds-checked decoder
  (docs/DESIGN.md §5, the compatibility contract); a format that changes
  gets a new version, and the reader for the old one stays.
- **The data-model port is frozen.** A model written against `core/model`
  and passing `model/contract` keeps working across minor versions; anything
  added beside it (like `model.Walker`) is optional.
- **Encryption is not optional** and never was: there is no plaintext mode
  to remove.

What a host imports: `core/repo` (open, create, GC), `core/vcs` (commits,
branches, tags, working sets), `core/object` (namespaces), `core/model` and
`model/contract` (own models), `core/seal` (keys), `core/auth`, a backend
under `core/blob/` and the models under `model/`. A model outside the core
also imports `model/mapobject` (the plumbing of a map-shaped model) and
`core/wire` (the bounded reader and writer its record decoders are built on). The rest (`core/pack`,
`core/dedup`, `core/cdc`, `core/prolly`, `core/stream`, `core/dnx`, anything
under `internal`) is how those are built and may change in a minor version.

Go 1.27 or later, `CGO_ENABLED=0`, no cgo anywhere; the one third-party
engine dependency, disknexus-engine, is pinned by tag and upgraded only
deliberately, in its own pull request.

## Developing

```
mise install          # Go 1.27, golangci-lint, govulncheck, built from source
mise run ci           # what CI runs: fmt, vet, lint, vuln, race, coverage gate,
                      # red-check, crash harness, scale guards, fuzz, mutants, S3 tier (SeaweedFS)
mise run ci:quick     # fmt, vet, lint, race
mise run redcheck     # every test: commit fails without its feat:/fix:
mise run mutate       # every checked-in mutant is killed
```

Changes land test-first: a `test(pkg):` commit that fails on assertions, then
the `feat(pkg):` or `fix(pkg):` that makes it pass; a test for behavior that
already exists names the mutant it kills. See [`CONTRIBUTING.md`](CONTRIBUTING.md)
and the testing standard, [`docs/TESTING.md`](docs/TESTING.md).

## Documents

- [`docs/specs/storage-core-spec.md`](docs/specs/storage-core-spec.md): the spec this repository implements
- [`docs/DESIGN.md`](docs/DESIGN.md): how the spec became code, on-disk formats, protocols, GC
- [`docs/PROGRESS.md`](docs/PROGRESS.md): milestones, checklists and their tests, what testing found
- [`docs/specs/engine-spec.md`](docs/specs/engine-spec.md): the consuming engine's spec; its L0 to L3 rules apply here

## License

Apache License 2.0; see [`LICENSE`](LICENSE).
