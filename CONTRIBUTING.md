# Contributing

snapshot-core stores other people's data and its history. A defect here is a
commit that will not read back, a merge that silently drops a change, or a
garbage collector that deletes something reachable. The rules below exist for
that reason.

The testing standard is `docs/TESTING.md` (a verbatim copy of
disknexus-engine's). This file adds the process that is specific to this
repository.

## Run everything locally

```
mise install        # Go 1.27, golangci-lint, govulncheck, all built from source
mise run ci         # everything CI runs that this box can run
mise run ci:quick   # fmt, vet, lint, race suite
```

`mise run ci` is `go run ./tools/ci`. It needs nothing but Go: the S3 backend
runs its contract suite against an in-process S3 server (`core/blob/s3/s3fake`).
The runner gates this module's layout by default (`core/` and `model/` are the
product; `core/dnx` and `core/blob/s3` adapt third-party APIs); another module
that takes the core as a dependency runs it as a Go tool told its own layout
with `-product` and `-adapters`, and its `redcheck` and `mutate` steps run the
core's tools the same way.
A run against a real S3 server is an extra tier (`mise run s3`, SeaweedFS in Docker) that CI requires.

## The loop for every change

1. **Red.** Write the smallest test that names the next behavior. Run it and
   confirm it fails *for the right reason*: an assertion, not a compile error
   and not a panic in setup. If the API does not exist yet, add a stub that
   compiles and returns the wrong answer (`errNotImplemented`, a zero value).
2. **Green.** Write the minimum code that passes. No extra options, no
   "while I'm here".
3. **Refactor.** With everything green, remove duplication and fix names.
   Behavior does not change in this step.
4. **Mutation-prove** the guard the test protects (`docs/TESTING.md` §5): add
   the mutant to `tools/mutate/mutants.txt` and run `mise run mutate`. A guard
   that survives its own mutation is decoration.

## Commits

Many small commits on a branch, one PR. Each behavior lands as a pair:

| Prefix | Contains | Must |
| --- | --- | --- |
| `test(pkg): ...` | the new tests, plus stubs so they compile | **fail**, on assertions; the verbatim red output goes in the commit body |
| `feat(pkg): ...` / `fix(pkg): ...` | the implementation | make that red green, and keep everything else green |

Other prefixes (`docs:`, `chore:`, `ci:`, `build:`, `refactor:`) carry no
behavior change and need no red.

`mise run redcheck` (`tools/redcheck`) checks every `test:` commit between the
branch and `main`: it checks the commit out in a scratch worktree, runs only
the tests that commit added or changed, and requires each of them to FAIL on
an assertion. A build failure, a panic, a skip, or a pass blocks the PR. Every
`feat:`/`fix:` needs a `test:` commit since the previous one. CI runs the same
tool on every pull request. A change to a port's contract suite
(`<pkg>/contract`) counts as a change to every Test function that calls it,
so a contract that grows is red on each implementation it catches out.

**Renames are not reds.** redcheck judges every test a `test:` commit
changed, so a rename that touches an existing test file blocks the commit:
those tests pass without the change. A rename goes in a `refactor:` commit
of its own, before the red.

**Backfills.** A test for behavior that already exists (a guard someone argued
for but never tested, `docs/TESTING.md` §11) cannot fail against its parent.
Its red is a mutant instead: add the mutant to `tools/mutate/mutants.txt` in
the same commit and name it in the commit body:

```
test(pkg): pin the stale-swap refusal

Red-Check: mutants mem-swap-compares-version
```

redcheck then requires the commit's tests to pass, and each named mutant to be
killed by those tests alone.

**Measurements are never a red.** A test that times something and holds
it to a bar is a distribution, and redcheck gives a verdict: the tree before
the change passes or fails by the machine's wobble whenever the change's
gain is within a few times the noise, and the PR blocks at random for the
rest of the branch's life. A performance change lands as `refactor:` (or
`chore:` for its test alone) with the figures before and after in the
commit body, measured on the same machine in the same run, and any
behavior it carries gets its own red. The test that guards the figure
afterwards is a regression guard: give its bar headroom against the
measured value, on the slowest machine that will run it, so it fails on a
regression and never on a bad day. A small benchmark process's live heap
sets its GC rate, so compare builds under equal GC headroom (`GOGC=off`
with a `GOMEMLIMIT`) before calling a difference a regression.

**Resource bugs have bounded reds.** A bug that costs memory, time or loop
iterations is proven like any other, with a failing test first. The red
asserts a small budget that the unfixed code measurably exceeds on a small
input, and never reproduces the blow-up itself. Budgets used here: a
`runtime.MemStats` `TotalAlloc` delta over a few hundred KiB, a counter of
reads, visits or root swaps, or growth between two input sizes. The red run
allocates tens of MB at most. A refusal at a limit has a positive control at
exactly that limit.

**Property tests and their red.** A `test:` commit's red run fails its
`rapid` properties on purpose, and rapid saves each failure under
`testdata/rapid/`. Those files record the stub, not a bug: delete them
before committing. (A real property failure found later is the opposite:
minimize it and check it in as a regression case.) And check that a
property's cases reach the inputs it is about: rapid draws mostly small
values, so a property over trees must see deep trees (`deepEnough` in
`core/prolly` fails a run that did not).

## Regressions

Every bug gets a test named after its ticket, written before the fix:
`TestRegression_SC123_MergeDropsDeletedPath`, in `<pkg>/regress_test.go`.
Regression tests are never deleted. A fuzz or property failure is minimized
and checked in as a permanent seed (`testdata/fuzz/...`) or regression case.

## Where work is tracked, and how it lands

- **Issues.** Every piece of work, open question and decision to revisit is
  an issue on this repository. A PR closes its issues with one keyword per
  issue (`Closes #3`, then `Closes #4` on the next line): GitHub closes only
  the first issue in "Closes #3, #4".
- **Branches.** One branch per batch of issues, started from `origin/main`,
  with one PR, squash-merged with its description as the commit body. The
  branch is kept afterwards: it holds the test-first history the squash
  hides.
- **Releases** are annotated tags `vX.Y.Z` on `main`, cut after CI is green,
  with a GitHub release whose notes list the changes. Until `v1` a minor
  version may change Go APIs and says so; a patch version does not. A
  throwaway module (`go get …@vX.Y.Z`, `go mod tidy`, `go build`) proves a
  tag resolves.
- **Before calling a branch done:** `mise run redcheck` over the branch, the
  whole mutant catalog, and `go run ./tools/ci -only` for fmt, vet, lint,
  race and cover. Update `docs/PROGRESS.md`, and `docs/DESIGN.md` for any
  decision (a new one takes the next free D number).

## Heavy runs

The race suite, fuzzing, the mutant catalog and the slow tier can use many
gigabytes. Once, uncapped fuzz workers (Go starts one per core) took a 62 GB
development host out of memory and killed every session on it (#22). On a
shared or development machine:

- Run each heavy command under a memory ceiling of its own, so a runaway
  kills only itself:
  ```
  systemd-run --user --scope -q -p MemoryMax=6G -p MemorySwapMax=0 go run ./tools/mutate -j 2
  ```
- The race suite fits 6 GiB only with `GOFLAGS=-p=1`: `core/gc` alone peaks
  near 5.9 GB under the race detector (#33).
- The fuzz step caps its own workers and memory (`-fuzzparallel`,
  `-fuzzmemlimit`, #22); a fuzz run started by hand should do the same.
- Timed runs on a shared machine take one lock per run (`flock <file> <cmd>`),
  one run per acquisition, and start only when the load is under 2 with no
  other tests running. Record the load beside the figures.

## Boundaries the build enforces

- **disknexus-engine is imported only by `core/dnx`.** `depguard` fails the
  build anywhere else. Never edit, fork, patch or `replace` it; if we need
  behavior it lacks, we wrap it or write our own beside it.
- **No upward imports.** Each layer imports only the layers below it (the
  table is in `docs/DESIGN.md`, enforced by `depguard`).
- **No `unsafe`, no `os/exec`, no `reflect`, no `encoding/json` or `gob` on
  stored bytes** in production code (`forbidigo`, `depguard`). Every on-disk
  structure has a hand-written, bounds-checked decoder and a fuzz target.
  `tools/` and `_test.go` files may use `os/exec`.
- **No globals, no `init()` side effects.** Model registries, authorizers and
  clocks are passed in.
- **Pure Go.** `CGO_ENABLED=0` everywhere except the race detector.

## Security checklist for a change

- Inputs validated with allowlists at the boundary; invalid input rejected,
  never repaired.
- On any error, nothing partial reaches the root.
- Error messages never carry keys, values, or file contents.
- New on-disk structure: hand-written decoder, limits on every length, fuzz
  target, sealed under its own domain tag.
