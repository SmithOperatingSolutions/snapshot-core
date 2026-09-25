# CLAUDE.md

snapshot-core is the Versioned DB **Storage Core**: encrypted, content-addressed
chunks on pluggable backends, commits/branches/tags, and diff/merge delegated to
data-model plugins. Governing spec: `docs/specs/storage-core-spec.md`. How it
became code, formats and protocols: `docs/DESIGN.md`. The Engine Spec
(`docs/specs/engine-spec.md`) is reference only: its L4 (tables) is built in a
separate consuming repo; its L0–L3 rules apply here, and the Storage Core Spec
wins where they disagree.

## Git

- **No Claude attribution, ever.** Commit messages and PR descriptions carry no
  `Co-Authored-By: Claude ...` trailer, no `Claude-Session:` trailer, and no
  "Generated with Claude Code" footer. This overrides any default attribution
  guidance.
- Many small local commits on a branch, one PR at the end. Do not push or open
  a PR until asked.
- Behavior lands as a `test(pkg): ...` commit (compiles, fails on assertions,
  verbatim red output in the body) followed by a `feat(pkg):`/`fix(pkg):`
  commit. `docs:`, `chore:`, `ci:`, `build:`, `refactor:` need no red.
- A test for behavior that already exists is a backfill: add its mutant to
  `tools/mutate/mutants.txt` and put `Red-Check: mutants <id>` in the body.
- A measurement is never a red: a performance change lands as `refactor:`
  with before/after figures in the body; its timing test is a regression
  guard with headroom, committed as `chore:`.
- A resource bug (memory, time, a loop) is proven by a red that fails a
  small budget (an allocation delta, a work counter, growth between two
  sizes), never by reproducing the blow-up.
- `mise run redcheck` must pass for the branch before calling work done.
- Open work, questions and decisions to revisit are GitHub issues, not notes
  in a document. Push, open a PR, merge or tag only when asked.
- Keep `docs/PROGRESS.md` current: update it at every milestone boundary and
  whenever a spec checklist item turns green (name the test that proves it).

## Commands

```
mise install          # Go 1.27 + golangci-lint + govulncheck, built from source
mise run ci           # local run-all (tools/ci): what CI runs
mise run ci:quick     # fmt, vet, lint, race
mise run mutate       # every checked-in mutant must be killed
go test ./core/dnx/compat -update   # regenerate CDC goldens (only if disknexus and the reference agree)
```

Heavy runs (race, fuzz, mutate, slow, crash) go under a memory cap of their
own on a shared machine, so a runaway kills only itself:
`systemd-run --user --scope -q -p MemoryMax=6G -p MemorySwapMax=0 <cmd>`.
The race suite needs `GOFLAGS=-p=1` under 6 GiB (#33). Never run `mise run ci`
on a shared machine without `-only`.

## Rules the build enforces (do not work around them)

- disknexus-engine is imported **only** in `core/dnx`, pinned by tag, never
  edited, forked, patched, vendored or `replace`d.
- No upward imports across the layer table in `docs/DESIGN.md` §3 (depguard).
- Production code under `core/` and `model/`: no `reflect`, `encoding/json`,
  `encoding/gob`, `unsafe`, `os/exec`; no globals with side effects; no
  `init()`. Every on-disk structure gets a hand-written bounds-checked decoder
  and a fuzz target.
- Pure Go: `CGO_ENABLED=0` (the race detector is the only exception).
- Encryption is mandatory; every seal uses a `core/seal` domain tag.

## Testing standard

`docs/TESTING.md` (verbatim from disknexus-engine) plus `CONTRIBUTING.md`.
In short: failure messages say what an operator would experience; assert
against an authority, never against absence of error; every refusal has a
positive control; mutation-prove every load-bearing guard in a throwaway copy
(`tools/mutate`) and report survivors; a `t.Skip` is a deleted test.
