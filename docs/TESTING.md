<!-- Copied verbatim from disknexus-engine v0.2.11 (commit b2c02f1),
     docs/TESTING.md, Apache License 2.0 (see NOTICE). It is the standard this
     repository's tests are held to; the Storage Core Spec adopts it by name.
     Repository-specific process (commit order, red-check, regression naming)
     is in CONTRIBUTING.md, not here, so this file stays a faithful copy.
     Issue numbers (#NNN) refer to the disknexus tracker. -->

# How we test

One rule, and everything else follows from it:

> **A test must fail when the behavior it names is wrong.**

Fail-first is how we get there, but writing the test first is not the point — a
test written first can still be unable to fail. The point is evidence. Every
rule below exists because a test in this repository broke it, passed for
months, and let a real defect ship.

---

## 1. The red names what an operator would experience

Not internal state. Not "it didn't error."

```
backing up UNCHANGED data after retention deleted a pack re-uploaded it:
new chunks=98 (want 0), stored bytes=3147902 (want 0), pack objects 4 -> 8
```

That message tells you the storage bill doubled. `bloom.Contains(h) == true`
would have told you nothing you could act on, and would have passed against a
filter that had forgotten the entire repository (#365).

Write the failure message for the person who will read it at 2am without this
context.

## 2. Interrogate the fixture before you trust it

Ask: **"what would this fixture look like if the bug were present?"** If the
answer is "identical", the fixture is wrong and the test is decoration.

Real cases from this repository:

- Watcher tests used repos with a non-default chunk profile but never compared
  the resulting geometry. They passed for months while every watcher backup
  wrote default 64KB chunks into fine-grained repos — 68 chunks where the
  shared path cut 365 (#354).
- `buildGCWorld` inserted entries carrying only a strong hash, so its bloom
  contained nothing but weak-hash zero. In that world a **forgotten** bloom and
  a **healthy** one are byte-identical, so nothing built on it could catch the
  forgetting (#365).
- A schedule "fail-closed doors" test used a repo it never initialized, so
  every request 400'd on `no repository at %s` before a window was ever
  validated — a *valid* window produced the same 400 (#378 item 1).
- A test asserting "a backup publishes its change, not the whole index" backed
  up a fully-deduplicating prefix, so it published no delta at all and was
  measuring an empty run (#374).

## 3. Assert against an authority

A corpus digest, a byte-exact restore, the shared code path's output, a
recomputed expectation. Never "no error returned."

The strongest example in the tree: the index invariant oracle used to assert
that every chunk *resolved* — hash found, pack named, object exists. It never
read the pack. A chunk pointing at the wrong pack satisfies all three, so
disabling pack leasing entirely left all 43 packages green while two of three
machines shipped completed, unrestorable backups (#376). The oracle now reads
the frame back and hashes it.

## 4. Every refusal test needs a positive control

Assert the *success* case first, in the same test, on the same fixture. Without
it a refusal test passes on any error — including one from a broken fixture
that never reached the code under test.

## 5. Mutation-prove every load-bearing guard

Before opening a PR: sever the exact line the test protects, run the named
test, confirm it fails. A guard that survives its own mutation is decoration.

- Do it in a **throwaway clone**, never the working tree — concurrent readers
  and other agents will see your edits and chase phantoms.
- Restore from a byte copy (`cp`), never `git checkout` — that has resurrected
  unfixed code here more than once.
- Prefer mutations that **compile**. A mutant that fails to build proves
  nothing.

## 6. When a mutant unexpectedly passes, suspect the mutant first

An invalid mutant looks exactly like a vacuous guard. In one session four
"findings" turned out to be bad mutants: one mangled a `switch`, one removed a
`return` without disabling the report, one used single quotes where the source
used double, and one neutered a different `if` than intended. Verify the mutant
did what you think before you write it up.

Corollary: if the mutation *cannot* reach the branch on this platform, say so
and add a seam. A rename-conflict test was unfalsifiable on Linux — POSIX
renames over open files — and passed whether or not the fix existed. It now
overrides a `renamePack` seam to force the denial POSIX will not give.

## 7. A skip is a deleted test

`t.Skip` on an unavailable dependency is honest for a developer. In CI it is a
test that silently stopped existing. Guard it: fail when the dependency is
declared, and make the harness fail loudly if a fixture drifts under a
threshold that would skip the assertion (#377, #378).

## 8. Know what a text scanner can and cannot do

Source-grep guards are the right tool for exactly one job: noticing a *new*
call site a behavioural table cannot know exists. They are defeated by
respelling — a regex copied with `\d` instead of `[0-9]`, a map declared with
`:=` instead of `=`, `1000*1000` instead of `1e6`. Pair every scanner with
behaviour, and give it a `checked > 0` counter so it cannot pass by scanning
nothing.

## 9. Do not pin a defect

A test that asserts current wrong behavior as correct makes the bug permanent
and invisible. If accepting a legacy shape is right, accept it *and* bound the
cost with a companion test that states the loss, so the next reader sees a
priced trade rather than an endorsement (#378, the v1 segment weak hash).

## 10. Report the mutations that did not fail

Honesty about coverage is worth more than a clean sweep. "M6, M7 and M10 did
not fail on the first pass; here is why, and here is what I changed" is a
better report than 11/11, and it is how three genuinely blind tests in this
repository got found.

---

## 11. The guard you argued for is not the guard you tested

The hazard you reasoned about hardest is the one most likely to be uncovered,
because thinking it through feels like handling it.

`downloadIndexDeltas` skips a delta it already holds, validated by SIZE rather
than by name. The size check exists for one case: a resumed backup keeps its
backup ID and republishes its delta under the same key with more entries, so
trusting the name stages a short index. That reasoning was written out in a
twelve-line comment above the check — and the check had no test. Weakening it
to mere presence SURVIVED the mutation pass (#439).

A comment explaining why a guard is subtle is evidence that it needs a test,
not evidence that it has one. Grep your own prose: where you argued, go look
for the assertion.

## 12. A verdict that needs to win a race is not a verdict

If the assertion only holds when the scheduler cooperates, the usual outcome
is not a flake — it is a test that passes without reaching the behavior.

`TestPauseSuspendsAndResumeRelinks` cancelled a resumed job immediately after
resubmitting it. Cancel almost always won, so the job's RunFunc was never
entered: a test named "cancel a resumed job" spent nearly every run cancelling
a job that had never started. The rare loss surfaced as a `-race` panic and
looked like an engine defect; the common case had been proving nothing for
much longer (#440, #442). The cloud pause gate had the same shape and came to
carry two `t.Skip`s before anyone noticed (#412).

Hold the state, then assert on it. If holding it needs a seam, add the seam —
that is cheaper than an assertion nobody can trust.

## 13. When it will not reproduce, the mutant still must be deterministic

An intermittent failure you cannot reproduce is not a licence to fix by
reasoning and ship on a green run.

The #440 panic did not reproduce locally in 200 runs under `-race`. The fix
does not rest on that: the MUTANT is deterministic — hand the resumed job the
shared closure and the new wait fails every time, because the closure panics
on entry and the channel it signals never closes. Reproducing the original
failure would have been nice; proving the guard has teeth was necessary.

Two corollaries, both learned the same way:

- **Name the culprit from the stack, not from the symptom.** `close of closed
  channel` inside `runRecovered` was filed as "the engine invoked one job's
  RunFunc twice" — serious, since a capture's RunFunc is not re-entrant
  either. The frame below it named the RESUBMITTED job, not the original. One
  misread escalated a fixture bug into an engine defect for half an hour.
- **Build-check every mutant.** A mutant that does not compile reports exactly
  like a surviving one through a test harness that only greps for FAIL. Two
  "survivors" in #439 were unused-import errors. Automate the check; do not
  rely on noticing.

---

## The checklist

Before a PR that adds or changes a test:

- [ ] The red was captured **before** the fix, and its verbatim output is in the commit message
- [ ] The failure message describes an operator-visible consequence
- [ ] The fixture would look *different* if the bug were present
- [ ] Refusals have a positive control
- [ ] The assertion compares against an authority, not against absence-of-error
- [ ] The guard was mutation-proven, in a clone, with a compiling mutant
- [ ] Any mutant that survived is reported, not omitted
- [ ] Every mutant was build-checked, so a compile error cannot pose as a survivor
- [ ] The assertion holds because the test established the state, not because it won a race
- [ ] Wherever the code argues that something is subtle, an assertion covers it
- [ ] No `t.Skip` silently swallows the assertion
