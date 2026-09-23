# Specs

Verbatim copies of the specs this repository implements, so the code and the
text it answers to live side by side. Edit them here, in a PR, when a decision
changes; `docs/DESIGN.md` records how each rule was turned into code and where
we deviated.

| File | Role here |
| --- | --- |
| `storage-core-spec.md` | **Governing spec.** snapshot-core is the Storage Core. |
| `engine-spec.md` | **Reference.** Its L4 (tables) is implemented in a separate, consuming repository. Its L0–L3 rules (backends, chunk store, prolly tree, version graph, diff and merge) moved into the Storage Core and apply here; where the two disagree, the Storage Core Spec wins (see the Sep 22 update note in the Engine Spec). |
