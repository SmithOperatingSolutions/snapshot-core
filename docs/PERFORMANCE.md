# Performance

What a host pays to write and read, measured with `tools/commitbench`
(`go run ./tools/commitbench -h`). Every figure here was taken on one
machine: an i7-1360P, ext4 with a 4 KiB write + fsync at p50 5.4 ms,
Go 1.27.1. Each run was alone under the measurement lock, started with the
1-minute load under 2 and no other test or bench running
(CONTRIBUTING.md, "Heavy runs"). Figures are one run each unless marked.

## The v0.2.0 baseline

v0.2.0 committed through one publish per commit: on disk, three fsync
latencies (pack, index object, root swap), about 33 ms; in memory, about
850 µs, most of it host work. A small object's write cost about 200 µs of
CPU, so a batch of small objects ran at 5,000 to 6,000 writes a second on
either backend. The table at the end has the v0.2.0 column.

## What each change bought

Each row links to its decision in `docs/DESIGN.md`; the figures are the
ones its commit body carries, measured before and after on the same
machine in the same session.

| change | decision | figure |
| --- | --- | --- |
| #40 small objects' write path | [D12](DESIGN.md#1-decisions-from-the-spec-review-sep-23-2026) | a 100-byte write 200 µs of CPU to about 7; batch of 10,000: 5,599 to 185,499 writes/s (mem), 5,727 to 143,849 (local) |
| #41 a size hint (`stream.WithLen`) | [D13](DESIGN.md#1-decisions-from-the-spec-review-sep-23-2026) | batch of 10,000 from a reader without a length: 18,535 to 174,806 writes/s (mem), 12,476 to 151,879 (local) |
| #42 tiny chunks raw (under 256 B) | [D14](DESIGN.md#1-decisions-from-the-spec-review-sep-23-2026) | batch (mem) 148–184k to 203–233k writes/s, pack bytes unchanged; single writer (mem) 1,704–1,768 to 1,883–1,891 commits/s |
| #43 a commit per object | [D15](DESIGN.md#1-decisions-from-the-spec-review-sep-23-2026) | single writer (mem) 1,688 to 2,371 commits/s (593 to 422 µs); two-step 1,351 / 1,433 to 1,792 / 1,826 |
| #34 journaled commits | [D16](DESIGN.md#1-decisions-from-the-spec-review-sep-23-2026) | one writer on disk 29.5 to 144 commits/s, p50 33 to 6.6 ms; grouped fsync, 16 writers 143 to 592 |
| #42 a tree's nodes raw (`chunk.RawWriter`) | [D17](DESIGN.md#1-decisions-from-the-spec-review-sep-23-2026) | single writer (mem) 2,757 to 4,501 commits/s (mean of two); batch of 10,000 flush 38.9 to 9.9 ms, its pack bytes +12.8% |
| pack writer buffer reuse | not landed | a new pack writer is 29% of a commit's allocated bytes but 3.7% of its CPU; the upper bound (no buffer at all) measured +3.7% commits/s, under the 5% bar (PROGRESS, Next 18) |

## v0.2.0 against v0.3.0

Both binaries run the same harness: `before` is the v0.2.0 tag with the
v0.3.0 `tools/commitbench` added, adapted only where v0.2.0 lacks an API
(no journal, `-reader hinted` is plain, commits by `Commit`); `after` is the
`v030` branch. On disk v0.3.0 commits through the journal (its default);
`blob/mem` keeps no journal. Batch columns: writes/s of the write phase,
then the batch's total (write, flush, commit). The load is the 1-minute
average at each run's start.

| run | backend | v0.2.0 | v0.3.0 | load (before / after) |
| --- | --- | --- | --- | --- |
| single writer, commits/s (p50, p99) | local | 29.1 (33.2 ms, 55.7 ms) | **172.4** (5.7 ms, 10.5 ms) | 1.42 / 1.82 |
| 4 writers, commits/s (p99) | local | 29.3 (2.80 s) | **355.3** (20.5 ms) | 1.65 / 1.86 |
| 16 writers | local | 29.1 (10.4 s) | **1,093.9** (89.9 ms) | 1.96 / 1.73 |
| 64 writers | local | 26.6 (17.6 s) | **681.1** (719 ms) | 1.98 / 1.87 |
| single writer, commits/s (p50, p99) | mem | 1,127.3 (850 µs, 2.21 ms) | **4,531.6** (200 µs, 620 µs) | 1.78 / 1.83 |
| 4 writers (p99) | mem | 1,207.1 (49.0 ms) | **4,196.7** (16.4 ms) | 1.83 / 1.65 |
| 16 writers | mem | 1,463.4 (158.5 ms) | **3,835.3** (95.2 ms) | 1.95 / 1.67 |
| 64 writers | mem | 1,355.1 (418.6 ms) | **2,713.5** (280.1 ms) | 1.88 / 1.59 |
| batch 1,000, lener reader | local | 4,681/s, 255 ms | **105,956/s, 19.5 ms** | 1.86 / 1.77 |
| batch 10,000, lener | local | 5,956/s, 1.765 s | **184,148/s, 90.0 ms** | 1.86 / 1.77 |
| batch 1,000, plain reader | local | 4,741/s, 247 ms | 8,433/s, 131 ms | 1.96 / 1.77 |
| batch 10,000, plain | local | 5,595/s, 1.863 s | 9,652/s, 1.073 s | 1.96 / 1.77 |
| batch 1,000, hinted reader | local | 4,748/s, 247 ms | **100,893/s, 19.5 ms** | 1.96 / 1.78 |
| batch 10,000, hinted | local | 5,970/s, 1.749 s | **180,349/s, 81.4 ms** | 1.96 / 1.78 |
| batch 1,000, lener | mem | 4,688/s, 220 ms | **110,158/s, 12.3 ms** | 1.77 / 1.80 |
| batch 10,000, lener | mem | 5,766/s, 1.778 s | **171,682/s, 81.8 ms** | 1.77 / 1.80 |
| batch 1,000, plain | mem | 4,709/s, 219 ms | 8,949/s, 116 ms | 1.95 / 1.80 |
| batch 10,000, plain | mem | 5,786/s, 1.764 s | 10,749/s, 964 ms | 1.95 / 1.80 |
| batch 1,000, hinted | mem | 4,617/s, 225 ms | **95,171/s, 13.9 ms** | 1.95 / 1.80 |
| batch 10,000, hinted | mem | 5,543/s, 1.836 s | **170,248/s, 89.3 ms** | 1.95 / 1.80 |
| batch 10,000, pack bytes the commit wrote | mem | 2,039,260 | 2,299,986 (+12.8%, D17) | |
| bulk 256 MiB: write MB/s, then commit | local | 504 MB/s, 90.6 ms | 565 MB/s, 96.9 ms | 1.96 / 1.80 |
| bulk 256 MiB | mem | 566 MB/s, 48.8 ms | 535 MB/s, 41.2 ms | 1.95 / 1.80 |
| point reads, 1 reader: reads/s (p50, p99) | local | 79,940 (10 µs, 30 µs) | **57,026 (20 µs, 50 µs)** | 1.97 / 1.82 |
| point reads, 16 readers | local | 360,163 (20 µs, 400 µs) | 361,801 (20 µs, 410 µs) | 1.97 / 1.82 |
| 16 readers beside one committing writer (commits in the run) | local | 316,070 (p99 470 µs; 301) | 324,136 (p99 500 µs; 1,690) | 1.97 / 1.82 |
| full read of 256 MiB | local | 568 MB/s | 577 MB/s | 1.97 / 1.82 |
| point reads, 1 reader | mem | 82,608 (10 µs, 20 µs) | **59,524 (20 µs, 40 µs)** | 1.87 / 1.80 |
| point reads, 16 readers | mem | 347,706 (20 µs, 400 µs) | 366,946 (20 µs, 390 µs) | 1.87 / 1.80 |
| 16 readers beside one committing writer | mem | 144,054 (p99 1.28 ms; 7,497) | 123,054 (p99 1.7 ms; 27,165) | 1.87 / 1.80 |
| full read of 256 MiB | mem | 645 MB/s | 631 MB/s | 1.87 / 1.80 |

What the table says:

- **Commits.** On disk a commit is the journal's append and fsync, not a
  publish: one writer 5.9× v0.2.0, and writers on their own branches share
  fsyncs (16 writers 38×, p99 10.4 s to 90 ms). In memory the per-commit
  work fell 4×.
- **Small objects.** A reader that says its length, or one hinted with
  `stream.WithLen`, writes 18× to 36× as fast; a reader that does not say
  its length and is not hinted gains only the per-chunk work (1.6× to 1.9×).
- **Reads beside a journaled writer.** The background publish costs
  readers nothing measurable on disk (16 readers 316k to 324k reads/s while
  the writer made 5.6× the commits). In memory the readers share the CPU
  with a writer that commits 3.6× as often.
- **Open: one reader's point reads are 28–29% slower.** Isolated to the
  D17 commit (a tree's nodes raw): the tree just before it read 81,894
  reads/s on mem, with it 58,649 (5 s runs, load 1.6). 16 readers are
  unaffected. In the profile a point read is the node reads' SHA-256
  verification (36%) and node decoding (30%); the cause is not yet found.
  Left to the owner with D17's pack bytes.
- **Bulk** write and read are unchanged within noise.
