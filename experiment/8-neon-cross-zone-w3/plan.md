# Experiment 8: experiment 5 with a 30% cache by experiment 4's method (approved 2026-10-25)

## Context

Experiment 5 called its cache "LFC 25%": the 36 MB LFC against the whole stock table (heap and primary key) at W=4.
Experiment 4 called its cache "15%": the share of the stock heap the LFC holds after the whole primary key. On the
same footing the two were 21% and 25% of the table, or 16% and 20% of the heap, and their low-load latencies came
out the same. The user asked to redo experiment 5 with a cache of 30% computed with experiment 4's method, by
changing the dataset size, not the cache size, and to rerun only the experiments where the LFC was set.

## Decisions (user)

- Dataset: W=3 with the standard 100,000 items per warehouse. With the 36 MB LFC (4,608 pages) the LFC then holds
  the whole primary key (825 pages), about 170 catalog pages and 28.3% of the heap (12,774 pages): the closest whole
  number of warehouses to 30% (W=2 would give 46%).
- Rerun: low load (A), the default-LFC scaling sweep and its two-pageserver sweep with their minimal-client checks
  (B), branch creation (C), cold start (D). Not the no-LFC sweep.
- The coordinator's disk was freed by the user (deleting an old neon_local deployment) before the start.

## Method (as experiment 5, unchanged unless stated)

- Experiment 5's scripts, cluster and compute settings: shared_buffers 8 MB, LFC 36 MB in 8 kB pages, prewarm heap
  first (3,674 pages = LFC - key - 109 pages of slack, as in experiment 5) then the primary key; data directories on
  disk for many-compute runs; op counts capped at 1,000,000 reads / 640,000 writes per run; balanced compute
  placement; statement_timeout 600 s for the bench role (now set by the seed); purge parallelism 64.
- A new tenant seeded with W=3 on pageserver node 1 (node 2 paused while seeding); one-pageserver runs first; then
  the tenant is split into 2 shards (stripe 256 pages) with shard 1 on a5; the two-pageserver sweep at N = 1, 64, 256,
  512, 1024 for point reads, range reads and updates, as in experiment 5.
- Variants are named `lfc` and `lfc_2ps` (experiment 5: `lfc25`, `lfc25_2ps`).
- Figures compare with experiment 5 (W=4) and experiment 4 where they share a measure.

## Expected

- Low load: about 28-29% of point reads from the cache; the median stays one round trip (40.7 ms) because most
  reads still miss; the mean falls from about 34.6 to about 29-30 ms.
- Scaling: about 0.7 GetPage per point read; one pageserver's 1 GbE link (about 12,500 pages/s) then caps point
  reads near 17,000-18,000/s; two pageservers would need about 35,000/s at N = 1,024, beyond what the client node
  can drive, so their line shows flat latency rather than a knee.
- Branch creation and compute start do not depend on the cache.

## Estimated duration

About 11 h of runs (low load 1.3 h, cold start 0.9 h, branch creation 0.8 h, default-LFC sweep 4.1 h + check
0.4 h, split 0.2 h, two-pageserver sweep 2.3 h + check 0.4 h) and about 2 h of analysis.
