# Experiment 8: experiment 5 with the cache holding 28% of the heap

## Question

Experiment 5 called its 36 MB LFC "25%": its share of the whole stock table at W = 4. On experiment 4's measure
(the heap the LFC holds after the whole primary key) it held 20% of the heap, close to experiment 4's 16%, and the
two experiments' low-load latencies came out the same. This experiment reruns every part of experiment 5 that used
the LFC with the cache at 30% on experiment 4's measure, reached by making the dataset smaller (W = 3, 28%), not
the cache larger:

- **A. Low load**: latency per operation, where the time goes, cache hits against misses.
- **B. Scaling**: N = 1 to 1,024 busy branches, one pageserver and two (tenant split into 2 shards), with the
  minimal client at N = 128 to 1,024.
- **C. Branch creation** with N = 1 to 512 busy branches.
- **D. Cold start**: point-read latency on a new branch while its cache fills.

Not rerun: experiment 5's no-LFC sweep (it does not depend on the cache).

## Verdict

A larger share of the heap in the cache speeds up whatever is limited by page fetches over the WAN, by about the
ratio of pages fetched per read, and changes nothing else (`report.md`).

| | Result (experiment 5 in brackets) |
|---|---|
| A. Low load | 28.6% of point reads hit the compute (20.4%); mean point read 29.8 ms (34.6), mean range read 161 ms (184). Medians unchanged at one round trip (point 40.7 ms, range 201 ms, insert 40.9 ms, update 41.6 ms), because most reads still miss (`figures/low_load*.png`) |
| B. Scaling | One pageserver: same knee (its 1 GbE uplink), 12% higher: 18,200 point reads/s (16,300). Two pageservers: 30,900 point reads/s at a 41 ms median (29,100), at the knee because Neon's stripe hash put 61% of the uncached heap on the second pageserver. Writes unchanged (16,500 inserts/s, 7,500-7,700 updates/s) (`figures/throughput_latency.png`) |
| C. Branch creation | 85-87 ms median, compute start 5.3-5.5 s, independent of load (the same) (`figures/branch_create.png`) |
| D. Cold start | Cache full after 174 s (the same); the warm mean is 29.5 ms (34.1-34.7), so the cold period costs about 52 s of extra latency (48-52 s) (`figures/cold_start.png`) |

## Layout

| Path | Contents |
|---|---|
| `plan.md` | the approved plan |
| `report.md` | results, figures, tables, caveats, data quality |
| `REPRODUCE.md` | what differs from experiment 5's procedure, commands in order, durations |
| `config/`, `scripts/` | experiment 5's, with the variants renamed (`lfc25` -> `lfc`, `lfc25_2ps` -> `lfc_2ps`), W = 3 and `PREWARM_HEAP_BLOCKS=3674` in `scripts/cluster.env`, the bench role's statement timeout set by the seed, and the figures drawing experiment 5 as a reference |
| `logs/notes.md` | run log with times and results |
| `logs/remote_b0/` | the client node's driver logs (`run_all.log`, `driver_*.out`, the sharded probe) |
| `data/seed/` | `seed.json` (W = 3 seed), `shard_split.json` (the split and the shards' placement) |
| `data/low_load/`, `data/scaling/{lfc,lfc_2ps}/`, `data/branch_create/`, `data/cold_start/` | as in experiment 5 |
| `data/archive/<node>/` | agent archives: `samples.jsonl.gz` (1 s samples), `starts.jsonl`, `pg/*.log` |
| `data/clock_offsets.json` | agent clock offsets measured from b0 |
| `figures/` | the figures and the numbers behind each (`*.csv`, `*.md`) |
| `code/shared_code.patch` | shared-code changes this experiment ran with (identical to experiment 5's) |

File formats are experiment 5's (`../5-neon-cross-zone-scaling/README.md`).

## Shared code changes

None beyond experiment 5's: the repo ran at commit `f004d9f` plus `code/shared_code.patch` (`NEON_LOCAL_COMPUTE_OPTS`,
prewarm for background clients, round-robin compute placement, purge parallelism, and their tests), the same patch
as experiment 5's.

## Deviations from the plan

None. No run failed, nothing was rerun, and `data/superseded/` is empty.
