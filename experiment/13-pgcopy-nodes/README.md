# Experiment 13: shared-nothing branching by copying Postgres data directories to other nodes (`pgcopy`)

## Question

What does branching cost when it is just "copy the database files to another node and start Postgres there", and
how do experiment 12's four workloads (low contention, scaling with data operations + ceiling check, branch creation
under load, cold start) behave on that design? It is the naive floor under the systems compared so far (Doltgres and
Neon local on 10 cores, Neon across zones over TCP and RDMA in experiment 12).

Verdict and numbers: [report.md](report.md). Plan (approved 5 Nov 2026, with the cluster review): [plan.md](plan.md).

## Design (backend `pgcopy`)

- A **branch** is a new PostgreSQL 17.11 instance on some node whose data directory is a `pg_basebackup` of the
  parent's live instance over the LAN (`-Fp -X stream --checkpoint=fast --no-sync --no-manifest`, with a temporary
  replication slot; see "Changes during the run"), then
  started. `branch()` returns when the instance answers `select 1` (copy + start; the agent records both phases).
- **Run root**: every run copies a run root ("main") from the seed instance onto the seed node; every branch of the
  run is copied from that root, so every branch is a network copy from the seed node.
- **Clients co-located** with their branches: a runner on host node i places every branch it creates on node i and
  connects over 127.0.0.1; only the fork crosses the network.
- Per instance: `shared_buffers = 40MB` (about Neon's 36 MB LFC + its 8 MB shared_buffers), `max_connections = 20`,
  `max_wal_senders = 8`; parents (seed, run roots) `max_wal_senders = 96`, `max_replication_slots = 96`,
  `max_connections = 160`, `wal_keep_size = 512MB`. Every node's agent trims `/mydata` after each purge (before
  each point). PostgreSQL defaults otherwise (fsync on, synchronous_commit on, autovacuum on).
- The same untimed prewarm as Neon's computes (3,674 stock heap pages, then `stock_pkey`), here into the 40 MB of
  shared_buffers. The OS page cache is shared by a node's instances and is warm right after a copy.
- Dataset: CH-benCHmark W = 3 (`bench` database 283 MiB, 300,000 stock rows), the seed loaded once.

## Cluster and placement

16 xl170 nodes at CloudLab Utah on one 25 GbE LAN (10.10.1.0/24), re-instantiated 5 Nov 2026, see
`.agent/cloudlab.md`. Network and disk probe of 5 Nov: [logs/network_probe_2026-11-05/](logs/network_probe_2026-11-05/)
(ping 0.07-0.15 ms, 13 Gbit/s per TCP stream, 23.9 Gbit/s from one node to all others; `/mydata` writes ~350 MB/s).

| Node | Role |
|---|---|
| n0 (hp024) | seed node: seed instance (`seed`), every run root; serves every copy |
| n1 (hp119) | host + the drivers (`run_all.sh`, `multi_scaling.py`), the GO coordinator for multi-node points |
| n2-n15 | hosts: branch instances with their co-located clients |
| pc20.cloudlab.umass.edu | coordinator outside the cluster (51 ms away, public addresses only): setup over ssh, launches, pulls |

Agents (`pgcopy-agent`, `dbfork-cpu-agent`) and every instance listen only on the node's LAN address (and 127.0.0.1
for instances): the nodes' public interfaces face the internet. Nodes reach each other by ssh over the LAN with a
cluster-only key (`~/.ssh/cluster_ed25519`, made on the coordinator by `pgcopy.py install`).

Multi-node points (scaling_data, its ceiling check, scaling_branch): N busy branches split over the 15 hosts as evenly
as possible (N < 15 uses N hosts), every node's timed window started together (`microbench/fast_client` cross-node
GO). The client side therefore runs with `BENCH_FAST_CLIENT=1` (experiment 12's client-side patches: Faker value
pool, comment-strip fast path, set-based key tracking; none inside the latency timer). scaling_branch: one creator
(n1) and n2-n15 keep their share of the busy branches running MIXED until n1's creates are done (fast_client hold
patch). A single-node scaling_branch series (`--single-ns 1,4,16,64,128` on n1, results under `data/run/single/`)
shows the contended case.

## Shared code added or changed (committed with this experiment; `code/shared_code.patch` is the same change against `code/base_commit.txt`, as it ran)

- New: `dblib/pgcopy.py` (backend), `scripts/setup/pgcopy_node_agent.py` (node agent), `scripts/setup/pgcopy.py`
  (deployment from the coordinator), `scripts/setup/pgcopy_ctl.py` (seed, purge, smoke, status, clock check on a
  node), `scripts/setup/topologies/cloudlab_pgcopy.toml`, `tests/test_pgcopy.py`.
- `microbench/task2.proto`, `macrobench/task.proto`: `PGCOPY = 14`; `microbench/runner2.py`, `macrobench/runner.py`,
  `scripts/run_throughput_bench.sh`: the backend wired in like `NEON_LOCAL` (run root from the seed, `--seed-db`).
- `scripts/setup/topo.py`: `[pgcopy]` section (seed, hosts, client), LAN-only agent URLs, per-node views.
  `scripts/setup/nodes.py`: CPU agent bind address, group `copy` (`pg_basebackup`).
- `scripts/workloads/`: `--backend pgcopy` in common.sh, the three drivers, `cold_start.py`, `ceiling_check.py`
  (pgcopy arm and a synchronised multi-node mode); `run_all.sh --multi`;
  `multi_client/multi_scaling.py` (`--backend`, `run-branch`, `run-ceiling`) and `node_run.sh` (`--mode branch`).
- `microbench/fast_client/patches.py`: `patch_pgcopy` (no cluster-wide purge in multi-client mode),
  `patch_branch_ops` (the hold of background-only nodes).

## Changes during the run, and what was superseded (5 Nov 2026, times UTC)

Each change is in the shared code (and in `code/shared_code.patch`), and every timed number in the report comes from
runs made after the change that concerns it.

1. **Replication slots for copies (10:15).** The first multi-node ceiling check failed on every node: copies used
   `--no-slot`, and with 16 copies of one run root at once each copy's end forced a WAL segment switch, so the
   parent removed WAL a slow copy still needed (`requested WAL segment ... has already been removed`). Copies now
   hold a temporary replication slot (`pg_basebackup` without `--no-slot`; parents `max_replication_slots = 96`,
   the agent rewrites the seed's settings when it adopts it). The low-contention creates were rerun with it (p50
   1,576 ms against 1,582 ms before; the no-slot run is in `data/run/pgcopy/low_contention/superseded/`).
   scaling_data's points copy only in their untimed setup and were kept.
2. **Background rows sampled 1/20 (11:35).** The runner holds every result row in memory; in scaling_branch the
   background clients run fast MIXED operations for minutes, and at N = 256 the runner processes on n4 reached
   ~5 GB each and the node swapped (3 GB of 62 available). `microbench/fast_client/patches.py`
   (`patch_background_sample`) makes background clients record one operation in 20 (per-thread recording: the
   timed creator records everything); background throughput is the sampled count x 20, its latencies a 1/20
   sample. On in multi-node mode and for pgcopy's single-node scaling_branch (`scaling_branch.sh`).
3. **SSD trim before every point (12:10).** `/mydata` is mounted without `discard` and was never trimmed: after a
   few hundred GB of copies and deletes per node, n1's idle fork start grew from 1.06 s (09:00-10:40) to 3.1-3.4 s
   (from 11:20), while a sequential fio write was still ~315-330 MB/s. `fstrim` on all 16 nodes (each ~350 GiB)
   restored it (three idle forks on n1: start 1,018-1,083 ms). The agent now trims after every purge, i.e. before
   every point (`--fstrim`, log `archive/<node>/trims.jsonl`). The whole scaling_branch step (multi-node and single)
   was rerun from 12:11; the untrimmed attempt is in `data/run/superseded_untrimmed_ssd/` (its creates were 7-110%
   slower, e.g. N = 512: 4.98 s against 2.37 s). scaling_data (09:27-10:10) and the ceiling (10:19-10:39) ran before
   the slowdown: n1's idle forks at 10:16-10:19 started in 1.08 s.

Also superseded: the smoke attempt whose multi-node branch point waited for two GO connections per node instead of
one (`data/smoke/pgcopy/scaling_branch.hung_go_count/`), and the failed ceiling attempt
(`data/run/pgcopy/scaling_data/superseded/ceiling_wal_removed/`).

Not done from the plan: `scripts/plotting/*` colour maps were not given `pgcopy` (they have no `neon_local` either;
this experiment's figures come from its own `plot.py`). The palette validator of the dataviz guidance needs `node`,
which the coordinator lacks; the figures use the reference palette's slots 1-4 in fixed order unchanged.

## Layout

```
README.md, plan.md, REPRODUCE.md, report.md
figures/  low_load, scaling, ceiling, branch_create, cold_start, fork_phases (.png + the aggregated .csv table)
results/  aggregated data, in git (scripts/collect_results.py from data/): every runner summary (*_summary.json),
          the ceiling CSVs, cold_start/runs.json, under run/ and smoke/ as in data/; archive/agents.csv (copies,
          GiB, failures, trims per node)
code/     base_commit.txt, shared_code.patch
scripts/  launch.sh (detached unit on a node), chain.sh (setup | smoke | full | pull), pull.sh, plot.py,
          fork_phases.py, collect_results.py
logs/     the network probe (summary; raw/ not in git), clock offsets, status snapshots; install, start, seed,
          deploy, smoke logs (*.log, not in git)
data ->   /mydata/exp13 on the coordinator, raw, not in git (62 GB): run/ (full run), smoke/ (smoke ladder),
          archive/<node>/ (agents' forks/failures/trims.jsonl, kept statement logs), nodes/<node>/ (each host's own
          copy of its results), fork_phases.csv (one row per fork)
```

What is in git: everything but the raw data and the execution logs. Experiment 12 kept only its Markdown and PNGs
and lost its numbers with the cluster; here `results/` and the figure tables keep every aggregated number (each
summary has the point's count, throughput, latency percentiles, backend observations and CPU samples), while the
per-operation parquet files, the per-event agent records and the run logs stay in `data/` and `logs/*.log`. Without
`data/`, `plot.py` cannot redraw the create-latency and cold-start panels (they read the parquet rows); their
values are in `figures/*.csv`.

Result files follow the shared drivers' layout (`run/pgcopy/<workload>/...`), as experiment 12's, so a later joint
plot with the Neon and Doltgres series reads both.
