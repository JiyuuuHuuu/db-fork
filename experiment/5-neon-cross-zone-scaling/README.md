# Experiment 5: Neon across two zones with a compute cache: low load, scaling, branch creation, cold start

## Question

Experiment 4's placement (Neon's storage layer in Wisconsin, client and computes in Utah, 38.9 ms round trip) with
a smaller dataset whose compute cache holds a quarter of the working table, which becomes the default for later
experiments. Four sub-experiments:

- **A. Low load.** One busy branch, one client: median latency per operation, where the time goes, and the latency
  of cache hits against cache misses.
- **B. Scaling with data operations.** N busy branches (1 to 1,024), one client per branch with one request in
  flight: throughput against median latency, in three variants: no LFC; the default LFC; the default LFC with the
  tenant split over two pageservers.
- **C. Branch creation** with N busy branches (1 to 512), default LFC: latency only.
- **D. Cold start.** Point-read latency over time on a newly created branch while its compute's caches fill.

## Verdict

The report was regenerated on 25 Oct 2026 with experiment 8's cache runs (W=3: the 36 MB LFC holds 28% of the
stock heap) in place of this experiment's W=4 cache runs (20% of the heap); the no-LFC sweep is this experiment's
own. Across two zones every Neon operation pays whole WAN round trips (38.9 ms); the storage layer's own work stays
below a millisecond (`report.md`; the first version is in `superseded/report_lfc_w4/`).

| | Result |
|---|---|
| A. Low load | Mean point read 29.8 ms, range read 161 ms, insert 41.0 ms, update 55.7 ms, branch create 85 ms, compute start 5.4 s; medians are whole round trips (point read 40.7 ms). 28.6% of point reads hit the compute's cache (0.89 ms); misses take 40.7 ms (`figures/low_load*.png`) |
| B. Scaling | One pageserver's 1 GbE WAN link caps reads at about 12,500-13,000 pages/s: 7,400 point reads/s without the LFC (W=4), 18,200/s with it. Two pageservers (tenant split into 2 shards): 30,900 point reads/s at N = 1,024 with the median still at one round trip, where the busier shard's pageserver reaches its link. Writes are limited by the safekeepers, not the pageserver: 16,300-16,500 inserts/s, 7,200-7,700 updates/s (`figures/throughput_latency.png`) |
| C. Branch creation | 85-87 ms median from 1 to 512 busy branches; compute start 5.3-5.5 s (`figures/branch_create.png`) |
| D. Cold start | Usable after 5.3 s; reads fall from about 86 ms (index and heap fetched) to the warm 29.5 ms mean after 150-156 s, about 52 s of extra latency over the first 3,100 reads (`figures/cold_start.png`) |

## Layout

| Path | Contents |
|---|---|
| `plan.md` | the approved plan |
| `report.md` | results, figures, tables, caveats |
| `REPRODUCE.md` | machines, versions, parameters, commands in order, checks, pitfalls |
| `config/` | compute config template (8 MB shared_buffers, LFC placeholders, sparse WAL settings, 25 connections), compute-hook stub |
| `scripts/cluster.env` | node roles, addresses, paths and the default parameters (W=4, SB 8 MB, LFC 36 MB) |
| `scripts/setup_neon_cluster.sh` | deploys the cluster from the coordinator (two pageservers, seven compute nodes) |
| `scripts/setup_remote_storage_nfs.sh` | shares the pageservers' remote storage (a2's directory, NFS over the LAN) |
| `scripts/neon_node_agent.py` | per-node agent: computes (per-compute LFC size, pageserver shards, statement log, data directory on disk), samples |
| `scripts/neon_ctl.py` | env files, seed, shard split, locate, purge, metrics, clock check, smoke test |
| `scripts/common.sh`, `run_low_load.sh`, `run_scaling.sh`, `run_branch_create.sh`, `cold_start.py`, `ceiling_check.py`, `run_all.sh`, `launch.sh` | the measured runs (on b0) |
| `scripts/probe_wan.sh`, `probe_memory.py` | probes: pageserver uplink to Utah, memory per compute |
| `scripts/pull_data.sh`, `sync_client.sh` | copy results back, copy the repo out |
| `scripts/breakdown.py`, `load_runs.py`, `resource_use.py` | analysis |
| `scripts/lfc_contents_probe.py` | probe: what the LFC holds after N reads, and the prewarm order's effect |
| `scripts/recompress.py` | rewrites large result files with zstd (lossless; the coordinator's disk was full) |
| `logs/notes.md` | incidents during the runs and what was changed, with times |
| `scripts/plot_low_load.py`, `plot_scaling.py`, `plot_branch_create.py`, `plot_cold_start.py` | figures of the first report (W=4 cache runs) |
| `scripts/plot_with_exp8.py` | the current figures: this experiment's no-LFC sweep plus experiment 8's cache runs, drawn with experiment 8's plot scripts |
| `superseded/report_lfc_w4/` | the first report and its figures (W=4 cache runs, compared with experiment 4) |
| `data/seed/` | `seed.json` (W=4 seed), `shard_split.json` (the split and the shards' placement) |
| `data/low_load/` | A: `data_ops/<OP>/`, `branch_create/`, `metrics/`, `logs/` |
| `data/scaling/<variant>/` | B: `data_ops/<OP>/` (one parquet and summary per N), `ceiling_check.csv`, `metrics/`, `logs/` |
| `data/branch_create/` | C: one parquet and summary per N |
| `data/cold_start/` | D: `run<k>.parquet` (one row per read), `runs.json` |
| `data/archive/<node>/` | agent archives: `samples.jsonl.gz` (1 s samples, gzipped), `starts.jsonl`, `pg/*.log` |
| `data/clock_offsets.json` | agent clock offsets measured from b0 |
| `data/superseded/` | smoke runs and aborted attempts |
| `logs/` | setup, seed, probes, driver logs |
| `code/shared_code.patch` | shared-code changes this experiment ran with |

## File formats

- `*.parquet` (runner): one row per operation (`dblib/result.proto`); statement rows carry `latency`, `start_time`,
  `end_time` in seconds on b0's clock; range reads are statement rows named READ. Files above 20 MB are zstd.
- `data/cold_start/run<k>.parquet`: one row per read: `i`, `t_start`, `t_end`, `latency_s`, `w`, `i_id`.
- `*_summary.json` (runner): throughput over the timed window, latency quantiles, client and remote CPU.
- `ceiling_check.csv`: minimal client, one row per kind and N: ops/s, p50/p95, client CPU, remote CPU and SSD rates.
- `samples.jsonl.gz`: one JSON object per second per node: `t` (agent clock), `node` (memory, compute tmpfs, NIC
  byte counters), `computes` (up to 8 computes per tick: `pc:`, `lfc:`, `bp:` counters), `services` (pageserver,
  safekeeper, storage-controller metrics).
- `figures/*.csv|.md`: the numbers behind each figure; `figures/resource_use.csv` per scaling run.

## Deviations from the plan

- **Two pageservers by shard split, not by branch.** Chosen with the user before execution (plan.md).
- **Compute data directories on disk for the many-compute runs.** At 102 MB per compute (tmpfs) N = 1,024 did not
  fit; with the data directory on the node's disk and max_connections 25 a compute takes 83 MB.
- **Balanced compute placement** (shared change 3) after the hash placement left one node 40% above average.
- **Operation counts capped from N = 512 on** (1,000,000 reads, 640,000 writes per run) after the client node ran
  out of memory at N = 1,024.
- **Prewarm order: heap first, key last.** The first default-LFC sweep (key first) was discarded and rerun.
- **Shorter two-pageserver sweep** (user's choice, to save run time): N = 1, 64, 256, 512, 1,024; no inserts.
- **Minimal client also at N = 128**, because the benchmark client is write-limited from N = 256.
- **Statement timeout 600 s** for the bench role from the scaling runs on (none hit).
- The shard split's smoke test was replaced by a sharded probe (`logs/lfc_probe_2ps_smoke.json`).

## Shared code changes

1. `dblib/neon_local.py`: `NEON_LOCAL_COMPUTE_OPTS` (JSON) is merged into every compute-start request to the node
   agent (LFC size, pageserver shards, statement logging, data directory on disk). Unset, nothing changes.
2. `microbench/runner2.py`: `DBFORK_PREWARM_SQL` also runs for background clients (moved into
   `OperationRunner.run_prewarm`), so the busy branches of the branch-creation runs start with filled caches.
3. `dblib/neon_local.py`: numbered branches (`..._<i>`) get timeline ids that `agent_for` maps to agent i mod
   (number of agents), so N branches' computes spread evenly over the compute nodes; one agent: unchanged.
4. `dblib/neon_local.py`: `NEON_LOCAL_PURGE_PARALLEL` (default 16) for purge and teardown.
5. Tests: `tests/test_neon_local.py` (new) and a case in `tests/test_micro_runner2.py`.

The repo ran at commit `f004d9f` plus `code/shared_code.patch` (`git apply --check` passes on f004d9f).

## Superseded data (`data/superseded/`)

- Since the regeneration (25 Oct 2026) the W=4 cache runs (`data/scaling/lfc25`, `data/scaling/lfc25_2ps`,
  `data/low_load`, `data/branch_create`, `data/cold_start`) are not used by `report.md`; they stay in place and
  back `superseded/report_lfc_w4/`. The current cache results are in `../8-neon-cross-zone-w3/data/`.

- `smoke_low_load/`, `smoke_scaling_nolfc/`, `smoke_scaling_lfc25/`, `smoke_branch_create/`: smoke runs of the
  drivers (tenth of the operations).
- `nolfc_interrupted_2026-10-23/`: the no-LFC INSERT N = 256 run whose client processes failed (logind removed the
  runner's semaphores on b0) and the UPDATE N = 256 run stopped to fix it; both rerun.
- `nolfc_client_oom_2026-10-23/`: the no-LFC INSERT N = 1,024 run that made the client node swap for an hour
  (stopped; rerun with the capped counts).
- `lfc25_prewarm_key_first/`: the first default-LFC sweep, prewarmed key first, which let index leaves be evicted
  (16% of point reads took two round trips); rerun with the heap first.
