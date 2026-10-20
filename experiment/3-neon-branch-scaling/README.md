# Experiment 3: Neon branch scaling with every branch busy

Experiment 2's workload (data operations and branch creation with N = 1 to 1,024 existing branches, every branch
busy) run on a self-hosted Neon cluster and compared with experiment 2's Dolt 2.4.2 and Doltgres 1.4.0 numbers.
CloudLab c220g2, 19 Oct 2026.

**Question.** The user's hypothesis: Neon's branch and data operations are slower than Dolt's at low load because
Neon is distributed, but Neon scales further, until the safekeepers and the pageserver become the bottleneck as the
load grows.

**Verdict: largely confirmed** ([report.md](report.md)). At one busy branch Neon is slower than Dolt for reads
(2.5 against 1.3 ms) and branch creation (6.5 against 2.7 ms), about even for updates, and faster for inserts (1.8
against 3.2 ms). Neon scales much further: 15,000-20,000 inserts/s and 6,000-9,600 updates/s against Dolt's ~680
writes/s, and branch creation stays at 6-7 ms up to 1,024 branches where Dolt's reaches 2.2 s. Neon levels off at
about 64 busy branches, on the pageserver's SSD reads (point reads ~31,000/s, updates) and on the safekeepers' SSD
writes (inserts), not on CPU or the computes.

- Report: [report.md](report.md)
- Plan: [plan.md](plan.md)
- Step-by-step rerun instructions: [REPRODUCE.md](REPRODUCE.md)
- Exact shared-code state used: [code/shared_code.patch](code/shared_code.patch) (against commit `24ab525`)
- Machines: `.agent/cloudlab.md` ("Experiment 3 layout")

## Layout

```
README.md, plan.md, report.md, REPRODUCE.md
code/            shared_code.patch: shared-code changes of experiments 2 and 3, against 24ab525
config/          compute spec template (rendered per compute by the agent), compute-hook stub
figures/         setup.png (setup diagram), data_ops.png, throughput_latency.png, branch_create.png, low_load.png, neon_bottleneck.png,
                 summary.csv / summary.md, activity.csv, internal_latency.csv
data/neon_local/ Neon results: data_ops/, branch_create/, metrics/ (pageserver and safekeeper /metrics before and
                 after every run), logs/, progress.log, seed.json, ceiling_check.csv (minimal-client check),
                 compute_start_probe.jsonl + compute_start_probe_last_pooled.log (compute start breakdown)
data/neon_local_nolimit/ branch creation at every N rerun with the storage controller's tenant rate limit lifted
                 (--tenant-rate-limit 1000); the "Neon" branch-creation series in the figures
data/neon_local_reruns/  three runs repeated after a failure (they replace the same points in the figures):
                 READ N = 32 and UPDATE N = 64 (a pageserver read error stopped one client for the whole run),
                 branch creation N = 1,024 (computes could not start: shared-memory ids exhausted)
data/superseded/ not analysed, see below
logs/            cluster bring-up, seed and driver output
scripts/         cluster.env (node roles), setup_neon_cluster.sh (deploy), neon_node_agent.py (per-node agent),
                 neon_ctl.py (env, seed, purge, status, metrics, smoke), sync_client.sh,
                 run_neon_busy_branches.sh (the study), summarize.py (one line per run),
                 plot_neon_vs_dolt.py (figures), plot_setup_diagram.py (setup.png),
                 ceiling_check.py (minimal-client check),
                 compute_start_probe.py (cold vs pooled compute start, run on a compute node),
                 branch_activity.py (requests in flight per run: figures/activity.csv),
                 internal_latency.py + ps_metrics_delta.py (pageserver/safekeeper metrics per run:
                 figures/internal_latency.csv)
```

Each run directory holds, per run id (`neon_local_chw1_tp_t<clients>_b<N>`): `<run_id>.parquet` (one row per timed
operation; in `branch_create/`, the creator is `thread_id` 0 and the background clients are 1..N),
`<run_id>_setup.parquet` (the timed branch creates and compute starts of the setup), and
`<run_id>_<OP>_threads<T>_summary.json` (throughput, latency quantiles in seconds, client CPU, and `remote_cpu`:
per machine the average CPU cores of the Neon process and of the whole node, and SSD reads/writes per second and
busy fraction, over the timed window).

`data/superseded/`:

| Directory | Why it is not analysed |
|---|---|
| `smoke_neon_local` | smoke test (N = 1, 4, 64; 10% of the ops). Its N = 64 UPDATE window includes each client process loading its key cache, because 10% warm-up of 9 ops rounds to none |
| `aborted_seqscan_range_read` | first full sweep, stopped during RANGE_READ at N = 1: Postgres planned the benchmark's row-comparison range predicate as a sequential scan (4.7 s per range read) |
| `check_range_read_rowcompare_index` | range read with the per-column bounds added to the row comparison: index scan, but ~200 page reads and 300-450 ms per range read |
| `check_range_read_percolumn` | check of the final range predicate at N = 1 (9.4 ms median) |
| `no_image_layers` | full sweep up to N = 64 (stopped at N = 128) on a seed timeline that had only delta layers: the bulk load never triggered image-layer creation, so every GetPage replayed the page's whole load history through WAL-redo (about 131 records per page) and point reads capped at about 5,000/s |
| `check_images_below_branch_point` | point reads after forcing image layers on the seed: no change, because the images sit one LSN above the seed's last record, where every branch starts |
| `short_windows` | data operations at N = 8 to 256 with experiment 2's op counts: from N = 16 up each window lasted 0.4-3 s, of which up to 0.6 s was the client processes starting after the barrier, so only a fraction of the N branches had a request in flight (34 of 256 for INSERT). Rerun with per-client minimums. Also holds a READ run at N = 16 in which one branch hit a persistent pageserver read error (5,000 failed reads) |
| `reruns_shm_exhausted` | first attempt at the three reruns: no compute could start (SysV shared-memory ids exhausted by computes killed earlier) |
| `check_images_read` | point reads at N = 1 and 64 once the seed's WAL was advanced past the images (2.5 ms; 27,900 reads/s at N = 64): the configuration of the final sweep |

## Shared code this experiment added

- `dblib/neon_local.py`: the `neon_local` backend (self-hosted Neon: timelines through the storage controller,
  one compute per branch started on first connection through the compute-node agents).
- `microbench/runner2.py`: `NEON_LOCAL` in setup, cleanup, seeding and tool creation; `setup_parallelism` (FAN_OUT
  setup branches created and loaded by several workers at once); `RemoteCpuSampler` (`DBFORK_REMOTE_CPU`: CPU and
  SSD counters of remote machines over the timed window); range reads use per-column key bounds instead of the row
  comparison when they select the same rows.
- `microbench/operations/branch.py`: `branch_op_config.connect_every` (connect to every k-th created branch).
- `microbench/task2.proto`, `macrobench/task.proto`: `NEON_LOCAL = 13`, `SetupConfig.setup_parallelism`,
  `BranchOpConfig.connect_every`.
- `scripts/run_throughput_bench.sh`: backend `neon_local`, `--seed-db` for it, `--setup-parallelism`,
  `--branch-create-connect-every`.
- Tests in `tests/test_micro_runner2.py`: connect_every, parallel setup, remote CPU interpolation, range predicate.
