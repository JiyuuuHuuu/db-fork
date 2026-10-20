# Experiment 1: Dolt branch scaling

Does a branchable database that runs on one machine get slower as branches
accumulate? Three hypotheses, each measured as single-client latency and as
throughput under concurrent load, against the number of existing branches N
(1 to 1,024), on Dolt 2.4.2 (`dolt_mysql`, MySQL protocol) and Doltgres 1.4.0
(`dolt`, Postgres protocol). Run on CloudLab, 15-16 Oct 2026.

- Report with figures: [report.md](report.md) (corrected 18 Oct 2026: E3's per-branch write rates were capped by
  the client, see the report's "Correction")
- Plan as approved: [plan.md](plan.md)
- Step-by-step rerun instructions: [REPRODUCE.md](REPRODUCE.md)
- Machines: `.agent/cloudlab.md` at the repo root

## Result

| Hypothesis | Verdict |
|---|---|
| H1 branch creation is slow | No for one client (2-4.5 ms, +26-43% from 1 to 1,024 branches). Partly under concurrency: creates are serialized, and Dolt's concurrent create rate halves as branches grow. |
| H2 data ops slow down with N | No for reads (within 2%). Writes +6-11% at 1,024 branches, also without divergence. |
| H3 branches interfere | No for idle branches and for reads (Doltgres -10-15% when reads span branches). Yes for writes: one branch serializes its writers, and all branches share a server-wide ceiling of about 350-370 single-row updates/s, reached at about 8 busy branches. |

Concurrent read throughput from the benchmark runner is limited by its single
Python process, and its per-branch write rates at low N by an untimed per-write
metadata query (fixed for experiment 2). The conclusions about the servers come
from the multi-process calibration (`data/*/calibration`, `figures/client_calibration.png`).
Experiment 2 repeats H2 and H3 with every branch busy.

## Layout

```
README.md            this file
plan.md              the plan approved before the runs
report.md            the report; its images are figures/*.png
figures/             plots and summary.csv / summary.md (every plotted point)
data/dolt_mysql/     Dolt results
data/dolt/           Doltgres results
data/superseded/     smoke tests and the first attempt (not analysed, see below)
logs/                node provisioning, study driver output, calibration logs
scripts/             everything specific to this experiment (see below)
```

Each `data/<backend>/` holds one directory per phase, written by the repo's
`microbench/runner2.py` through `scripts/run_single_thread_bench.sh` and
`scripts/run_throughput_bench.sh`:

| Directory | Phase |
|---|---|
| `e1_latency/{fan_out,spine}` | E1-L: 10 BRANCH_CREATE after N setup branches, 1 client, N = 1, 2, 4, ..., 1024 |
| `e1_throughput` | E1-T: 16 threads x 50 BRANCH_CREATE after N, N = 1, 4, 16, 64, 256, 1024 |
| `e2_latency/{fan_out,spine}` | E2-L: READ, RANGE_READ, INSERT, UPDATE, RANGE_UPDATE, 1 client, N = 1, 4, 16, 64, 256, 1024 (spine: 1, 64, 1024) |
| `e2_throughput` | E2-T: 16 threads x 4 in flight spread over N branches, N = 1, 2, 4, 8, 16, 64, 256, 1024 |
| `e3_proportional` | E3: 1 thread x 4 in flight per branch, N = 1, 2, 4, 8, 16, 32 |
| `ctrl_latency`, `ctrl_throughput` | E2-L and E2-T with no divergence (`--updates-per-branch 0`), N = 1, 64, 1024 |
| `calibration` | `calib_<b>.txt` reads, `calib_write_<b>.txt` writes, from `scripts/calibrate_dolt_clients.sh` |
| `logs`, `progress.log` | per-run runner output and the phase timeline |

Files in a phase directory, per run id (`<backend>_chw1_<N>_<shape>` or
`<backend>_chw1_tp_t<threads>_b<N>[_cr<inflight>]`):

- `<run_id>.parquet`: one row per timed operation, all operations of that run id appended.
  `branch_count` is the number of branches that existed when the row was taken;
  `op_name` READ/UPDATE rows with `>=` in `sql_query` are the RANGE_ variants.
- `<run_id>_setup.parquet`: the timed BRANCH and CONNECT rows of the N-branch setup.
- `<run_id>_<OP>_threads<T>_summary.json`: throughput, failed ops, `latency` quantiles
  (seconds), `loaded_branches`, `data_source` (seeding time and row counts).

Setup for every run: CH-benCHmark at one warehouse (about 608k rows), foreign
keys skipped, copied from the seed database `ch_w1_seed` on the server; benchmark
table `stock`; each setup branch gets 100 untimed updates (uncommitted, so a spine
child does not inherit its parent's changes).

`data/superseded/aborted_<b>/` holds the smoke tests (`smoke/`) and the first
start of the study, stopped after E1-L fan_out because the setup was too slow
(about 2.5-5.6 s per branch). The setup was then made about 5-9 times faster and
the study restarted from scratch. A range-query bug was also fixed before any
range operation ran. Its data is consistent with the final run but was not
used in the analysis.

## Scripts

| File | Purpose |
|---|---|
| `scripts/setup_cloudlab_dolt.sh` | Provision a CloudLab node: SSD, Dolt or Doltgres server pinned to NUMA node 0, repo copy and venv |
| `scripts/run_dolt_scaling.sh` | Run all phases on the node (`--smoke` for a quick check, `--phases` to choose) |
| `scripts/calibrate_dolt_clients.sh` | Client calibration: threads vs processes, same vs distinct branches, reads and writes |
| `scripts/driver_ceiling.py` | The calibration client |
| `scripts/plot_branch_scaling.py` | All figures and `summary.csv/md` from `data/` |
| `scripts/range_update_probe.py` | Doltgres RANGE_UPDATE check: row-value range vs BETWEEN |
| `scripts/probe_copy.py` | Check that a server can copy a table from another database's branch |

Reproduce (from the repo root, `X=experiment/1-dolt-branch-scaling`):

```bash
$X/scripts/setup_cloudlab_dolt.sh dolt_mysql <host>
ssh <host> "cd db-fork && $X/scripts/run_dolt_scaling.sh dolt_mysql --seed-db ch_w1_seed"
ssh <host> "cd db-fork && $X/scripts/calibrate_dolt_clients.sh dolt_mysql"
rsync -az <host>:db-fork/$X/data/dolt_mysql/ $X/data/dolt_mysql/
uv run python $X/scripts/plot_branch_scaling.py --run-dir $X/data/dolt_mysql --run-dir $X/data/dolt --outdir $X/figures
```

## Repo changes this experiment depends on

These live in the shared benchmark code, not in this directory:

- `util/db_helpers.py`: composite primary-key columns were returned in reverse
  order, which made every range query a full index scan.
- `microbench/runner2.py`, `microbench/task2.proto`: generated CH-benCHmark data
  source (optionally copied from a seed database), `branch_count` on every row,
  load only on setup branches, latency quantiles in the summary, cached column
  lookups in the setup helpers.
- `dblib/result_collector.py`: async rows keep the thread's context (table, seed, branch count).
- `microbench/operations/branch.py`: BRANCH_CREATE records the live branch count.
- `scripts/run_single_thread_bench.sh`, `scripts/run_throughput_bench.sh`:
  `--generated-scale`, `--seed-db`, `--*-per-branch`, `--branch-create-ops`;
  BRANCH_CREATE runs after the N-branch setup.
- Tests: `tests/test_micro_runner2.py`.
