# Experiment 2: Dolt with every branch busy

Data operations and branch creation with N existing branches (N = 1 to 1,024), where every existing branch is
running a workload. Dolt 2.4.2 (`dolt_mysql`, MySQL protocol) and Doltgres 1.4.0 (`dolt`, Postgres protocol) on
CloudLab, 18 Oct 2026. Follow-up to `experiment/1-dolt-branch-scaling`, whose data-op runs kept the other branches
idle.

- Report: [report.md](report.md)
- Plan: [plan.md](plan.md)
- Step-by-step rerun instructions: [REPRODUCE.md](REPRODUCE.md)
- Exact shared-code state used: [code/shared_code.patch](code/shared_code.patch) (against commit `24ab525`)
- Machines and server setup: `.agent/cloudlab.md` and experiment 1's `scripts/setup_cloudlab_dolt.sh`

## Layout

```
README.md, plan.md, report.md, REPRODUCE.md
code/               shared_code.patch: the shared-code changes this experiment ran with, against 24ab525
figures/            data_ops.png, branch_create.png, summary.csv / summary.md (every plotted point)
data/dolt_mysql/    Dolt results
data/dolt/          Doltgres results
data/superseded/    not analysed: smoke tests (smoke_*), the first Dolt branch-creation run at N = 1 (12 ms right
                    after a server restart; replaced by the rerun in recheck_dolt_mysql_n1, which is the plotted point)
logs/               driver output
scripts/            run_busy_branches.sh (the study), plot_busy_branches.py (the figures),
                    read_ceiling_check.py (client-vs-server read check),
                    branch_activity.py (how many branches were really busy: figures/activity.csv),
                    fsync_probe.sh (server fsync calls per write and SSD flush latency: data/fsync_probe.md)
```

Each `data/<backend>/` has:

| Directory | Runs |
|---|---|
| `data_ops/` | N branches, one client per branch running READ, RANGE_READ (100 rows; the throughput script fixes the range at 100), INSERT or UPDATE back to back |
| `branch_create/` | one client creates 100 branches while one client per existing branch runs 80% point reads / 20% point updates |
| `read_check.csv` | the minimal-client read check (`scripts/read_ceiling_check.py`): point and range reads at 16-1,024 busy branches |
| `logs/`, `progress.log` | per-run output and the timeline |

Per run id (`<backend>_chw1_tp_t<clients>_b<N>`): `<run_id>.parquet` holds one row per timed operation (in
`branch_create/`, the creator is `thread_id` 0 and the background clients are 1..N); `<run_id>_setup.parquet`
holds the timed branch creations of the setup; `<run_id>_<OP>_threads<T>_summary.json` holds throughput, latency
quantiles (seconds), server and client CPU, and for branch creation the background clients' throughput.

## Differences from experiment 1

| | Experiment 1 | Experiment 2 |
|---|---|---|
| Other branches during data ops | idle (E2-L: one client on one of N branches) | all busy: one client per branch |
| Other branches during branch creation | idle (E1-L, 1 creator; E1-T, 16 creators) | all busy with 80% reads / 20% updates; 1 creator |
| Client | one Python process (threads; async with 4 in flight for E2-T/E3) | up to 20 processes, sync, 1 request in flight per client |
| Per-write metadata query | yes (information_schema before every write, untimed) | no (looked up once per process) |
| RANGE_READ size | 200 rows (E2-L) / 100 rows (E2-T, E3) | 100 rows |
| Creates per point | 10 (E1-L), 800 (E1-T) | 100, starting 1 s after the background load is running |
| N values | powers of 2 (E1-L), 1-4-16-64-256-1024 (E2-L) | powers of 2 |
| Unchanged | nodes, server versions and configs (except Dolt max_connections 1000 → 2048), data, table, 100-update divergence per branch, fan_out shape, server on socket 0 and client on socket 1 | |

At N = 1 both experiments run the same thing (one client, one branch), and the numbers agree: Dolt point read
1.33 ms in both, UPDATE 3.27 / 3.31 ms, INSERT 3.05 / 3.16 ms.

## Reproduce

From the repo root, on a node set up with experiment 1's `setup_cloudlab_dolt.sh`:

```bash
X=experiment/2-dolt-busy-branches
ssh <host> "cd db-fork && numactl --cpunodebind=1 --membind=1 $X/scripts/run_busy_branches.sh dolt_mysql"
rsync -az <host>:db-fork/$X/data/dolt_mysql/ $X/data/dolt_mysql/
uv run python $X/scripts/plot_busy_branches.py --run-dir $X/data/dolt_mysql --run-dir $X/data/dolt --outdir $X/figures
```

## Shared code this experiment added

- `microbench/runner2.py`: `num_processes` (client threads spread over processes, rows merged),
  `background_load` / `background_operation` (one client per setup branch running a workload during the
  measurement), server and client CPU per run, table metadata looked up once per client process.
- `microbench/operations/crud.py`: the `MIXED` operation (80% point reads, 20% point updates); the write
  operations no longer query information_schema before every operation.
- `scripts/run_throughput_bench.sh`: `--processes`, `--background-op`, `--sweep-threads`.
- Tests: `tests/test_micro_runner2.py`; `tests/fake_backend.py` accepts named placeholders.
