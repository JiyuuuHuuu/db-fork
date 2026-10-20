# Reproducing experiment 1

Everything needed to rerun this experiment: machines, server setup, code version, every parameter, the commands in
order, how long each step takes, how to check it worked, and how to clean up. Commands run from the repo root.
`X=experiment/1-dolt-branch-scaling`.

## 1. What you need

| Item | Value used on 15-16 Oct 2026 |
|---|---|
| Machines | 2 CloudLab nodes, type **c220g2** (Wisconsin), Ubuntu 22.04, one per server. The original run used experiment `branchdb` in project `rasl-PG0`: `c220g2-010806.wisc.cloudlab.us` (Dolt), `c220g2-010812.wisc.cloudlab.us` (Doltgres), user `JiyuHu23`. |
| Node hardware | 2× Xeon E5-2660 v3 (10 cores / 20 threads per socket, 2 NUMA nodes), 157 GB RAM, unused 447 GB SSD at `/dev/sdc`. Full specs: `.agent/cloudlab.md`. |
| Local machine | This repo, `uv`, `protoc` 32.x, ssh key with passwordless access to the nodes. |
| Servers | Dolt **2.4.2** (`dolt_mysql`, MySQL protocol, :3306) and Doltgres **1.4.0** (`dolt`, Postgres protocol, :5432). |

### Code version

Experiment 1 ran on base commit `24ab525` plus uncommitted changes that were not saved separately. Experiment 2 later
added more changes on top, saved as `experiment/2-dolt-busy-branches/code/shared_code.patch` (base `24ab525`). Use
that patch: it contains everything experiment 1 used, plus these later changes:

| Later change | Effect on rerunning experiment 1 |
|---|---|
| Write operations look up table columns once per client process instead of with an information_schema query before every write | E2-L latencies and all branch-creation numbers: none (the query was untimed). E2-T and E3 write throughput: higher, because the client is no longer capped near 50 writes/s per async worker. The original E3 per-branch write rates were client-capped (see the report's "Correction"). |
| `num_processes`, `background_load`, the `MIXED` operation, CPU fields in summaries | None unless used; this experiment's scripts do not use them. |
| Dolt `max_connections` 2048 instead of 1000 in the setup script | None (this experiment opens at most about 100 connections). |

```bash
git checkout 24ab525
git apply experiment/2-dolt-busy-branches/code/shared_code.patch
uv sync --group dev && uv run python -m pytest tests/ -q     # 1 known failure: test_db_api::test_branch_commit_log_diff_reset_delete
```

## 2. Prepare each node (once, about 5 minutes per node)

```bash
$X/scripts/setup_cloudlab_dolt.sh dolt_mysql <dolt-host> <user> <ssh-key>
$X/scripts/setup_cloudlab_dolt.sh dolt       <doltgres-host> <user> <ssh-key>
```

The script **formats `/dev/sdc`** if it has no filesystem and mounts it at `/mnt/ssd`, installs the server release,
writes its `config.yaml` (Dolt: listener `127.0.0.1:3306`, `max_connections: 2048`, automatic GC off, data in
`/mnt/ssd/dolt/databases`; Doltgres: listener `127.0.0.1:5432`, data in `/mnt/ssd/doltgres/databases`), runs it as a
systemd unit pinned to CPU socket 0 (`numactl --cpunodebind=0 --membind=0`), copies the repo to `~/db-fork`, installs
`protoc`, `uv`, Python 3.13 and the venv, and writes `~/db-fork/.env` for local connections. Details and checks:
`experiment/2-dolt-busy-branches/REPRODUCE.md`, section 2.

## 3. Smoke test (about 15 minutes per node)

```bash
ssh <host> "cd db-fork && numactl --cpunodebind=1 --membind=1 $X/scripts/run_dolt_scaling.sh dolt_mysql --smoke \
    --seed-db ch_w1_seed"
```

The first run creates the seed database `ch_w1_seed` (about 60 s on Dolt, 115 s on Doltgres); later runs copy it in
about 7 s. Pass criteria: every line of `progress.log` is `DONE`, and the summaries show `total_ops > 0` and
`failed_ops == 0`.

## 4. Full run (5.3 hours on Dolt, 6.3 hours on Doltgres, both nodes in parallel)

```bash
ssh <host> "cd db-fork && (setsid nohup numactl --cpunodebind=1 --membind=1 $X/scripts/run_dolt_scaling.sh dolt_mysql \
    --seed-db ch_w1_seed < /dev/null > $X/logs/driver_dolt_mysql.out 2>&1 &)"
ssh <host> "tail -3 db-fork/$X/data/dolt_mysql/progress.log"
```

A complete run has 9 phase `DONE` lines and ends with `=== study done`. Results go to `$X/data/<backend>/<phase>/`.

### Phases and parameters (all in `scripts/run_dolt_scaling.sh`)

Common to all: CH-benCHmark at 1 warehouse (about 608,000 rows), foreign keys skipped, copied from `ch_w1_seed`;
table `stock`; every setup branch gets 100 untimed single-row UPDATEs right after creation
(`--updates-per-branch 100`, uncommitted); a fresh database per run.

| Phase | Script and mode | N | Measured |
|---|---|---|---|
| E1-L | `run_single_thread_bench.sh`, 1 client, `--shape fan_out` and `--shape spine` | 1, 2, 4, …, 1024 | 10 BRANCH_CREATE after the N-branch setup |
| E1-T | `run_throughput_bench.sh --mode sync --sweep-branches --threads 16` | 1, 4, 16, 64, 256, 1024 | 16 threads × 50 BRANCH_CREATE |
| E2-L | `run_single_thread_bench.sh`, 1 client, fan_out (spine at 1, 64, 1024) | 1, 4, 16, 64, 256, 1024 | 1,000 READ / INSERT / UPDATE, 1,000 RANGE_READ and 200 RANGE_UPDATE of **200 rows**, on one branch; the other N−1 idle |
| E2-T | `run_throughput_bench.sh --sweep-branches --threads 16 --concurrent-requests 4` (async) | 1, 2, 4, 8, 16, 64, 256, 1024 | READ, RANGE_READ (**100 rows**), UPDATE, INSERT; 1,000 point / 300 range ops per thread, 10% warm-up |
| E3 | `run_throughput_bench.sh --sweep-proportional --threads-per-branch 1 --concurrent-requests 4` | 1, 2, 4, 8, 16, 32 | same operations, one thread per branch |
| ctrl_latency, ctrl_throughput | E2-L and E2-T with `--updates-per-branch 0` | 1, 64, 1024 | as E2-L / E2-T |

To rerun a subset: `--phases e1l,e1t,e2l,e2t,e3,ctrl`. Use a new `--outroot`: result files append by run id.

## 5. Client calibration (about 25 minutes per node, after the study)

```bash
ssh <host> "cd db-fork && $X/scripts/calibrate_dolt_clients.sh dolt_mysql"
```

`scripts/driver_ceiling.py` with 1, 2, 4, 8, 16, 32 clients × 4 requests in flight for 20 s each: point and 200-row
range reads as threads of one process and as separate processes, on one branch and on one branch per client; then
single-row updates from processes on one shared branch and one branch per client, plus one sequential writer. It
creates branches `calib_shared` and `calib_0..31` on `ch_w1_seed`, writes only to them, and deletes them.
Results: `$X/data/<backend>/calibration/calib_<backend>.txt` and `calib_write_<backend>.txt`.

## 6. One-off probes

- `scripts/range_update_probe.py` (Doltgres): times a 200-row UPDATE with a row-value range predicate against the same
  update with `BETWEEN`, on a scratch branch `ru_probe` that it deletes. Run with
  `uv run python $X/scripts/range_update_probe.py` on the Doltgres node.
- `scripts/probe_copy.py <dolt|dolt_mysql>`: checks that the server can copy a table from another database's branch
  (the mechanism behind `--seed-db`).

## 7. Collect, plot, check

```bash
rsync -az <host>:db-fork/$X/data/dolt_mysql/ $X/data/dolt_mysql/      # and dolt from the other node
uv run python $X/scripts/plot_branch_scaling.py --run-dir $X/data/dolt_mysql --run-dir $X/data/dolt --outdir $X/figures
```

Checks: `failed_ops` in the summaries (the original run had 296 failed UPDATEs out of about 1.0 million timed writes,
all serialization conflicts on hot branches); E2-L N = 1 latencies about 1.3 ms READ and 3.3 ms UPDATE on Dolt.

## 8. Clean up

Each run drops its database; Dolt's dropped databases are purged by the scripts. The seed database `ch_w1_seed` stays
for reruns. To stop a server: `ssh <host> 'sudo systemctl disable --now dolt'` (or `doltgres`). Then terminate the
CloudLab experiment in the CloudLab portal.

Pitfalls are listed in `experiment/2-dolt-busy-branches/REPRODUCE.md`.
