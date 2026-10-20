# Reproducing experiment 2

Everything needed to rerun this experiment from scratch: machines, server setup, code version, every parameter, the
commands in order, how long each step takes, how to check it worked, and how to clean up. Commands run from the repo
root unless stated otherwise. `X=experiment/2-dolt-busy-branches`.

## 1. What you need

| Item | Value used on 18 Oct 2026 |
|---|---|
| Machines | 2 CloudLab nodes, type **c220g2** (Wisconsin), Ubuntu 22.04, one per server: Dolt on one, Doltgres on the other. The original run used experiment `branchdb` in project `rasl-PG0`, hosts `c220g2-010806.wisc.cloudlab.us` (Dolt) and `c220g2-010812.wisc.cloudlab.us` (Doltgres), user `JiyuHu23`. |
| Node hardware | 2× Xeon E5-2660 v3 (10 cores / 20 threads per socket, 2 NUMA nodes), 157 GB RAM, unused 447 GB SSD at `/dev/sdc`, root on HDD. Full specs: `.agent/cloudlab.md`. |
| Local machine | This repo, `uv`, `protoc` 32.x (`~/.local/bin/protoc`), ssh key with passwordless access to the nodes (`~/.ssh/dassl_rsa` originally). |
| Servers | Dolt **2.4.2** and Doltgres **1.4.0** release binaries (downloaded by the setup script). |
| Code | Base commit `24ab525` plus [`code/shared_code.patch`](code/shared_code.patch) (the shared-code changes this experiment ran with; nothing was committed at the time). Experiment scripts are in `scripts/`. |

If the shared-code changes have since been committed, use that commit and skip the patch. Otherwise:

```bash
git checkout 24ab525
git apply experiment/2-dolt-busy-branches/code/shared_code.patch   # 12 files: runner2, crud ops, scripts, tests, ...
uv sync --group dev && uv run python -m pytest tests/test_micro_runner2.py -q   # 15 passed
```

## 2. Prepare each node (once, about 5 minutes per node)

The setup script lives in experiment 1. **It formats `/dev/sdc`** (only if it has no filesystem yet) and mounts it at
`/mnt/ssd`.

```bash
S=experiment/1-dolt-branch-scaling/scripts/setup_cloudlab_dolt.sh
$S dolt_mysql <dolt-host> <user> <ssh-key>     # Dolt 2.4.2, MySQL protocol, :3306
$S dolt       <doltgres-host> <user> <ssh-key> # Doltgres 1.4.0, Postgres protocol, :5432
```

What it does on the node:

1. `apt-get install xfsprogs numactl rsync curl unzip`; formats `/dev/sdc` as XFS if blank, mounts it at `/mnt/ssd`.
2. Downloads the server release into `~/dolt/bin/dolt` or `~/doltgres-1.4.0/dl/...`, and writes its config:
   - Dolt `~/dolt/config.yaml`: `log_level: warning`, `behavior.auto_gc_behavior.enable: false`,
     `listener: {host: 127.0.0.1, port: 3306, max_connections: 2048}`, `data_dir: /mnt/ssd/dolt/databases`.
   - Doltgres `~/doltgres-1.4.0/config.yaml`: `log_level: warning`, `listener: {host: 127.0.0.1, port: 5432}`,
     `data_dir: /mnt/ssd/doltgres/databases`. Doltgres rejects `max_connections` and `behavior` keys; it accepted
     1,100 connections without them.
3. Installs a systemd unit (`dolt.service` or `doltgres.service`) that starts the server with
   `numactl --cpunodebind=0 --membind=0`, so the server uses only CPU socket 0.
4. Copies the repo to `~/db-fork` (without `experiment/*/data`), installs `protoc` 32.1 and `uv`, Python 3.13,
   `uv sync --group dev`, compiles the protos, and writes `~/db-fork/.env` pointing the adapters at `127.0.0.1` and at
   the SSD data directories.

Check the node:

```bash
ssh <host> 'systemctl is-active dolt; ss -ltn | grep 3306; df -h /mnt/ssd'      # doltgres / 5432 on the other node
```

If the nodes already ran experiment 1, only the code needs updating (the original run was set up that way):

```bash
rsync -az --exclude run_stats --exclude 'experiment/*/data' --exclude .venv --exclude .git --exclude .env \
    --exclude '*_pb2.py' ./ <user>@<host>:db-fork/
ssh <host> 'cd db-fork && ~/.local/bin/uv run python build_protos.py'
# Dolt needs max_connections >= 1100 for 1,024 clients plus setup connections:
ssh <host> 'grep max_connections ~/dolt/config.yaml'   # expect 2048; if 1000, edit it and: sudo systemctl restart dolt
```

## 3. Smoke test (about 10 minutes per node, both nodes in parallel)

```bash
ssh <host> "cd db-fork && numactl --cpunodebind=1 --membind=1 $X/scripts/run_busy_branches.sh dolt_mysql --smoke"
# on the Doltgres node: ... run_busy_branches.sh dolt --smoke
```

N = 1, 4, 64 with READ and UPDATE, and branch creation with 10 creates. Writes to `$X/data/superseded/smoke_<backend>/`.
The first run also creates the seed database `ch_w1_seed` (about 60 s on Dolt, 115 s on Doltgres). It stays on the
server and every later run copies it in about 7 s.

Pass criteria: every line in `progress.log` is `DONE`; in each `*_summary.json`, `total_ops > 0`, `failed_ops == 0`,
`num_processes` equals min(N, 20) (min(N + 1, 20) for branch creation), and for branch creation
`background.ops_in_window > 0` and `background.failed == 0`.

The smoke run also warms the server after a restart. Without it, the first measured point can be slow: the original
run's first Dolt branch-creation point measured 12 ms instead of 2.7 ms right after a restart, and was rerun.

## 4. Full run (about 2 hours per node, both nodes in parallel)

Launch detached, with the client on CPU socket 1:

```bash
ssh <host> "cd db-fork && (setsid nohup numactl --cpunodebind=1 --membind=1 $X/scripts/run_busy_branches.sh dolt_mysql \
    < /dev/null > $X/logs/driver_dolt_mysql.out 2>&1 &)"
# Doltgres node: same with dolt
```

Watch it:

```bash
ssh <host> "tail -3 db-fork/$X/data/dolt_mysql/progress.log; grep -c FAILED db-fork/$X/data/dolt_mysql/progress.log"
```

A complete run has 55 `DONE` lines (11 values of N × 4 data operations + 11 branch-creation runs) and ends with
`=== study done`. The original run took 2 h 2 min (Dolt) and 2 h 19 min (Doltgres); most of it is building the
branches (about 0.5 s per branch) for the large N.

### Parameters (all set by `scripts/run_busy_branches.sh`)

| Parameter | Value |
|---|---|
| N (existing branches) | 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024; fresh database per run |
| Branch shape and divergence | all branches forked from `main`; each gets 100 untimed single-row UPDATEs on `stock` right after creation (uncommitted working-set changes) |
| Data | CH-benCHmark at 1 warehouse (about 608,000 rows), foreign keys skipped, copied from `ch_w1_seed`; benchmark table `stock` |
| Data-op runs | N clients, client *i* on branch *i*, one request in flight (sync mode); operations READ (point), RANGE_READ (100 rows), INSERT, UPDATE, one run each |
| Ops per client | READ max(50, ⌈40000/N⌉), RANGE_READ max(10, ⌈10000/N⌉), INSERT and UPDATE max(10, ⌈6000/N⌉); 10% extra untimed warm-up |
| Branch-creation runs | 1 measured client creates 100 branches (10 untimed first) from its branch; N background clients, one per branch, run MIXED (80% point READ, 20% point UPDATE) back to back; measurement starts 1 s after every background client completed an operation and stops them when it ends |
| Client processes | min(N, 20) for data ops, min(N + 1, 20) for branch creation (the 20 hardware threads of socket 1) |
| Server pinning | socket 0 via the systemd unit; client via `numactl --cpunodebind=1 --membind=1` |
| CPU accounting | `DBFORK_SERVER_PID` = the unit's MainPID (set by the driver), so each summary has `server_cpu_cores` |

Each run is one call of the repo's throughput script, which the driver logs in `logs/<run>.log`. For example, a
data-op run at N = 64 and the branch-creation run at N = 64 are:

```bash
./scripts/run_throughput_bench.sh dolt_mysql --sweep-proportional --threads-per-branch 1 --branch-list 64 \
    --processes 20 --operations UPDATE --num-ops 94 --mode sync --warmup-fraction 0.1 --table-name stock \
    --updates-per-branch 100 --seed-db ch_w1_seed --output-dir $X/data/dolt_mysql/data_ops
./scripts/run_throughput_bench.sh dolt_mysql --sweep-branches --threads 1 --branch-list 64 \
    --processes 20 --operations BRANCH_CREATE --branch-create-ops 100 --background-op MIXED --mode sync \
    --warmup-fraction 0.1 --table-name stock --updates-per-branch 100 --seed-db ch_w1_seed \
    --output-dir $X/data/dolt_mysql/branch_create
```

To rerun only some points: `run_busy_branches.sh dolt_mysql --phases branch_create --ns 1,2 --outroot <new dir>`.
Write reruns to a new directory: the result parquet files append by run id, so rerunning into the same directory
mixes old and new rows.

## 5. Read check (about 5 minutes per node, server otherwise idle)

Checks whether the read limit is the benchmark client or the server, with a minimal asyncio client and the same load
shape (N busy branches, one connection each, one request in flight):

```bash
ssh <host> "cd db-fork && export DBFORK_SERVER_PID=\$(systemctl show -p MainPID --value dolt.service) && \
    numactl --cpunodebind=1 --membind=1 ~/.local/bin/uv run python $X/scripts/read_ceiling_check.py dolt_mysql \
    --out $X/data/dolt_mysql/read_check.csv"
```

It creates branches `rc_0..rc_1023` on `ch_w1_seed` from `main`, reads only, and deletes them at the end.

### Disk-flush probe (about 1 minute per node)

```bash
scp $X/scripts/fsync_probe.sh <host>:   # or run it from the repo copy on the node
ssh <host> "numactl --cpunodebind=1 --membind=1 bash db-fork/$X/scripts/fsync_probe.sh dolt_mysql dolt"
# Doltgres node: ... fsync_probe.sh dolt doltgres
```

Times 4 KiB `fsync` / `fdatasync` appends and overwrites on `/mnt/ssd`, then counts the server's flush calls with
`strace` (needs sudo) during 500 sequential UPDATEs on a scratch branch it creates and deletes. The original results are
in `data/fsync_probe.md`.

## 6. Collect, plot, check

```bash
for b in dolt_mysql dolt; do   # with the matching host for each
    rsync -az <host>:db-fork/$X/data/$b/ $X/data/$b/
    rsync -az <host>:db-fork/$X/logs/ $X/logs/
done
uv run python $X/scripts/plot_busy_branches.py --run-dir $X/data/dolt_mysql --run-dir $X/data/dolt --outdir $X/figures
```

Outputs `figures/data_ops.png`, `figures/branch_create.png` and `figures/summary.csv` / `summary.md`.

```bash
uv run python $X/scripts/branch_activity.py    # figures/activity.csv: requests in flight per run (Little's law)
```

This shows whether the N branches were really busy in each run: average requests in flight close to N means yes.

Checks before reading the numbers:

- `failed` is 0 in `summary.csv`, and `background.failed` is 0 in the branch-creation summaries.
- Compare `server_cpu_cores` with `client_cpu_cores`. A read point where the server used fewer than 15 of its 20 threads
  was limited by the client; the plot draws it hollow.
- N = 1 data-op latencies should match experiment 1's single-client numbers (READ about 1.3 ms, UPDATE 3.3 ms on Dolt).

## 7. Clean up

The run scripts drop each run's database and purge Dolt's dropped databases. Left on the servers: the seed database
`ch_w1_seed` (keep it for reruns). To stop the servers and release the machines:

```bash
ssh <host> 'sudo systemctl disable --now dolt'      # or doltgres
```

Then terminate the CloudLab experiment from the CloudLab web portal.

## Known pitfalls

- `pkill -f <pattern>` over ssh also matches the ssh command's own shell; use a pattern such as `[r]un_busy_branches`.
- Copied scripts must keep their execute bit (`chmod +x $X/scripts/*`).
- Doltgres `config.yaml` with keys it does not know (`max_connections`, `behavior`) stops the server from starting;
  `journalctl -u doltgres` shows the error.
- A fresh CloudLab node needs `apt-get update` before installing packages (the setup script does it).
