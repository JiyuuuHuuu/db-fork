# Experiment 7: Doltgres reads and branch creation on the local machine, server on 10 cores

## Context

Experiment 2 (CloudLab c220g2, Doltgres 1.4.0, 20 server hardware threads) found that point reads across busy
branches plateau at ~22k/s with the server at 19.8 of 20 threads, but its range-read and Dolt numbers were limited by
the Python client, and branch creation slowed from 3.9 ms to 1.85 s with an 80/20 read/update background. Two things
are still open: (1) a clean demonstration that the read ceiling is **server CPU** and nothing else, and (2) whether
**read-only** busy branches slow branch creation at all (the hypothesis: only writes interfere).

Rerun both on this 4-socket Xeon Gold 6418H box (4 NUMA nodes × 24 cores, no SMT), Doltgres only, server limited to
10 cores, clients on 38 cores, with per-core CPU monitoring, a minimal-client read check and a 5-vs-10-core sweep.
Everything lives in `experiment/7-doltgres-local-cpu/`; **no shared code is edited** (experiment 5's session is
concurrently modifying `microbench/runner2.py` / `dblib/neon_local.py` in this checkout).

User decisions: NUMA nodes 2 and 3; branch-creation sub-experiment with READ-only background only (compare against
experiment 2's MIXED numbers in the report); core sweep at 5 and 10 server cores; Doltgres 1.4.0.

## Machine and placement

| | CPUs | Memory | Notes |
|---|---|---|---|
| Server (10 cores) | node 2: `2,6,10,14,18,22,26,30,34,38` | `--membind=2` | `GOMAXPROCS=10` set explicitly too |
| Server (5-core sweep) | `2,6,10,14,18` | `--membind=2` | `GOMAXPROCS=5` |
| Clients (38 cores) | node 2: `42,46,...,94` (14) + node 3: `3,7,...,95` (24) | `--membind=2,3` | driver, client processes, CPU agent, read check |

- Root fs is 100% full (4.5 GB free): results (~100-200 MB) go in the experiment dir; **everything server-side goes on
  NVMe at `/mydata/jiyu/doltgres-exp7/`** (binary download, `config.yaml`, `databases/`, `.doltcfg/`, pid file).
- Port 5433 (free; non-default to avoid clashes on this shared box). No `.env` is created; the scripts export
  `DOLT_HOST=127.0.0.1 DOLT_PORT=5433 DOLT_DATA_DIR=/mydata/jiyu/doltgres-exp7/databases` themselves
  (`dblib/dolt.py:22-26` reads these; nothing sources `.env` in the local path).
- No sudo. `perf_event_paranoid=1`, `mpstat`/`numactl` present, `ulimit -n 1048576` (Doltgres 1.4.0 took 1,025
  connections in experiment 2 without config).

## Layout

```
experiment/7-doltgres-local-cpu/
  README.md  plan.md  REPRODUCE.md  report.md
  figures/   data_ops.png (latency, throughput, server CPU vs N), branch_create.png (latency only),
             summary.csv / summary.md, core_sweep.md
  data/dolt/data_ops/        A: READ, RANGE_READ at N = 1, 4, 16, 64, 256, 1024, server on 10 cores
  data/dolt/branch_create/   B: 100 creates, background READ, same N
  data/dolt/cpu/             per-second per-core samples per run (<run name>.csv)
  data/dolt/read_check.csv   minimal client, 10 server cores, N = 64, 256, 1024, point + range
  data/dolt/read_check_5c.csv  the same at N = 256 with the server on 5 cores (the core sweep)
  data/dolt/logs/, progress.log
  data/superseded/           the smoke run
  logs/                      driver_*.log, server_*.log, pinning_*.log   (name them *.log: **.out is gitignored)
  scripts/                   env.sh, doltgres_server.sh, cpu_agent.py, run_busy_branches.sh, read_ceiling_check.py,
                             plot_busy_branches.py, run_all.sh
  code/                      base_commit.txt, git_status.txt, shared_code.patch
```

## Scripts (all new, under `scripts/`)

1. **`env.sh`** (sourced by the others): `ROOT=/mydata/jiyu/doltgres-exp7`, the three CPU lists above, exports
   `DOLT_HOST/PORT/DATA_DIR`, `DOLT_ROOT_PATH=$ROOT/home` (Dolt committer identity, see pitfalls), `PATH` with
   `~/.local/bin`, `PYTHONUNBUFFERED=1`; `unset DBFORK_SERVER_HOST DBFORK_SERVER_SSH DBFORK_PREWARM_SQL`; sets
   `DBFORK_SERVER_PID` from `$ROOT/doltgres.pid` if present.
2. **`doltgres_server.sh install | start [K=10] | stop | status`**
   - `install`: download `https://github.com/dolthub/doltgresql/releases/download/v1.4.0/doltgresql-linux-amd64.tar.gz`
     into `$ROOT/dl/` (skip if present), write `$ROOT/config.yaml` like
     `experiment/1-dolt-branch-scaling/scripts/setup_cloudlab_dolt.sh:77-84` (`log_level: warning`, listener
     `127.0.0.1:5433`, `data_dir`, `cfg_dir`; no `max_connections`/`behavior` keys: 1.4.0 rejects them), write
     `$ROOT/home/.dolt/config_global.json` with user name/email (setup script line 43).
   - `start K`: refuse if pid alive or port listening; first K CPUs of the 10-core list;
     `GOMAXPROCS=$K setsid nohup numactl --physcpubind=<cpus> --membind=2 doltgres -config ... > logs/server_<ts>_<K>c.log &`;
     write `$ROOT/doltgres.pid` and `$ROOT/doltgres.cores`; wait for the port; verify `taskset -cp`, `GOMAXPROCS`
     in `/proc/<pid>/environ`, `Max open files` in `/proc/<pid>/limits`, `SELECT 1` via psycopg2.
   - `status` (= the pinning check): affinity, `ps -L -o psr` histogram of the server's threads, RSS/`numastat -p`,
     connection count, `du -sh databases`, `mpstat -P <server cpus> 1 3` (idle baseline must be ~100%: nobody else on
     those cores), `Cpus_allowed_list` of any `microbench.runner2` processes.
3. **`cpu_agent.py`** (stdlib): per-second `/proc/stat` sampler for two CPU sets (`server=`, `clients=`) writing
   `data/dolt/cpu/<run>.csv` (busy/usr/sys/softirq/iowait fractions + per-core busy), and an HTTP endpoint
   `GET /cpu?cpus=<list>&pid=<pid>` in the format `RemoteCpuSampler` (`microbench/runner2.py:1317-1404`, committed
   code, not part of experiment 5's diff) expects (`{t, node_busy_s, node_iowait_s, ncpu, proc_s}`). The driver
   exports `DBFORK_REMOTE_CPU="server=http://127.0.0.1:9007/cpu?cpus=<server>&pid=<pid>;clients=http://127.0.0.1:9007/cpu?cpus=<clients>"`,
   so every `*_summary.json` gets `remote_cpu.server.node_cores` (busy server cores over the exact timed window) and
   `remote_cpu.clients.node_cores`, next to the existing `server_cpu_cores` (process CPU from `/proc/<pid>/stat`).
4. **`run_busy_branches.sh`**: copy of `experiment/2-dolt-busy-branches/scripts/run_busy_branches.sh` with: backend
   fixed to `dolt`; `source env.sh` instead of `.env` (line 26); server pid from the pid file instead of `systemctl`
   (line 59), abort unless alive and `doltgres.cores` matches `--server-cores` (default 10); `DATA_OPS="READ RANGE_READ"`;
   `--background-op READ` (line 107); `MAX_PROCS` from `numactl --show` as before (38 here); guard that the
   driver's CPU mask is disjoint from the server's; `flock $ROOT/driver.lock`. `run()` starts `cpu_agent.py` per run,
   exports `DBFORK_REMOTE_CPU`, runs `scripts/run_throughput_bench.sh` with the same flags as experiment 2
   (`--mode sync --warmup-fraction 0.1 --table-name stock --updates-per-branch 100 --seed-db ch_w1_seed`, same
   `ops_per_client`), stops the agent, then checks the summary (`failed_ops == 0`, `background.failed == 0`,
   `server_cpu_cores` and `remote_cpu.server.node_cores` present) → `DONE` / `CHECK-FAIL` in `progress.log`.
   Logs `md5sum` of `microbench/runner2.py dblib/dolt.py scripts/run_throughput_bench.sh` at start and end so a
   mid-run edit by the other session is visible. `--smoke`: N = 1,4,64, ops/10, 10 creates, output under
   `data/superseded/smoke_dolt`.
5. **`read_ceiling_check.py`**: copy of experiment 2's, Doltgres only, host/port from env, `--procs 38` default,
   adds a `server_cores` column. Same `rc_*` branches on `ch_w1_seed`, same SQL, same `/proc/<pid>` CPU.
6. **`plot_busy_branches.py`**: copy of experiment 2's with `--server-cores` (replaces `SERVER_THREADS = 20`,
   hollow-marker rule `server_cpu_cores < 0.75 * cores`), Doltgres only, `DATA_OPS = [READ, RANGE_READ]`,
   `data_ops.png` gains a third row (server busy cores from `server_cpu_cores` and `remote_cpu.server.node_cores`,
   with a line at the core count), `branch_create.png` becomes a **single latency panel** (median + p95 vs N; title
   "…while every existing branch runs point reads"), `--read-check` / `--read-check-5c` write
   `figures/core_sweep.md` (kind, server cores, N, ops/s, p50, p95, server CPU cores, client CPU cores) from the
   two read-check CSVs. `summary.csv/.md` gain the busy fractions.
7. **`run_all.sh`**: steps 4-6 below, detached.

## Where the time goes, and what is cut

Experiment 2's Doltgres logs: a run's time is almost all **branch setup** (N branches × 100 updates, ~0.65 s per
branch), not measurement. N = 1,024: READ run 653 s with a 2.5 s timed window; create run 961 s with a 259 s window.
Full experiment-2 reads + creates = 86 min. Cuts that keep the same conclusions:

- **N = 1, 4, 16, 64, 256, 1024** (6 points instead of 11; the plateau from ~16 on and the 1,024 endpoint are kept).
  Experiment-2 timings for these: reads 35 min, creates 22 min → ~57 min, less here (faster CPUs, NVMe, and the
  read-only background makes the create windows ~1 s instead of 54-259 s).
- **Core sweep via the minimal read check only** (no 256-branch setup, 20 s per point): `read_ceiling_check.py`
  at N = 256, point and range, on 5 cores vs 10 cores. Same load shape, same conclusion (ceiling ∝ cores), ~1 min
  instead of 15. `data/dolt/core_sweep_5c/` is dropped; the 5-core rows go into `read_check_5c.csv`.
- **Read check at N = 64, 256, 1024** (not 16): 2 min.
- **One smoke run** (N = 1, 16; ops/10; 10 creates), no separate 5-core smoke/warm-up: the 5-core read check
  starts with a 20 s point-read run that also serves as the warm-up.

## Run order (`X=experiment/7-doltgres-local-cpu`, from the repo root)

| Step | What | ~Time | Pass check |
|---|---|---|---|
| 0 | Create layout, `plan.md` (this plan), `doltgres_server.sh install`; `code/`: `git rev-parse HEAD`, `git status --short`, `git diff f004d9f -- dblib microbench scripts tests util` + untracked `tests/test_neon_local.py` → `shared_code.patch`, `git apply --check` | 5 min | `doltgres --version` 1.4.0 |
| 1 | `doltgres_server.sh start 10; status > logs/pinning_idle.log` | 1 min | affinity = 10 ids, GOMAXPROCS=10, port up, SELECT 1 |
| 2 | Smoke: `numactl --physcpubind=<clients> --membind=2,3 $X/scripts/run_busy_branches.sh --smoke` (N = 1, 16) | ~5 min (first run builds `ch_w1_seed`, ~2 min) | all DONE; `num_processes == min(N,38)`; CPU fields non-null; `background.ops_in_window > 0`; no committer-identity error |
| 3 | Full A+B: `run_busy_branches.sh --ns 1,4,16,64,256,1024` detached (`setsid nohup … > logs/driver_full.log`) | ~45-55 min | 18 DONE; md5 lines identical; `status` during the N=256 READ run → `logs/pinning_busy.log` |
| 4 | Read check, 10 cores: `read_ceiling_check.py --ns 64,256,1024 --out data/dolt/read_check.csv` | 2 min | server CPU ≈ 9.5-10 cores at N ≥ 64 |
| 5 | Core sweep: `stop; start 5; read_ceiling_check.py --ns 256 --kinds point,range --out data/dolt/read_check_5c.csv; stop; start 10` | 3 min | throughput ≈ half of the 10-core N=256 point, server CPU ≈ 5 |
| 6 | `plot_busy_branches.py --run-dir data/dolt --server-cores 10 --read-check data/dolt/read_check.csv --read-check-5c data/dolt/read_check_5c.csv --outdir figures` | 2 min | `failed == 0`; `server_busy ≈ server_cpu_cores`; `core_sweep.md` written from the two CSVs |
| 7 | Write `report.md`, `README.md`, `REPRODUCE.md`; `doltgres_server.sh stop`; leave `/mydata/jiyu/doltgres-exp7` (seed) unless asked to delete | | root disk unchanged apart from `data/` |

Total: about 1 h 15 min wall clock including setup and smoke (was ~2 h 15 min).

## How the evidence is read (report.md)

1. **Server CPU saturates, client does not:** from the plateau N on, `server_cpu_cores` (process) and
   `remote_cpu.server.node_cores` (the 10 cores) both sit at ≈ 9.5-10, while the 38 client cores stay well below
   full. `softirq`/`sys` fractions show how much of the server's CPU is loopback networking.
2. **Minimal client reaches the same ceiling** as the benchmark (unlike experiment 2's Dolt and range reads).
3. **Ceiling scales with server cores:** the minimal client's N=256 throughput roughly halves on 5 cores (server
   CPU ≈ 5), latency doubles.
4. **Branch creation with read-only busy branches:** median stays at a few ms across N (experiment 2 with MIXED:
   3.9 ms → 1.85 s). If it rises anyway, report that reads contend too. Latency figure only.

## Pitfalls (go into REPRODUCE.md)

- **Never create `.env`**; never download or sync into `$HOME` (root fs full). Driver logs are `*.log`.
- **Shared code is experiment 5's uncommitted state**: `shared_code.patch` records it honestly (README: experiment 7
  depends on none of it, `DBFORK_PREWARM_SQL` unset); the md5 lines show whether it changed mid-run.
- **Parquet appends by run id** (`_execute_multi_process`): any rerun of a point goes to a separate `--outroot`
  (or delete that run id's files first).
- **`DOLT_DATA_DIR` must be the real data dir** or `run_throughput_bench.sh:451-466` cleanup of
  `.dolt_dropped_databases` is a no-op; check `du -sh $ROOT/databases` between runs.
- **Committer identity**: the seed's `dolt_commit` needs `config_global.json`; try `DOLT_ROOT_PATH=$ROOT/home`
  first, fall back to `~/.dolt/config_global.json` (tiny; nothing else on this host uses Dolt) if ignored.
- **GOMAXPROCS** comes from the affinity mask at startup; set explicitly too; change K only by restart.
- **Memory on node 2**: `--membind=2` is strict; node 2 has ~19 GB free + reclaimable cache. Watch `numastat -p` at
  N = 1024; fall back to `--preferred=2` and say so if it runs short.
- **Cold server after restart** (experiment 2 saw 12 ms creates right after one): the smoke run precedes the full
  run; the 5-core read check begins with a point-read window that doubles as warm-up (if its first point looks
  cold, rerun it: 20 s).
- **Expected magnitudes** (experiment 2 Doltgres): N=1 READ 1.4 ms, RANGE_READ 3.0 ms, creates 3.9 ms; READ plateau
  ~22k/s on 20 Haswell threads. N=1 numbers off by >2× mean a config problem.

## Verification

- `uv run pytest tests/test_micro_runner2.py` still passes (nothing shared changed; just a sanity check).
- Smoke runs (steps 2-3) exercise every script path, including the server restart and the CPU agent, before the
  1.5 h run; `status` logs prove pinning and that the server cores are otherwise idle.
- After step 7: every summary has `failed_ops == 0`, both CPU fields filled, `num_processes == min(N, 38)`;
  `figures/summary.md` and `core_sweep.md` hold every plotted point.
