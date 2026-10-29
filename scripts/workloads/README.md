# Workloads: Neon and Doltgres

These scripts rerun the workloads of experiments 4, 5, 7 and 11 against a deployment described by a topology
(`scripts/setup/topologies/*.toml`), for two backends: Neon (runner backend `neon_local`, self-hosted, on one host
or on a CloudLab cluster) and Doltgres (runner backend `dolt`). The setup scripts in `scripts/setup/` deploy and
start the servers. The workload scripts never start or stop a server.

All measurement goes through the shared code, unchanged: `scripts/run_throughput_bench.sh` calls
`microbench/runner2.py`, which uses `dblib/neon_local.py` or `dblib/dolt.py`. The cold start and the ceiling check
are small dedicated clients, as they were in experiments 5 and 7.

Every parameter comes from the topology. The defaults are experiment 5's (`WORKLOAD_DEFAULTS` in
`scripts/setup/topo.py`), and `[workload]`, `[neon.workload]` or `[doltgres.workload]` override them:

- W = 4 warehouses.
- Neon compute: shared_buffers 8 MB, LFC 36 MB, LFC chunk 1 page.
- Prewarm: 3,400 heap pages first, then `stock_pkey`.
- Table `stock`, with 100 untimed updates per setup branch.
- 10% of each run is an untimed warm-up.
- N for the data workloads: 1, 4, 16, 64, 128, 256, 512, 1024. N for branch creation: 1 to 512.
- Branch creation: 100 creates, connecting to every 5th, with a MIXED background.
- Cold start: 3 runs of 30,000 reads, with 30 s between runs.

## Before running

```bash
T=scripts/setup/topologies/local_dassl.toml        # or cloudlab_two_zone.toml, ...
B=neon                                             # or doltgres
uv run python scripts/setup/$B.py $T install
uv run python scripts/setup/$B.py $T start
uv run python scripts/setup/$B.py $T seed
uv run python scripts/setup/$B.py $T env-init      # writes <state_dir>/<backend>.env (never the repo's .env)
# client on another node (CloudLab): uv run python scripts/setup/$B.py $T deploy-client, then run there
```

`common.sh` does the following before every run:

1. It clears any inherited `DBFORK_*`, `NEON_LOCAL_*` and `DOLT_*` variables.
2. It loads the variables of `topo.py shell $T --backend $B`.
3. It sources `$ENV_FILE`, which holds `NEON_LOCAL_*`, `DOLT_*` and `DBFORK_REMOTE_CPU`.
4. It runs every runner under the client placement:
   - `taskset -c CLIENT_CPUS numactl --membind=CLIENT_MEM` when the topology gives client CPUs;
   - `numactl --cpunodebind/--membind=CLIENT_NUMA` when it gives a client NUMA node;
   - no binding otherwise.

A driver refuses to start in any of these cases:

- It is not on the client node.
- The environment file or the Neon seed is missing.
- The server on this host is not running.
- Neon and Doltgres would both be running on the same host.
- The client CPUs overlap the server CPUs.
- The disk has less than `disk_guard_gb` free.
- Another driver holds `<state_dir>/workload.lock`.
- The repo's `.env` sets `NEON_LOCAL_*`, `DOLT_*` or `DBFORK_*` variables.

## The workloads

| Script | Source | What it measures |
|---|---|---|
| `low_contention.sh` | exp 4/5 A, exp 11 A | N = 1, one client. Data runs, each its own run: READ 40,000, RANGE_READ 10,000 (100-row ranges), INSERT 6,000, UPDATE 6,000 (`--sweep-proportional --threads-per-branch 1 --branch-list 1 --processes 1`). Branch run: 100 creates, connecting to every 5th, while the one setup branch runs MIXED in the background (`--sweep-branches --threads 1 --branch-list 1 --processes 2`). `--parts data,branch`. Neon: statement log on. |
| `scaling_data.sh` | exp 5 B (exp 7/11 B) | N busy branches with one client each and one request in flight. min(N, MAX_PROCS) client processes. Per-client statements: `k = max(ceil(total/N), min)`, then lowered so that `k*N <= cap`. READ: total 12,000, min 3,000, cap 1,000,000. RANGE_READ: 2,500 / 600 / 1,000,000. INSERT and UPDATE: 6,000 / 2,500 / 640,000. `--ceiling` then runs `ceiling_check.py` at the N in `--ceiling-ns` (default 128, 256, 512, 1024, kept only if in the sweep). Neon: statement log off, compute data directory on disk. |
| `scaling_branch.sh` | exp 5 C (exp 7/11 C with `--background READ`) | 100 timed creates, connecting to every 5th, while N setup branches each run a background client. Background MIXED: 80% point reads, 20% point updates. Background READ: point reads. min(N+1, MAX_PROCS) processes. Results go to `branch_create/<background>/`. |
| `cold_start.py` | exp 5 D, plus Doltgres | Point-read latency on a new branch while its caches fill (details below). |
| `ceiling_check.py` | exp 5 (exp 3/7) | The minimal asyncio client with the benchmark's load shape (N branches, one connection each, one request in flight): kinds point, range, insert and update for 30 s. If it reaches the benchmark's ceiling while the client is not saturated, the ceiling is the server. |
| `run_all.sh` | exp 5 / 11 run_all | Runs the four workloads in order for one backend: low_contention, scaling_data `--ceiling`, scaling_branch, cold_start. |

`cold_start.py` per backend:

- **Neon:** a new timeline (`create_ms`), its compute started on the first compute agent (`compute_start_ms`,
  plus the agent's own `agent_start_ms`), the first connection (`first_connect_ms`), then the point reads. The
  compute starts with empty caches (LFC_MB, statement log on).
- **Doltgres:** "a new branch on a warm server". A fresh database is copied from the seed with the runner's own
  helpers. Each run then does `dolt_branch('cold_k','main')` (`create_ms`), opens a new connection
  (`first_connect_ms`) and calls `dolt_checkout` (`checkout_ms`) before the point reads.
- **Both:** the reads use uniform keys with `Random(1000+k)`. `runs.json` also holds `median_first_100_ms` and
  `median_last_5000_ms` (`median_last_10pct_ms` for runs under 10,000 reads).

Neon cache rules (experiment 5):

- Before its warm-up, every client, background clients included, runs
  `SET maintenance_io_concurrency = 128; pg_prewarm('stock','buffer','main',0,PREWARM_HEAP_BLOCKS-1); pg_prewarm('stock_pkey')`.
  This is the heap prefix first and the key last; the other order evicts index leaves.
- `NEON_LOCAL_COMPUTE_OPTS` holds `lfc_mb` and `statement_log`, `pgdata_on_disk` for the scaling runs, and the
  shards from `NEON_LOCAL_PS_OPTS`.
- The LFC chunk size is a start option of the compute agent (setup), not a per-compute option.

## Running

```bash
# always check first: prints every runner command line and the environment it gets, runs nothing
scripts/workloads/run_all.sh --topology $T --backend $B --outdir DIR --dry-run
# smoke (counts /10 with a minimum of 30, N = 1,4, 10 creates, cold start 1 x 3,000 reads; to DIR/<backend>/smoke/)
scripts/workloads/run_all.sh --topology $T --backend $B --outdir DIR --smoke
# everything, detached (CloudLab: as a transient systemd unit, so an ssh logout does not end it)
setsid nohup scripts/workloads/run_all.sh --topology $T --backend $B --outdir DIR > DIR/run_all.out 2>&1 < /dev/null &
# one workload
scripts/workloads/scaling_data.sh --topology $T --backend $B --outdir DIR [--ns 1,64] [--ops READ,UPDATE] [--ceiling]
scripts/workloads/scaling_branch.sh --topology $T --backend $B --outdir DIR --background READ
uv run python scripts/workloads/cold_start.py --topology $T --backend $B --outdir DIR [--runs 3 --reads 30000 --pause 30]
```

Every driver takes these common options:

- `--topology FILE --backend neon|doltgres --outdir DIR`
- `[--smoke]`, `[--dry-run]`
- `[--ns LIST]`: replaces the topology's `ns_data` / `ns_branch`.
- `[--ops LIST]`: data operations.

Run `cold_start.py` and `ceiling_check.py` under the client placement. `run_all.sh` and `scaling_data.sh --ceiling`
already do this.

Every run is limited to `timeout -k 120 -s INT RUN_TIMEOUT`. The limit is 5,400 s, raised to 7,200 s for
low contention and 10,800 s for the cold start.

## Output

```
DIR/<backend>/                          (smoke: DIR/<backend>/smoke/)
  run_all.log, driver_<workload>.out
  <workload>/                           low_contention, scaling_data, scaling_branch, cold_start
    progress.log                        START / DONE (+ check summary) / CHECK-FAIL / FAILED / SKIP / RETRY / MOVED, md5 of the shared code at start and end
    data_ops/<OP>/                      runner output: <run_id>.parquet, <run_id>_setup.parquet, *_summary.json (one directory per op: the parquet name has no op)
    branch_create/[<background>/]       the same for BRANCH_CREATE runs
    cpu/<run>.csv (+ .procs.csv)        per-run CPU agent (only when the server is on this host)
    metrics/<run>_{before,after}.txt    Neon: setup/neon.py metrics, if that subcommand exists
    logs/<run>.log, <run>.agent.log, purge_*.log
    superseded/<run>_<epoch>/           files of an earlier unfinished attempt, moved aside before a rerun
    ceiling_check.csv                   scaling_data --ceiling
  cold_start/run<k>.parquet, runs.json  (i, t_start, t_end, latency_s, w, i_id)
```

The run id is `<runner backend>_chw<W>_tp_t<threads>_b<branches>`. The summary file is
`<run id>_<OP>_threads<threads>_summary.json`.

## Pass checks

A run is `DONE` only if its summary has:

- `failed_ops` = 0 and `background.failed` = 0;
- `num_processes` equal to the intended number;
- `server_cpu_cores` set, when the server process is on this host (`DBFORK_SERVER_PID`);
- for every `DBFORK_REMOTE_CPU` label that samples the server CPUs (`set=server`): `remote_cpu.<label>.node_cores`;
- `proc_cores` for the `pageserver` and `doltgres` groups;
- when the CPU agent records it, server-node memory above `mem_guard_gb`.

Otherwise the run is `CHECK-FAIL`. A run whose runner exits non-zero is `FAILED`, and on Neon it is followed by a
purge.

Each driver retries a run that is not `DONE` once, at the end of its sweep. A rerun skips `DONE` runs, so a sweep
resumes where it stopped. The smoke and the full runs keep separate progress logs.

## Pitfalls

- **Parquet appends by run id.** The runner appends to `<run_id>.parquet`. Before a rerun, the files of an earlier
  unfinished attempt are moved to `superseded/`. To repeat a finished workload, use a new `--outdir`; deleting
  `DONE` lines does not clear the old rows. `cold_start.py` exits without doing anything if `runs.json` already
  holds the requested runs.
- **The client is CPU-bound** at about 0.3 ms of client CPU per statement (runner overhead). One process caps a few
  thousand ops/s, so N clients are spread over `min(N, MAX_PROCS)` processes. `MAX_PROCS` defaults to the number
  of client CPUs (16 without a CPU list). Compare a plateau with `ceiling_check.py` before calling it the server's.
- **Neon and Doltgres never run together on shared cores** (`local_dassl`: both servers use the same 10 CPUs). The
  drivers refuse to start if the other server's processes run on the host.
- **RemoveIPC.** systemd-logind removes a user's POSIX semaphores at logout unless `RemoveIPC=no`. The client
  processes then fail to start (`SemLock._rebuild ... FileNotFoundError`). This applies to the client node too.
  The setup is meant to set it; check `loginctl show-logind` or `/etc/systemd/logind.conf`. Run long sweeps detached.
- **Client memory.** The runner keeps every result row in memory until the run ends. This is why the per-run caps
  exist (1,000,000 reads, 640,000 writes): at N = 1,024, a 15 GB client swapped for an hour without them.
- **Never create the repo's `.env`.** `dblib` modules load it at import and fill in anything the environment file
  leaves unset (a stale `NEON_LOCAL_PS_OPTS` would route every compute to shards). The drivers refuse to run if it
  sets such variables.
- **Prewarm order:** heap first, then key (see above). The prewarm statements may not contain `;`, because the
  runner splits `DBFORK_PREWARM_SQL` on it.
- **Sharded tenant (Neon).** A shard split is one-way. Branch creation in experiment 5 ran on the unsharded tenant,
  so `scaling_branch.sh` warns if `NEON_LOCAL_PS_OPTS` names pageservers.
- **Computes per node (Neon).** Each compute uses about 80 MB plus its LFC. On `local_dassl`, `ns_data` stops at
  512.
- **Doltgres data directory.** Dropped databases stay under `.dolt_dropped_databases` until purged. The runner
  purges them when the server is on this host (`DOLT_DATA_DIR` in the environment file).
- **CPU sampling.** The per-run `scripts/setup/cpu_agent.py` serves `DBFORK_REMOTE_CPU` on `CPU_AGENT_PORT` only
  when the server is on the client host. Otherwise the setup runs it as a service on the server nodes. If the port
  is already in use, no per-run agent is started.
